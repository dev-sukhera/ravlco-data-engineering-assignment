"""Phase 4: CRS discipline, PIP, H3, timezone, snapping, weather, GeoParquet.

No test in this file touches the network. Three kinds of test, deliberately
separated:

  * **Unit** -- pure functions on in-memory geometry and on two small committed
    fixtures (`tests/fixtures/geo/`: a 7 KB clip of four real Montgomery block
    groups, and one real Open-Meteo response). Always run.
  * **Integration** -- the real builder over the session's gold with the real
    TIGER polygons. Skipped with a reason when `data/reference/` is absent;
    `CRASH_TEST_FULL_REFERENCE=1` turns that skip into a failure.
  * **Negative control** -- the one place `3857` is allowed to appear in this
    repo, because the test's job is to measure how wrong it is.

The pinned GEOIDs and H3 cells below were derived once against TIGER 2025 and
frozen. If TIGER re-vintages they must be re-derived, and the failure will say
so -- which is the point: a silently moving GEOID is a restatement nobody
noticed.
"""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import shapely
from shapely.geometry import LineString, Point, Polygon

from src import config, contracts
from src.geo import census_join, envelope, h3_index, reference, snap, tz
from src.geo import build as geo_build
from src.geo import weather as weather_mod
from tests.conftest import using_full_bronze

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "geo"
BG_CLIP = FIXTURES / "montgomery_bg_clip.geojson"

# The assignment's Rockville coordinate, and the 2025 TIGER geography it falls
# in. Derived once (`python -m src.geo.build`), frozen here.
ROCKVILLE = (39.0840, -77.1528)
ROCKVILLE_BG = "240317009011"
ROCKVILLE_TRACT = "24031700901"
ROCKVILLE_COUNTY = "24031"

EL_PASO = (31.7619, -106.4850)
PENSACOLA = (30.4213, -87.2169)
MIAMI = (25.7617, -80.1918)


def _one(con, sql, *params):
    return con.execute(sql, list(params)).fetchone()[0]


# ===========================================================================
# 1. timezone from coordinates
# ===========================================================================


@pytest.fixture(scope="module")
def zone_finder():
    return tz.ZoneFinder()


@pytest.mark.parametrize(
    "coord, expected, why",
    [
        (EL_PASO, "America/Denver", "Texas is not one timezone"),
        (PENSACOLA, "America/Chicago", "Florida is not one timezone"),
        (ROCKVILLE, "America/New_York", "Maryland is entirely Eastern"),
        (MIAMI, "America/New_York", "the rest of Florida is Eastern"),
    ],
)
def test_zone_comes_from_the_coordinate_not_the_state(zone_finder, coord, expected, why):
    assert zone_finder.zone_at(*coord) == expected, why


def test_zone_is_none_without_a_coordinate(zone_finder):
    assert zone_finder.zone_at(None, None) is None
    assert zone_finder.zone_at(np.nan, np.nan) is None


# ===========================================================================
# 2. DST: the gap, the ambiguity, and the flags
# ===========================================================================


def test_spring_forward_gap_is_shifted_and_flagged():
    """2024-03-10 02:30 in New York never happened. Policy: shift forward."""
    got = tz.localise(datetime(2024, 3, 10, 2, 30), "America/New_York")
    assert got.gap_adjusted is True
    assert got.ambiguous is False
    assert got.utc.isoformat() == "2024-03-10T07:30:00+00:00"
    assert got.offset_minutes == -240  # EDT
    assert got.time_status == tz.TIME_OK


def test_fall_back_ambiguity_takes_fold_zero_and_is_flagged():
    """2024-11-03 01:30 in New York happened twice. Policy: fold=0 (DST)."""
    got = tz.localise(datetime(2024, 11, 3, 1, 30), "America/New_York")
    assert got.ambiguous is True
    assert got.gap_adjusted is False
    assert got.utc.isoformat() == "2024-11-03T05:30:00+00:00"
    assert got.offset_minutes == -240
    # fold=1 would have been 06:30Z. The choice is a policy; the flag is what
    # makes it safe, so both are asserted.
    other = datetime(2024, 11, 3, 1, 30, tzinfo=tz._zone("America/New_York"), fold=1)
    assert other.utcoffset().total_seconds() == -5 * 3600


def test_an_ordinary_time_sets_neither_flag():
    got = tz.localise(datetime(2024, 6, 1, 12, 0), "America/New_York")
    assert (got.gap_adjusted, got.ambiguous) == (False, False)
    assert got.utc.isoformat() == "2024-06-01T16:00:00+00:00"


def test_the_same_policies_hold_in_central_time():
    gap = tz.localise(datetime(2024, 3, 10, 2, 30), "America/Chicago")
    amb = tz.localise(datetime(2024, 11, 3, 1, 30), "America/Chicago")
    assert gap.gap_adjusted and gap.utc.isoformat() == "2024-03-10T08:30:00+00:00"
    assert amb.ambiguous and amb.utc.isoformat() == "2024-11-03T06:30:00+00:00"


def test_localise_refuses_an_already_aware_timestamp():
    """A tz-aware value here means somebody converted instead of localising."""
    aware = datetime(2024, 6, 1, 12, 0, tzinfo=tz.UTC)
    with pytest.raises(ValueError, match="NAIVE"):
        tz.localise(aware, "America/New_York")


def test_no_time_means_no_utc_not_midnight():
    got = tz.localise(None, "America/New_York")
    assert got.utc is None and got.time_status == tz.TIME_UNKNOWN


# ===========================================================================
# 3. fallback provenance
# ===========================================================================


@pytest.fixture
def county_zones():
    # El Paso is Mountain, Bexar is Central, Gulf County FL is genuinely split.
    return tz.county_zone_table(
        [("48141", "America/Denver")] * 9
        + [("48029", "America/Chicago")] * 5
        + [("12045", "America/Chicago")] * 10 + [("12045", "America/New_York")] * 9
    )


def test_null_coordinate_falls_back_to_the_county_zone(county_zones):
    zone, source, low = tz.resolve_zone(
        coordinate_zone=None, county_geoid="48141", jurisdiction="TX",
        county_zones=county_zones,
    )
    assert (zone, source, low) == ("America/Denver", tz.TZ_SOURCE_COUNTY, False)


def test_a_split_county_fallback_is_low_confidence(county_zones):
    zone, source, low = tz.resolve_zone(
        coordinate_zone=None, county_geoid="12045", jurisdiction="FL",
        county_zones=county_zones,
    )
    assert (zone, source, low) == ("America/Chicago", tz.TZ_SOURCE_COUNTY, True)
    assert county_zones["12045"].split_tz is True
    assert county_zones["12045"].share == pytest.approx(10 / 19)


def test_no_county_falls_back_to_the_jurisdiction_default(county_zones):
    for juris, expected in (("MD", "America/New_York"), ("TX", "America/Chicago"),
                            ("FL", "America/New_York")):
        zone, source, low = tz.resolve_zone(
            coordinate_zone=None, county_geoid=None, jurisdiction=juris,
            county_zones=county_zones,
        )
        assert (zone, source, low) == (expected, tz.TZ_SOURCE_JURISDICTION, True)


def test_no_county_and_no_jurisdiction_is_unresolved(county_zones):
    zone, source, low = tz.resolve_zone(
        coordinate_zone=None, county_geoid=None, jurisdiction=None,
        county_zones=county_zones,
    )
    assert (zone, source) == (None, tz.TZ_SOURCE_UNRESOLVED) and low


def test_the_coordinate_always_wins(county_zones):
    zone, source, low = tz.resolve_zone(
        coordinate_zone="America/Denver", county_geoid="12045", jurisdiction="FL",
        county_zones=county_zones,
    )
    assert (zone, source, low) == ("America/Denver", tz.TZ_SOURCE_COORDINATE, False)


def test_county_zone_table_ties_are_deterministic_and_flagged():
    table = tz.county_zone_table(
        [("12045", "America/New_York"), ("12045", "America/Chicago")]
    )
    assert table["12045"].tz_iana == "America/Chicago"  # name order breaks the tie
    assert table["12045"].split_tz is True


# ===========================================================================
# 4. point in polygon
# ===========================================================================


def _synthetic_polygons() -> gpd.GeoDataFrame:
    """Three unit squares sharing edges, in EPSG:4326."""
    return gpd.GeoDataFrame(
        {"GEOID": ["240310000001", "240310000002", "240310000003"]},
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs=4326,
    )


def _points(coords) -> gpd.GeoDataFrame:
    df = pd.DataFrame({
        "crash_sk": list(range(1, len(coords) + 1)),
        "latitude": [c[1] if c else None for c in coords],
        "longitude": [c[0] if c else None for c in coords],
    })
    return census_join.points_frame(df)


def test_pip_matches_a_point_inside_and_derives_the_tract_prefix():
    out, stats = census_join.point_in_polygon(
        _points([(0.5, 0.5)]), _synthetic_polygons(), key="crash_sk", out_col="bg_geoid"
    )
    assert out.loc[0, "bg_geoid"] == "240310000001"
    assert out.loc[0, "pip_status"] == census_join.PIP_MATCHED
    assert census_join.tract_from_bg(out["bg_geoid"])[0] == "24031000000"
    assert stats["hit_rate"] == 1.0


def test_pip_reports_no_polygon_rather_than_a_null_that_could_mean_anything():
    out, stats = census_join.point_in_polygon(
        _points([(9.5, 9.5)]), _synthetic_polygons(), key="crash_sk", out_col="bg_geoid"
    )
    assert out.loc[0, "pip_status"] == census_join.PIP_NO_POLYGON
    assert pd.isna(out.loc[0, "bg_geoid"]) and stats["no_polygon"] == 1


def test_a_missing_coordinate_keeps_its_row_and_says_why():
    out, stats = census_join.point_in_polygon(
        _points([None]), _synthetic_polygons(), key="crash_sk", out_col="bg_geoid"
    )
    assert len(out) == 1
    assert out.loc[0, "pip_status"] == census_join.PIP_NO_GEOMETRY
    assert stats["with_geometry"] == 0


def test_a_boundary_point_matches_exactly_once_and_the_tie_is_counted():
    """x=1 is the shared edge of polygons 1 and 2. `intersects` hits both."""
    out, stats = census_join.point_in_polygon(
        _points([(1.0, 0.5)]), _synthetic_polygons(), key="crash_sk", out_col="bg_geoid"
    )
    assert len(out) == 1
    assert out.loc[0, "bg_geoid"] == "240310000001"  # smallest GEOID wins
    assert stats["boundary_ties_broken"] == 1


def test_pip_refuses_two_different_crs_rather_than_silently_aligning_them():
    polys = _synthetic_polygons().to_crs(5070)  # any other CRS; not 3857
    with pytest.raises(ValueError, match="one CRS on both sides"):
        census_join.point_in_polygon(
            _points([(0.5, 0.5)]), polys, key="crash_sk", out_col="bg_geoid"
        )


def test_pip_against_four_real_block_groups_from_the_committed_clip():
    """The committed 7 KB clip: real TIGER 2025 geometry, no PII, no network."""
    bgs = gpd.read_file(BG_CLIP)
    assert bgs.crs.to_epsg() == 4326
    out, _ = census_join.point_in_polygon(
        _points([(ROCKVILLE[1], ROCKVILLE[0])]), bgs, key="crash_sk", out_col="bg_geoid"
    )
    assert out.loc[0, "bg_geoid"] == ROCKVILLE_BG
    assert census_join.tract_from_bg(out["bg_geoid"])[0] == ROCKVILLE_TRACT
    assert ROCKVILLE_BG[:11] == ROCKVILLE_TRACT


def test_source_county_geoid_only_accepts_a_five_digit_county():
    """-1 is UNKNOWN and 24 is a STATE member; neither is a county to compare to."""
    got = envelope.source_county_geoid(pd.Series([24031, -1, 24, 48141, None]))
    assert got[0] == "24031" and got[3] == "48141"
    assert pd.isna(got[1]) and pd.isna(got[2]) and pd.isna(got[4])


# ===========================================================================
# 5. H3
# ===========================================================================


def test_r8_and_r7_are_parents_of_the_stored_r9():
    cells = h3_index.index_point(*ROCKVILLE)
    assert h3_index.parent(cells["h3_r9"], 8) == cells["h3_r8"]
    assert h3_index.parent(cells["h3_r8"], 7) == cells["h3_r7"]


def test_a_known_coordinate_gives_a_pinned_cell():
    cells = h3_index.index_point(*ROCKVILLE)
    assert cells["h3_r9"] == "892aa84163bffff"
    assert cells["h3_r8"] == "882aa84163fffff"
    assert cells["h3_r7"] == "872aa8416ffffff"


def test_a_missing_coordinate_gives_null_cells_not_a_sentinel():
    for lat, lon in ((None, None), (np.nan, np.nan)):
        assert h3_index.index_point(lat, lon) == {
            "h3_r9": None, "h3_r8": None, "h3_r7": None
        }


def test_the_module_calls_only_the_h3_v4_api():
    """Every `h3.<name>` the code CALLS, found by parsing rather than grepping.

    A substring search would trip over the docstring that names the v3 calls in
    order to say they no longer exist; the AST sees code and not prose.
    """
    import ast

    tree = ast.parse(Path(h3_index.__file__).read_text())
    called = {
        node.attr for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name) and node.value.id == "h3"
    }
    v3 = {"geo_to_h3", "h3_to_parent", "k_ring", "h3_to_geo", "hex_area",
          "h3_to_children", "h3_get_resolution"}
    assert not (called & v3), f"h3 v3 names do not exist in 4.x: {called & v3}"
    assert called == {"latlng_to_cell", "cell_to_parent", "grid_disk", "cell_area"}


def test_grid_disk_is_sorted_and_contains_the_cell_itself():
    cell = h3_index.index_point(*ROCKVILLE)["h3_r8"]
    disk = h3_index.grid_disk(cell, 1)
    assert disk == sorted(disk) and cell in disk and len(disk) == 7


def test_disk_weights_only_link_cells_that_are_present():
    cell = h3_index.index_point(*ROCKVILLE)["h3_r8"]
    neighbours = [c for c in h3_index.grid_disk(cell, 1) if c != cell]
    weights = h3_index.disk_weights([cell, neighbours[0]])
    assert weights[cell] == [neighbours[0]]
    assert weights[neighbours[0]] == [cell]


# ===========================================================================
# 6. snapping and linear referencing
# ===========================================================================


@pytest.fixture
def network():
    """Two parallel segments 300 m apart, in EPSG:26985 metres."""
    x0, y0 = 400_000.0, 130_000.0
    return gpd.GeoDataFrame(
        {
            "osm_way_id": [100, 200],
            "osm_highway": ["residential", "primary"],
            "osm_maxspeed": ["25 mph", None],
            "osm_maxspeed_mph": [25.0, np.nan],
            "osm_lanes": [None, "4"],
            "osm_name": ["A St", "B Ave"],
            "osm_ref": [None, "MD-355"],
        },
        geometry=[
            LineString([(x0, y0), (x0 + 1000, y0)]),
            LineString([(x0, y0 + 300), (x0 + 1000, y0 + 300)]),
        ],
        crs=26985,
    )


def _projected_points(coords):
    return gpd.GeoDataFrame(
        {"crash_sk": list(range(1, len(coords) + 1))},
        geometry=[Point(*c) if c else None for c in coords],
        crs=26985,
    )


def test_a_point_ten_metres_from_a_segment_snaps_with_a_linear_reference(network):
    pts = _projected_points([(400_250.0, 130_010.0)])
    out, stats = snap.snap_points(pts, network, epsg=26985,
                                  max_distance_m=50, search_distance_m=500)
    row = out.iloc[0]
    assert row["snap_status"] == snap.SNAP_SNAPPED
    assert row["osm_way_id"] == 100
    assert row["snap_distance_m"] == pytest.approx(10.0)
    assert row["segment_length_m"] == pytest.approx(1000.0)
    assert row["offset_m"] == pytest.approx(250.0)
    assert 0.0 <= row["offset_frac"] <= 1.0
    assert row["offset_frac"] == pytest.approx(0.25)
    assert row["osm_highway"] == "residential" and row["osm_maxspeed_mph"] == 25.0
    assert row["snap_crs_epsg"] == 26985
    assert stats["crs_epsg"] == 26985


def test_a_point_far_from_everything_is_rejected_with_null_attributes(network):
    pts = _projected_points([(400_500.0, 130_900.0)])
    out, _ = snap.snap_points(pts, network, epsg=26985,
                              max_distance_m=50, search_distance_m=500)
    row = out.iloc[0]
    assert row["snap_status"] == snap.SNAP_REJECTED
    assert pd.isna(row["osm_way_id"]) and pd.isna(row["osm_highway"])
    assert pd.isna(row["offset_m"])


def test_a_rejected_but_measured_point_keeps_its_distance(network):
    """150 m from both roads: rejected, but we know it was 150 m and not 4 km."""
    pts = _projected_points([(400_400.0, 130_150.0)])
    out, stats = snap.snap_points(pts, network, epsg=26985,
                                  max_distance_m=50, search_distance_m=500)
    assert out.iloc[0]["snap_status"] == snap.SNAP_REJECTED
    assert out.iloc[0]["snap_distance_m"] == pytest.approx(150.0)
    assert pd.isna(out.iloc[0]["osm_highway"])


def test_an_equidistant_tie_goes_to_the_lowest_way_id(network):
    pts = _projected_points([(400_400.0, 130_150.0)])
    out, stats = snap.snap_points(pts, network, epsg=26985,
                                  max_distance_m=200, search_distance_m=500)
    assert stats["ties_broken"] == 1
    assert out.iloc[0]["osm_way_id"] == 100


def test_a_null_geometry_gets_no_geometry_not_a_rejection(network):
    out, _ = snap.snap_points(_projected_points([None]), network, epsg=26985)
    assert out.iloc[0]["snap_status"] == snap.SNAP_NO_GEOMETRY
    assert pd.isna(out.iloc[0]["snap_crs_epsg"])


def test_the_linear_reference_round_trips_geodesically(network):
    """Interpolate back along the line and measure with pyproj on the ellipsoid."""
    pts = _projected_points([(400_250.0, 130_010.0), (400_800.0, 129_995.0)])
    out, _ = snap.snap_points(pts, network, epsg=26985,
                              max_distance_m=50, search_distance_m=500)
    check = snap.verify_linear_reference(
        pts.to_crs(4326), network.to_crs(4326), out, epsg=26985, sample=10
    )
    assert check["sampled"] == 2
    assert check["max_abs_delta_m"] < 0.5


def test_maxspeed_is_normalised_or_left_null_never_guessed():
    got = snap.normalise_maxspeed(
        pd.Series(["35 mph", "50", "60 km/h", "walk", None, "40 mph;30 mph"])
    )
    assert got[0] == 35.0
    assert got[1] == pytest.approx(31.1, abs=0.1)   # bare number is km/h
    assert got[2] == pytest.approx(37.3, abs=0.1)
    assert pd.isna(got[3]) and pd.isna(got[4]) and pd.isna(got[5])


def test_snap_crs_is_config_driven_per_jurisdiction():
    assert snap.snap_crs_for("MD") == 26985
    assert snap.snap_crs_for("TX") == 32139
    with pytest.raises(KeyError, match="no projected CRS"):
        snap.snap_crs_for("ZZ")


# ===========================================================================
# 7. the CRS negative control -- the only place 3857 may appear in tests/
# ===========================================================================


def test_a_buffer_built_in_web_mercator_is_wrong_by_one_over_cos_squared_latitude():
    """Why EPSG:3857 is banned, measured rather than asserted in prose.

    A 500 m buffer around a Baltimore-latitude point is built twice: once in
    EPSG:26985 (state plane, metres on the ground) and once in EPSG:3857, where
    "500" is 500 Mercator units and not 500 metres. Mercator's scale factor is
    1/cos(lat) in BOTH directions, so the area is inflated by 1/cos^2(lat) --
    about 1.67 at 39.3 deg N. The 3857 buffer is not slightly wrong, it is 67%
    too big, and a distance threshold built on it admits the wrong crashes.
    """
    lat, lon = 39.2904, -76.6122  # Baltimore
    point = gpd.GeoSeries([Point(lon, lat)], crs=4326)

    # Both measured in EPSG:5070 (equal-area) so the comparison is of AREA ON
    # THE GROUND and not of two different definitions of "square unit".
    honest = point.to_crs(26985).buffer(500).to_crs(5070)
    mercator = point.to_crs(3857).buffer(500).to_crs(5070)

    ratio = float(honest.area.iloc[0] / mercator.area.iloc[0])
    expected = 1.0 / np.cos(np.radians(lat)) ** 2
    assert expected == pytest.approx(1.67, abs=0.02)
    assert ratio == pytest.approx(expected, rel=0.01), (
        f"the honest buffer is {ratio:.3f}x the ground area of the 3857 one"
    )
    # The radius error is the 1/cos(lat) the assignment quotes from the other
    # direction: 500 Mercator units cover about 387 m on the ground here, so a
    # 500 m rejection threshold built in 3857 silently becomes a 387 m one.
    ground_radius = float(np.sqrt(mercator.area.iloc[0] / np.pi))
    assert ground_radius == pytest.approx(500 * np.cos(np.radians(lat)), rel=0.01)
    assert ground_radius == pytest.approx(387, abs=1)


def test_no_source_file_contains_3857_as_executable_code():
    """`grep -rn 3857 src/` may only ever hit comments and docstrings.

    Parsed, not grepped: comments never reach the AST, and a docstring that
    explains WHY Web Mercator is banned must stay legal. A numeric 3857
    constant anywhere in the tree is by construction a value the code would
    actually use -- a `to_crs(3857)` or an `epsg=3857` -- which is the thing
    ASSIGNMENT.md 3a says scores zero.
    """
    import ast

    offenders = []
    for path in sorted((Path(__file__).resolve().parents[1] / "src").rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Constant) and not isinstance(node.value, bool) \
                    and node.value == 3857:
                offenders.append(f"{path.name}:{node.lineno}")
    assert not offenders, f"3857 used as a value in: {offenders}"


# ===========================================================================
# 8. weather
# ===========================================================================

WEATHER_FIXTURE = next(FIXTURES.glob("open_meteo_*.json"))
WEATHER_CELL = WEATHER_FIXTURE.stem.split("_")[2]


class ExplodingClient:
    """Any HTTP call is a test failure. Used to prove the cache is doing the work."""

    def get(self, *a, **k):  # pragma: no cover - the point is that it never runs
        raise AssertionError("the weather stage made a network call with a warm cache")


def test_a_committed_response_parses_with_the_utc_index_and_the_grid_coordinate():
    payload = json.loads(WEATHER_FIXTURE.read_text())
    df = weather_mod.response_to_frame(WEATHER_CELL, payload)
    assert len(df) == 120  # five days, hourly
    assert str(df["era5_hour_utc"].dt.tz) == "UTC"
    assert set(weather_mod.era5_columns()) <= set(df.columns)
    # The grid coordinate the server returned is NOT the one we asked for; that
    # difference is the ~9-25 km resolution of the product, and it is stored.
    lat, lon = h3_index.h3.cell_to_latlng(WEATHER_CELL)
    assert df["era5_grid_lat"].iloc[0] != pytest.approx(lat, abs=1e-6)
    assert abs(df["era5_grid_lat"].iloc[0] - lat) < 0.5


def test_hour_alignment_across_the_spring_forward_transition_is_on_the_utc_key():
    """07:30Z is 02:30 EST-that-never-was; the ERA5 row for 07:00Z is the one."""
    payload = json.loads(WEATHER_FIXTURE.read_text())
    era5 = weather_mod.response_to_frame(WEATHER_CELL, payload)

    crash = tz.localise(datetime(2024, 3, 10, 2, 30), "America/New_York")
    key = pd.Timestamp(crash.utc).floor("h")
    assert key == pd.Timestamp("2024-03-10 07:00", tz="UTC")
    row = era5[era5["era5_hour_utc"] == key]
    assert len(row) == 1

    # And an hour of local wall clock either side of the transition is one hour
    # apart in UTC too -- which is only true because the join key is UTC.
    before = tz.localise(datetime(2024, 3, 10, 1, 30), "America/New_York").utc
    after = tz.localise(datetime(2024, 3, 10, 3, 30), "America/New_York").utc
    assert (after - before).total_seconds() == 3600


def test_a_warm_cache_makes_zero_network_calls(tmp_path):
    dest = weather_mod.cache_path(tmp_path, WEATHER_CELL, 2024)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(WEATHER_FIXTURE, dest)
    budget = weather_mod.RequestBudget()
    payload = weather_mod.fetch_cell_year(
        tmp_path, WEATHER_CELL, 2024, start=date(2024, 1, 1), end=date(2024, 12, 31),
        budget=budget, client=ExplodingClient(),
    )
    assert payload is not None
    assert budget.requests == 0 and budget.cache_hits == 1


def test_offline_with_a_cold_cache_fails_loudly_naming_the_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="not cached"):
        weather_mod.fetch_cell_year(
            tmp_path, WEATHER_CELL, 2024, start=date(2024, 1, 1),
            end=date(2024, 12, 31), budget=weather_mod.RequestBudget(),
            client=ExplodingClient(), offline=True,
        )


def test_a_non_utc_response_is_refused_rather_than_silently_joined():
    payload = json.loads(WEATHER_FIXTURE.read_text())
    payload["timezone"] = "America/New_York"
    with pytest.raises(ValueError, match="expected UTC"):
        weather_mod.response_to_frame(WEATHER_CELL, payload)


def test_the_request_budget_refuses_to_exceed_the_documented_daily_limit():
    budget = weather_mod.RequestBudget()
    budget.requests = budget.per_day
    with pytest.raises(RuntimeError, match="daily budget exhausted"):
        budget.spend()


def test_precipitation_agreement_is_a_confusion_matrix_not_an_accuracy():
    df = pd.DataFrame({
        "weather_status": [weather_mod.WEATHER_JOINED] * 4,
        "era5_precipitation_mm": [0.0, 0.2, 0.0, 1.0],
        "officer_is_precipitation": [False, True, True, False],
    })
    got = weather_mod.precipitation_agreement(df)
    assert got["both_wet"] == 1 and got["both_dry"] == 1
    assert got["officer_wet_era5_dry"] == 1 and got["officer_dry_era5_wet"] == 1
    assert got["agreement_rate"] == 0.5


# ===========================================================================
# 9. ACS
# ===========================================================================


def test_acs_annotation_values_become_null_not_a_population():
    for annotation in sorted(reference.ACS_ANNOTATIONS):
        assert census_join._clean_estimate(annotation) is None
    assert census_join._clean_estimate("1234") == 1234
    assert census_join._clean_estimate(None) is None
    assert census_join._clean_estimate("") is None


def _non_docstring_strings(path: Path) -> list[str]:
    """Every string literal in a module except its docstrings.

    Docstrings are where the proxy variables are NAMED, at length, to explain
    why they are absent -- so a grep would fail and an AST walk that keeps them
    would too. What matters is that none of them appears in a string the code
    could put in a URL.
    """
    import ast

    tree = ast.parse(path.read_text())
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and \
                    isinstance(body[0].value, ast.Constant) and \
                    isinstance(body[0].value.value, str):
                docstrings.add(id(body[0].value))
    return [
        n.value for n in ast.walk(tree)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
        and id(n) not in docstrings
    ]


def test_only_population_is_ever_requested_from_acs():
    """The Part 4 control: the proxy variables exist nowhere the code can use.

    Income, tenure, vehicles and commute are strong proxies for protected
    classes. The strongest guarantee that they do not leak into a lead score is
    that the pipeline never loads them, and this test is what keeps that true
    when somebody later adds "just one more useful variable".
    """
    assert config.geo()["reference"]["acs_variable"] == "B01003_001E"
    literals = (_non_docstring_strings(Path(reference.__file__))
                + _non_docstring_strings(Path(census_join.__file__)))
    for proxy in ("B19013", "B25044", "B08301", "B08303", "B17001", "B02001"):
        assert not [s for s in literals if proxy in s], proxy
    # ... and the request the code actually builds asks for population only.
    url = reference.acs_api_url("24")
    assert "B01003_001E" in url and url.count("get=") == 1
    assert "&key=" not in url  # the key is appended by the caller, never logged


# ===========================================================================
# 10. integration: the real builder over the session's gold
# ===========================================================================


def test_every_fact_crash_row_has_exactly_one_crash_geo_row(geo_con):
    fact = _one(geo_con, "SELECT COUNT(*) FROM geo_fact_crash")
    rows = _one(geo_con, "SELECT COUNT(*) FROM geo_crash_geo")
    distinct = _one(geo_con, "SELECT COUNT(DISTINCT crash_sk) FROM geo_crash_geo")
    assert fact == rows == distinct
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_fact_crash f
        WHERE NOT EXISTS (SELECT 1 FROM geo_crash_geo g WHERE g.crash_sk = f.crash_sk)
    """) == 0


def test_every_row_leaves_with_a_timezone_and_a_provenance(geo_con):
    """Phase 6 reads tz_source; UNRESOLVED would mean a row it cannot judge."""
    assert _one(geo_con, "SELECT COUNT(*) FROM geo_crash_geo WHERE tz_iana IS NULL") == 0
    assert _one(geo_con, "SELECT COUNT(*) FROM geo_crash_geo "
                         "WHERE tz_source = 'UNRESOLVED'") == 0


def test_a_row_with_a_local_time_has_a_utc_stamp_and_one_without_says_so(geo_con):
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo
        WHERE crash_datetime_local IS NOT NULL AND crash_datetime_utc IS NULL
    """) == 0
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo
        WHERE crash_datetime_local IS NULL AND time_status <> 'TIME_UNKNOWN'
    """) == 0
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo
        WHERE time_status = 'TIME_UNKNOWN' AND crash_datetime_utc IS NOT NULL
    """) == 0


def test_a_time_unknown_row_still_has_its_crash_date(geo_con):
    """FARS HOUR=99: the date is real, only the hour is missing."""
    assert _one(geo_con, "SELECT COUNT(*) FROM geo_crash_geo "
                         "WHERE time_status = 'TIME_UNKNOWN' AND crash_date IS NULL") == 0


def test_every_geocoded_row_has_a_block_group_tract_and_h3(geo_con):
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo
        WHERE pip_status = 'MATCHED'
          AND (bg_geoid IS NULL OR tract_geoid IS NULL
               OR h3_r9 IS NULL OR h3_r8 IS NULL OR h3_r7 IS NULL)
    """) == 0
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo
        WHERE geo_quality = 'OK' AND h3_r9 IS NULL
    """) == 0


def test_the_tract_is_the_block_group_prefix_on_every_matched_row(geo_con):
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo
        WHERE bg_geoid IS NOT NULL AND tract_geoid <> substr(bg_geoid, 1, 11)
    """) == 0


def test_h3_parents_are_consistent_on_real_rows(geo_con, geo_manifest):
    rows = geo_con.execute(
        "SELECT h3_r9, h3_r8, h3_r7 FROM geo_crash_geo WHERE h3_r9 IS NOT NULL LIMIT 5000"
    ).fetchall()
    assert rows
    for r9, r8, r7 in rows:
        assert h3_index.parent(r9, 8) == r8
        assert h3_index.parent(r9, 7) == r7
    assert geo_manifest["stats"]["h3"]["parent_invariant_checked"] is True


def test_geometry_is_null_exactly_where_the_coordinate_is(geo_con):
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo g JOIN geo_fact_crash f USING (crash_sk)
        WHERE (g.geometry IS NULL) <> (f.latitude IS NULL)
    """) == 0
    assert _one(geo_con, "SELECT COUNT(*) FROM geo_crash_geo "
                         "WHERE geometry IS NULL AND pip_status <> 'NO_GEOMETRY'") == 0


def test_the_geometry_matches_the_facts_coordinate(geo_con):
    """Not a re-projection, not a re-geocode: the same number, as a POINT."""
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo g JOIN geo_fact_crash f USING (crash_sk)
        WHERE g.geometry IS NOT NULL
          AND (abs(ST_X(g.geometry) - f.longitude) > 1e-9
               OR abs(ST_Y(g.geometry) - f.latitude) > 1e-9)
    """) == 0


def test_the_county_refinement_never_changes_silvers_geo_quality(geo_con):
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo g JOIN geo_fact_crash f USING (crash_sk)
        WHERE g.geo_quality <> f.geo_quality
    """) == 0


def test_a_pinned_rockville_coordinate_lands_on_its_pinned_2025_geography(
    geo_reference_store,
):
    """The integration counterpart of the committed-clip test: full TIGER."""
    bgs = census_join.load_block_groups(geo_reference_store)
    counties = census_join.load_counties(geo_reference_store)
    pts = _points([(ROCKVILLE[1], ROCKVILLE[0])])
    bg, _ = census_join.point_in_polygon(pts, bgs, key="crash_sk", out_col="bg_geoid")
    county, _ = census_join.point_in_polygon(pts, counties, key="crash_sk",
                                             out_col="pip_county_geoid")
    assert bg.loc[0, "bg_geoid"] == ROCKVILLE_BG
    assert county.loc[0, "pip_county_geoid"] == ROCKVILLE_COUNTY


def test_dim_block_group_covers_the_three_states_with_population(geo_con):
    states = dict(geo_con.execute(
        "SELECT state_fips, COUNT(*) FROM geo_dim_block_group GROUP BY 1 ORDER BY 1"
    ).fetchall())
    assert set(states) == {"12", "24", "48"}
    assert _one(geo_con, "SELECT COUNT(*) FROM geo_dim_block_group "
                         "WHERE tract_geoid <> substr(bg_geoid, 1, 11)") == 0
    # Density is population over STORED land area, and is NULL (not infinite)
    # for a water-only block group.
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_dim_block_group
        WHERE aland_m2 = 0 AND density_per_km2 IS NOT NULL
    """) == 0
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_dim_block_group
        WHERE aland_m2 > 0 AND population IS NOT NULL
          AND abs(density_per_km2 - population / (aland_m2 / 1e6)) > 1e-3
    """) == 0


def test_every_matched_row_points_at_a_real_block_group(geo_con):
    assert _one(geo_con, """
        SELECT COUNT(*) FROM geo_crash_geo g
        WHERE g.pip_status = 'MATCHED'
          AND NOT EXISTS (SELECT 1 FROM geo_dim_block_group d
                          WHERE d.bg_geoid = g.bg_geoid)
    """) == 0


# ===========================================================================
# 11. GeoParquet
# ===========================================================================


def _geo_metadata(path: Path) -> dict:
    import pyarrow.parquet as pq

    return json.loads(pq.ParquetFile(path).schema_arrow.metadata[b"geo"])


def test_the_flat_file_is_geoparquet_1_1_0_with_a_bbox_covering_column(geo_root):
    meta = _geo_metadata(geo_root / "crash_geo.parquet")
    assert meta["version"] == "1.1.0"
    assert meta["primary_column"] == "geometry"
    column = meta["columns"]["geometry"]
    assert column["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]
    assert column["encoding"] == "WKB"
    assert column["crs"]["id"] == {"authority": "EPSG", "code": 4326}


def test_every_partition_file_is_also_geoparquet_1_1_0(geo_root):
    parts = sorted((geo_root / "crash_geo").rglob("*.parquet"))
    assert parts, "the partitioned dataset was not written"
    for p in parts:
        meta = _geo_metadata(p)
        assert meta["version"] == "1.1.0"
        assert "covering" in meta["columns"][meta["primary_column"]]
    # jurisdiction=XX/year=YYYY
    assert all(p.parent.parent.name.startswith("jurisdiction=") for p in parts)
    assert all(p.parent.name.startswith("year=") for p in parts)


def test_a_duckdb_bbox_query_returns_the_same_rows_as_a_brute_force_filter(geo_con,
                                                                           geo_root):
    """The covering bbox must prune, not change the answer."""
    root = str(geo_root / "crash_geo").replace("'", "''")
    box = (-77.20, 39.05, -77.05, 39.15)
    via_bbox = geo_con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{root}/**/*.parquet', hive_partitioning=0)
        WHERE bbox.xmin >= {box[0]} AND bbox.ymin >= {box[1]}
          AND bbox.xmax <= {box[2]} AND bbox.ymax <= {box[3]}
    """).fetchone()[0]
    brute = geo_con.execute(f"""
        SELECT COUNT(*) FROM geo_crash_geo g JOIN geo_fact_crash f USING (crash_sk)
        WHERE f.longitude BETWEEN {box[0]} AND {box[2]}
          AND f.latitude BETWEEN {box[1]} AND {box[3]}
    """).fetchone()[0]
    assert via_bbox == brute


def test_the_partitioned_copy_holds_the_same_rows_as_the_flat_file(geo_con, geo_root):
    root = str(geo_root / "crash_geo").replace("'", "''")
    assert geo_con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT crash_sk FROM read_parquet('{root}/**/*.parquet', hive_partitioning=0)
            EXCEPT SELECT crash_sk FROM geo_crash_geo
        )""").fetchone()[0] == 0
    assert geo_con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{root}/**/*.parquet', hive_partitioning=0)"
    ).fetchone()[0] == _one(geo_con, "SELECT COUNT(*) FROM geo_crash_geo")


def test_rows_are_sorted_by_h3_inside_each_partition_so_the_bbox_prunes(geo_root):
    """A random row order makes every row group's bbox the whole partition."""
    import pyarrow.parquet as pq

    for p in sorted((geo_root / "crash_geo").rglob("*.parquet")):
        cells = pq.read_table(p, columns=["h3_r9"])["h3_r9"].to_pylist()
        present = [c for c in cells if c is not None]
        assert present == sorted(present), p
        # NULL cells sort first, into their own row groups.
        assert all(c is None for c in cells[:len(cells) - len(present)])


# ===========================================================================
# 12. contracts
# ===========================================================================


def _logical_crash_geo(con, name: str = "geo_crash_geo") -> str:
    """The contract's view of crash_geo: no `bbox`, geometry as WKB.

    `bbox` is a physical GeoParquet artefact -- the covering column the format
    adds so a reader can prune -- and `geometry` comes back from DuckDB as its
    own GEOMETRY type. The contract describes the LOGICAL table, which is what
    `build_geo` validates before it writes, so the test validates the same
    shape rather than a different one that happens to be on disk.
    """
    cols = ", ".join(
        "ST_AsWKB(geometry) AS geometry" if c == "geometry" else f'"{c}"'
        for c in geo_build.CRASH_GEO_COLUMNS
    )
    con.execute(f"CREATE OR REPLACE VIEW {name}_logical AS SELECT {cols} FROM {name}")
    return f"{name}_logical"


def test_crash_geo_validates_against_the_gold_contract(geo_con):
    violations = contracts.validate_relation(
        geo_con, _logical_crash_geo(geo_con),
        contracts.load_contract(geo_build.GOLD_CONTRACT),
        "gold.crash_geo", check_row_count_min=using_full_bronze(),
    )
    assert not violations, [v.render() for v in violations]


def test_dim_block_group_validates_against_the_gold_contract(geo_con):
    violations = contracts.validate_relation(
        geo_con, "geo_dim_block_group",
        contracts.load_contract(geo_build.GOLD_CONTRACT), "gold.dim_block_group",
    )
    assert not violations, [v.render() for v in violations]


def test_a_matched_row_with_a_null_block_group_fails_naming_table_and_column(geo_con):
    """The constraint that makes `bg_geoid IS NULL` legitimate only when it is."""
    geo_con.execute("""
        CREATE OR REPLACE TABLE broken_crash_geo AS
        SELECT * REPLACE (NULL::VARCHAR AS bg_geoid) FROM geo_crash_geo
        WHERE pip_status = 'MATCHED' LIMIT 5
    """)
    geo_con.execute("CREATE OR REPLACE VIEW bg_ref AS SELECT * FROM geo_dim_block_group")
    violations = contracts.validate_foreign_keys(
        geo_con, contracts.load_contract(geo_build.GOLD_CONTRACT), "gold.crash_geo",
        "broken_crash_geo", {"gold.dim_block_group": "bg_ref"},
    )
    assert len(violations) == 1
    assert violations
    rendered = "\n".join(v.render() for v in violations)
    assert "gold.crash_geo" in rendered and "bg_geoid" in rendered


def test_the_column_order_is_the_contract(geo_con):
    described = [r[0] for r in
                 geo_con.execute("DESCRIBE SELECT * FROM geo_crash_geo").fetchall()]
    # GeoParquet adds the covering `bbox` struct; it is a physical artefact of
    # the format, not a contract column, so it is excluded and then asserted.
    assert "bbox" in described
    assert [c for c in described if c != "bbox"] == geo_build.CRASH_GEO_COLUMNS


# ===========================================================================
# 13. determinism and restatement
# ===========================================================================


def _hash_tree(root: Path) -> dict[str, str]:
    import hashlib

    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*.parquet"))
    }


@pytest.fixture
def geo_runner(tmp_path, gold_root: Path, geo_reference_store):
    def run(name: str, mutate=None) -> tuple[Path, dict[str, str]]:
        dest = tmp_path / name
        dest.mkdir(parents=True, exist_ok=True)
        for p in sorted(gold_root.glob("*.parquet")):
            shutil.copy2(p, dest / p.name)
        shutil.copy2(gold_root / "_build_manifest.json", dest / "_build_manifest.json")
        if mutate is not None:
            mutate(dest)
        geo_build.build_geo(
            gold_root=dest, reference_root=reference.reference_root(),
            skip_snap=True, skip_weather=True,
            small_corpus=not using_full_bronze(),
        )
        return dest, _hash_tree(dest)

    return run


def test_two_builds_over_the_same_gold_are_byte_identical(geo_runner):
    _, first = geo_runner("a")
    _, second = geo_runner("b")
    geo_only = {k: v for k, v in first.items() if "crash_geo" in k or "block_group" in k}
    assert geo_only
    assert geo_only == {k: v for k, v in second.items()
                        if "crash_geo" in k or "block_group" in k}


def test_changing_one_fact_row_changes_exactly_one_crash_geo_row(geo_runner):
    """The restatement guarantee: a moved coordinate moves one row and no other."""
    import duckdb

    first_root, _ = geo_runner("base")
    target = duckdb.connect().execute(
        f"SELECT crash_sk FROM read_parquet('{first_root / 'fact_crash.parquet'}') "
        "WHERE geo_quality = 'OK' ORDER BY crash_sk LIMIT 1"
    ).fetchone()[0]

    def move_one(dest: Path) -> None:
        con = duckdb.connect()
        src = str(dest / "fact_crash.parquet").replace("'", "''")
        con.execute(f"""
            COPY (SELECT * REPLACE (
                      CASE WHEN crash_sk = {target} THEN latitude + 0.01
                           ELSE latitude END AS latitude)
                  FROM read_parquet('{src}') ORDER BY crash_sk)
            TO '{src}.new' (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 122880)
        """)
        Path(f"{src}.new").replace(dest / "fact_crash.parquet")

    second_root, _ = geo_runner("moved", mutate=move_one)

    con = duckdb.connect()
    a = str(first_root / "crash_geo.parquet").replace("'", "''")
    b = str(second_root / "crash_geo.parquet").replace("'", "''")
    # `_geo_build_sha` hashes the inputs, so it moves for EVERY row when
    # fact_crash changes -- by design, and excluded from the row diff the same
    # way Phase 3 excludes `_silver_build_sha`.
    changed = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT * EXCLUDE (_geo_build_sha, bbox) FROM read_parquet('{a}')
            EXCEPT
            SELECT * EXCLUDE (_geo_build_sha, bbox) FROM read_parquet('{b}')
        )""").fetchone()[0]
    assert changed == 1, "moving one coordinate must change exactly one crash_geo row"
    assert con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{a}')"
    ).fetchone()[0] == con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{b}')"
    ).fetchone()[0]


def test_the_build_sha_is_a_function_of_inputs_not_of_the_clock():
    a = geo_build.build_sha("fact", {"tiger/bg.zip": "abc"})
    b = geo_build.build_sha("fact", {"tiger/bg.zip": "abc"})
    c = geo_build.build_sha("fact", {"tiger/bg.zip": "def"})
    assert a == b and a != c


def test_the_manifest_records_every_reference_hash_and_the_stage_counts(geo_manifest):
    assert geo_manifest["reference_files"], "no reference file hashes recorded"
    for info in geo_manifest["reference_files"].values():
        assert len(info["sha256"]) == 64
    for stage in ("county_refinement", "block_group_pip", "h3", "timezone",
                  "snap", "weather", "reconciliation", "dim_block_group"):
        assert stage in geo_manifest["stats"], stage
    assert "requests" in geo_manifest["stats"]["weather"]["budget"]
    assert geo_manifest["stats"]["reconciliation"]["missing_from_crash_geo"] == 0
    # The census key must never reach the manifest.
    blob = json.dumps(geo_manifest)
    assert "&key=" not in blob


def test_offline_with_missing_reference_data_fails_loudly_naming_the_file(tmp_path):
    store = reference.ReferenceStore(tmp_path / "empty", offline=True)
    with pytest.raises(reference.MissingReference) as exc:
        store.ensure("tiger/2025/BG/tl_2025_24_bg.zip", "https://example.invalid/x.zip")
    assert "tl_2025_24_bg.zip" in str(exc.value)
    assert "python -m src.geo.reference" in str(exc.value)


def test_skip_snap_still_produces_a_typed_schema_with_a_stated_reason(geo_con,
                                                                     geo_manifest):
    assert geo_manifest["stats"]["snap"]["enabled"] is False
    statuses = dict(geo_con.execute(
        "SELECT snap_status, COUNT(*) FROM geo_crash_geo GROUP BY 1"
    ).fetchall())
    assert set(statuses) <= {"NOT_ATTEMPTED", "NO_GEOMETRY"}
    assert _one(geo_con, "SELECT COUNT(*) FROM geo_crash_geo "
                         "WHERE snap_status = 'NOT_ATTEMPTED' AND osm_way_id IS NOT NULL") == 0


# ===========================================================================
# 14. snapping against the real OSM network (opt-in: needs the 203 MB extract)
# ===========================================================================


def _road_cache() -> Path | None:
    store = reference.ReferenceStore(reference.reference_root(), offline=True)
    pbf = store.root / reference.osm_relpath("maryland")
    if not pbf.exists():
        return None
    return snap.road_network_path(store, "maryland", snap.clip_bbox())


@pytest.mark.skipif(_road_cache() is None or not _road_cache().exists(),
                    reason="no cached OSM road network -- run `python -m src.geo.build` "
                           "once to build it from the Geofabrik extract")
def test_the_real_road_network_snaps_montgomery_crashes_within_the_threshold(geo_con):
    roads = gpd.read_parquet(_road_cache())
    assert len(roads) > 1000 and roads.crs.to_epsg() == 4326
    assert set(roads["osm_highway"]) <= set(config.geo()["snap"]["highway_values"])

    df = geo_con.execute("""
        SELECT f.crash_sk, f.latitude, f.longitude FROM geo_fact_crash f
        WHERE f.primary_source_system = 'MONTGOMERY_MD' AND f.geo_quality = 'OK'
        ORDER BY f.crash_sk LIMIT 500
    """).df()
    if df.empty:
        pytest.skip("no geocoded Montgomery rows in this corpus")
    pts = census_join.points_frame(df)
    out, stats = snap.snap_points(pts, roads, epsg=26985)
    assert stats["snapped"] > 0
    accepted = out[out["snap_status"] == snap.SNAP_SNAPPED]
    assert (accepted["snap_distance_m"] <= config.geo()["snap"]["max_distance_m"]).all()
    assert accepted["offset_frac"].between(0, 1).all()
    check = snap.verify_linear_reference(pts, roads, out, epsg=26985, sample=200)
    # State-plane scale error over Montgomery County is < 1e-4, so a metre of
    # tolerance is generous; anything larger would mean the offset is wrong.
    assert check["max_abs_delta_m"] < 1.0

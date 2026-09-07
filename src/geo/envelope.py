"""The polygon refinement of silver's bounding-box check.

`config/geo.toml`'s header makes a promise: each envelope is a STRICT SUPERSET
of its jurisdiction polygon, so a point outside the box is definitively outside
the jurisdiction and no polygon test can rescue it -- while a point inside is
only a CANDIDATE. This module is the other half of that promise. It refines what
the bbox accepted; it never revisits what the bbox rejected.

Concretely, for every `geo_quality = 'OK'` crash it answers two questions the
bbox could not:

  `pip_county_geoid`          which TIGER 2025 county the point actually falls in
  `county_agrees_with_source` whether that is the county the source said

and it does NOT touch `geo_quality`. Silver's tier is a statement about the
coordinate's plausibility against a declared envelope, computed with no
reference data and no network; this is a finer, additional tier that needs a
250 MB polygon set. Overwriting the first with the second would destroy the
property that makes silver auditable on its own -- and would mean a TIGER
re-vintage could silently move rows between quality classes.

Two populations are worth naming separately, because they mean different things:

  * **Inside the padded Montgomery envelope, outside Montgomery County.** These
    are exactly the rows the config header predicted: the envelope is padded
    0.02 deg beyond the county, so a crash reported by a Montgomery agency but
    located in Howard, Frederick or the District lands here. Reported by the
    county they are actually in.
  * **Source county disagrees with the polygon.** TxDOT publishes `cnty_id`
    and FARS publishes `COUNTY`; both are the reporting county, which is not
    always the county the coordinate is in (a crash on a county line, a report
    filed by the responding agency's county). Counted, never corrected --
    `fact_crash.geography_sk` stays the source's own answer and `crash_geo`
    carries the polygon's, so a consumer can pick and a reviewer can see the
    gap.

CRS: EPSG:4326 throughout, no projection. Point-in-polygon is topological; see
`src/geo/census_join.py` for the argument. The only metric number produced here
is the distance from the envelope, and that is silver's, computed geodesically
on the WGS84 ellipsoid by `common.GEOD`.
"""

from __future__ import annotations

import logging
from typing import Any

import geopandas as gpd
import pandas as pd

from .. import config
from . import census_join
from . import reference

log = logging.getLogger("geo.envelope")


def source_county_geoid(geography_sk: pd.Series) -> pd.Series:
    """`fact_crash.geography_sk` -> a 5-character county GEOID, or NA.

    Phase 3 made `geography_sk` the integer county GEOID (24031), with -1 for
    UNKNOWN and the 2-digit state FIPS for STATE-level members. Only the
    5-digit values are counties; anything else has no county to compare against
    and becomes NA rather than a zero-padded lie.
    """
    s = pd.to_numeric(geography_sk, errors="coerce")
    ok = s.notna() & (s >= 1001) & (s <= 99999)
    out = pd.Series(pd.NA, index=geography_sk.index, dtype="string")
    out[ok] = s[ok].astype("int64").astype(str).str.zfill(5)
    return out


def refine(
    points: gpd.GeoDataFrame,
    counties: gpd.GeoDataFrame,
    *,
    key: str = "crash_sk",
    source_county_col: str = "source_county_geoid",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """PIP every geocoded crash to its TIGER county. Returns (frame, stats).

    The frame has `pip_county_geoid`, `county_pip_status` and
    `county_agrees_with_source` (NULL when either side is unknown -- "we cannot
    tell" is a third answer and collapsing it to False would inflate the
    disagreement count with rows that never had a source county).
    """
    result, stats = census_join.point_in_polygon(
        points, counties, key=key, out_col="pip_county_geoid"
    )
    result = result.rename(columns={"pip_status": "county_pip_status"})
    merged = result.merge(points[[key, source_county_col]], on=key, how="left")

    both = merged["pip_county_geoid"].notna() & merged[source_county_col].notna()
    agrees = pd.Series(pd.NA, index=merged.index, dtype="boolean")
    agrees[both] = (
        merged.loc[both, "pip_county_geoid"].astype(str)
        == merged.loc[both, source_county_col].astype(str)
    )
    merged["county_agrees_with_source"] = agrees

    disagreements = merged[both & ~agrees.fillna(True)]
    stats = {
        **stats,
        "comparable_rows": int(both.sum()),
        "county_agrees": int(agrees.fillna(False).sum()),
        "county_disagrees": int(len(disagreements)),
        "county_not_comparable": int((~both).sum()),
        "disagreement_pairs": _top_pairs(disagreements, source_county_col),
    }
    return (
        merged[[key, "pip_county_geoid", "county_pip_status",
                "county_agrees_with_source"]],
        stats,
    )


def _top_pairs(df: pd.DataFrame, source_col: str, limit: int = 20) -> list[dict[str, Any]]:
    if df.empty:
        return []
    pairs = (
        df.groupby([source_col, "pip_county_geoid"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["rows", source_col, "pip_county_geoid"],
                     ascending=[False, True, True])
        .head(limit)
    )
    return [
        {"source_county": str(r[source_col]),
         "pip_county": str(r["pip_county_geoid"]), "rows": int(r["rows"])}
        for _, r in pairs.iterrows()
    ]


def bbox_versus_polygon(
    merged: pd.DataFrame,
    *,
    source_system: str = "MONTGOMERY_MD",
    envelope_name: str = "montgomery",
    county_geoid: str = "24031",
    source_col: str = "primary_source_system",
) -> dict[str, Any]:
    """What the padded envelope let in that the county polygon does not contain.

    This is the number `config/geo.toml` promised and never had the data to
    produce: rows the bbox accepted (`geo_quality = 'OK'`, inside the padded
    Montgomery box by construction) that the TIGER county polygon puts
    somewhere else, broken out by where they actually are.

    A row here is not a data error. The envelope is padded 0.02 deg on purpose
    so the superset property is true rather than approximately true, and the
    price of a superset is admitting a margin. This measures the margin.
    """
    env = config.envelope(envelope_name)
    subset = merged[merged[source_col] == source_system]
    outside = subset[
        subset["pip_county_geoid"].notna()
        & (subset["pip_county_geoid"].astype(str) != county_geoid)
    ]
    no_polygon = subset[
        (subset["county_pip_status"] == census_join.PIP_NO_POLYGON)
    ]
    by_county = (
        outside.groupby("pip_county_geoid").size().sort_values(ascending=False)
        if not outside.empty else pd.Series(dtype="int64")
    )
    return {
        "envelope": {k: env[k] for k in
                     ("min_lat", "max_lat", "min_lon", "max_lon", "label")},
        "geocoded_rows": int(len(subset)),
        "in_county_polygon": int(len(subset) - len(outside) - len(no_polygon)),
        "in_envelope_outside_county": int(len(outside)),
        "in_envelope_no_county_polygon": int(len(no_polygon)),
        "by_actual_county": {str(k): int(v) for k, v in by_county.items()},
    }


def load_counties(store: reference.ReferenceStore) -> gpd.GeoDataFrame:
    """The in-scope TIGER county polygons. Thin wrapper so callers import one name."""
    return census_join.load_counties(store)

"""The four Montgomery County defects.

Each test must FAIL against the raw bronze layer and PASS against your silver
layer. That is the point: a test that only passes proves nothing about whether
your transform did anything.

Fill in the assertions. The expected values are for you to discover — they are
reported in your DATA_QUALITY.md, and we check them against ground truth.
"""

import pytest


@pytest.mark.xfail(reason="must fail on bronze")
def test_coordinates_within_montgomery_envelope(bronze_incidents):
    """bhju-22kf has zero null and zero zero-valued coordinates, and still
    contains records well outside the county. A null check does not catch this.

    Montgomery County envelope is roughly lat 38.9-39.36, lon -77.54 to -76.87.
    """
    raise NotImplementedError


@pytest.mark.xfail(reason="must fail on bronze")
def test_substance_abuse_dictionary_normalised(bronze_drivers):
    """driver_substance_abuse mixes an old uppercase single-value scheme with a
    newer comma-joined pair scheme, and carries at least three distinct
    spellings of null across the two.
    """
    raise NotImplementedError


@pytest.mark.xfail(reason="must fail on bronze")
def test_dictionary_cutover_overlap_handled(bronze_drivers):
    """The two encodings coexist for several days. A hardcoded cutover date is
    wrong. Find the overlap window and prove your handling covers it.
    """
    raise NotImplementedError


@pytest.mark.xfail(reason="must fail on bronze")
def test_incidents_drivers_report_number_reconciliation(bronze_incidents, bronze_drivers):
    """The two tables do not agree on the set of report_numbers. An inner join
    silently drops crashes. Quantify with an anti-join in both directions.
    """
    raise NotImplementedError


def test_crash_fact_grain_is_one_row_per_crash(silver_crash_fact):
    """Drivers is one row per driver but carries denormalised crash-level
    attributes. Aggregating off it overcounts multi-vehicle crashes.
    """
    raise NotImplementedError


def test_fars_sentinel_coordinates_excluded(silver_crash_fact):
    """FARS encodes unknown coordinates as 77.7777 / 88.8888 / 99.9999."""
    raise NotImplementedError


def test_pipeline_is_idempotent_under_restatement(pipeline_runner):
    """Re-running over a date range that includes an amended TxDOT report must
    produce byte-identical output.
    """
    raise NotImplementedError

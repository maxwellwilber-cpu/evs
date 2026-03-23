"""
tests/test_stage_3v.py
Pytest tests for Stage 3V checks against the sample data.

Run from evs/ directory:
    /usr/bin/python3 -m pytest tests/test_stage_3v.py -v
"""

from core.loader import load_data_pack
from validators.stage_3v import run_stage_3v, ALL_CHECKS


def get_findings():
    """Load sample data and run 3V checks."""
    pack = load_data_pack("data")
    return run_stage_3v(pack)


def test_3v_runs_all_checks():
    """Verify all 11 active 3V checks are registered."""
    assert len(ALL_CHECKS) == 11


def test_3v_finding_count():
    """3V should produce exactly 5 findings against sample data."""
    findings = get_findings()
    assert len(findings) == 5


def test_3v_all_blockers():
    """All 3V findings should be BLOCKERs."""
    findings = get_findings()
    assert all(f.severity == "BLOCKER" for f in findings)


def test_3v_03_duplicate_pk():
    """SCV-3V-03: M001 appears twice with same PK."""
    findings = get_findings()
    f03 = [f for f in findings if f.check_id == "SCV-3V-03"]
    assert len(f03) == 1
    assert f03[0].table_name == "METRICS_DB"
    assert f03[0].fail_count == 2  # 2 rows share the duplicate PK


def test_3v_04_unapproved_metric():
    """SCV-3V-04: M999 is not in the approved dictionary."""
    findings = get_findings()
    f04 = [f for f in findings if f.check_id == "SCV-3V-04"]
    assert len(f04) == 1
    assert "M999" in f04[0].message


def test_3v_09_negative_amount():
    """SCV-3V-09: PNL_FACT has -1500.00 on Equipment Repair."""
    findings = get_findings()
    f09 = [f for f in findings if f.check_id == "SCV-3V-09"]
    assert len(f09) == 1
    assert f09[0].table_name == "PNL_FACT"
    assert f09[0].fail_count == 1


def test_3v_11_calculated_null_value():
    """SCV-3V-11: M020 is Calculated but has null value."""
    findings = get_findings()
    f11_value = [f for f in findings
                 if f.check_id == "SCV-3V-11" and f.column_name == "value"]
    assert len(f11_value) == 1
    assert "M020" in str(f11_value[0].sample_failing_rows)


def test_3v_11_notcalculated_empty_notes():
    """SCV-3V-11: M050 is NotCalculated but has empty calc_notes."""
    findings = get_findings()
    f11_notes = [f for f in findings
                 if f.check_id == "SCV-3V-11" and f.column_name == "calc_notes"]
    assert len(f11_notes) == 1
    assert "M050" in str(f11_notes[0].sample_failing_rows)


def test_3v_01_metrics_present():
    """SCV-3V-01 should pass — METRICS_DB exists and proceed_status=PROCEED."""
    findings = get_findings()
    f01 = [f for f in findings if f.check_id == "SCV-3V-01"]
    assert len(f01) == 0


def test_3v_10_pct_in_range():
    """SCV-3V-10 should pass — unclassified_pct=0.02 is within [0,1]."""
    findings = get_findings()
    f10 = [f for f in findings if f.check_id == "SCV-3V-10"]
    assert len(f10) == 0

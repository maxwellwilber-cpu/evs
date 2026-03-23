"""
tests/test_stage_2v.py
Pytest tests for Stage 2V checks against the sample data.

Run from evs/ directory:
    /usr/bin/python3 -m pytest tests/test_stage_2v.py -v
"""

from core.loader import load_data_pack
from validators.stage_2v import run_stage_2v, ALL_CHECKS


def get_findings():
    """Load sample data and run 2V checks."""
    pack = load_data_pack("data")
    return run_stage_2v(pack)


def test_2v_runs_all_checks():
    """Verify all 9 active 2V checks are registered."""
    assert len(ALL_CHECKS) == 9


def test_2v_finding_count():
    """2V should produce exactly 1 finding against sample data."""
    findings = get_findings()
    assert len(findings) == 1


def test_2v_07_partial_period_overlap():
    """SCV-2V-07 should WARN about partial period overlap.
    PNL_FACT has {2025-01, 2025-02} but BS_FACT only has {2025-01}.
    """
    findings = get_findings()
    f07 = [f for f in findings if f.check_id == "SCV-2V-07"]
    assert len(f07) == 1
    assert f07[0].severity == "WARN"
    assert "period_key" in f07[0].column_name


def test_2v_no_blockers():
    """2V should produce zero BLOCKERs against sample data.
    All planted errors are in 3V/5V/8V territory.
    """
    findings = get_findings()
    blockers = [f for f in findings if f.severity == "BLOCKER"]
    assert len(blockers) == 0


def test_2v_01_tables_present():
    """SCV-2V-01 should pass — both required tables exist."""
    findings = get_findings()
    f01 = [f for f in findings if f.check_id == "SCV-2V-01"]
    assert len(f01) == 0


def test_2v_08_schema_match():
    """SCV-2V-08 should pass — all 2V tables match SCHEMA_REGISTRY."""
    findings = get_findings()
    f08 = [f for f in findings if f.check_id == "SCV-2V-08"]
    assert len(f08) == 0


def test_2v_09_no_duplicate_columns():
    """SCV-2V-09 should pass — no duplicate column headers."""
    findings = get_findings()
    f09 = [f for f in findings if f.check_id == "SCV-2V-09"]
    assert len(f09) == 0

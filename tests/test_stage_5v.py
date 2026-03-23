"""
tests/test_stage_5v.py
Pytest tests for Stage 5V checks against the sample data.

Note: 5V is for L1 only. Our sample data is L2, so 5V-01 correctly
flags RECOMMENDATIONS_DB as prohibited. We test the logic directly.

Run from evs/ directory:
    /usr/bin/python3 -m pytest tests/test_stage_5v.py -v
"""

from core.loader import load_data_pack
from validators.stage_5v import run_stage_5v, ALL_CHECKS


def get_findings():
    """Load sample data and run 5V checks."""
    pack = load_data_pack("data")
    return run_stage_5v(pack)


def test_5v_runs_all_checks():
    """Verify all 17 active 5V checks are registered."""
    assert len(ALL_CHECKS) == 17


def test_5v_finding_count():
    """5V should produce 10 findings against sample data."""
    findings = get_findings()
    assert len(findings) == 10


def test_5v_all_blockers():
    """All 5V findings should be BLOCKERs."""
    findings = get_findings()
    assert all(f.severity == "BLOCKER" for f in findings)


def test_5v_01_prohibited_table():
    """SCV-5V-01: RECOMMENDATIONS_DB is prohibited in L1."""
    findings = get_findings()
    f01 = [f for f in findings if f.check_id == "SCV-5V-01"]
    assert len(f01) == 1
    assert f01[0].table_name == "RECOMMENDATIONS_DB"


def test_5v_02_duplicate_pk():
    """SCV-5V-02: METRICS_DB duplicate PK detected."""
    findings = get_findings()
    f02 = [f for f in findings if f.check_id == "SCV-5V-02"]
    assert len(f02) == 1
    assert f02[0].table_name == "METRICS_DB"


def test_5v_07_orphan_metric_id():
    """SCV-5V-07: M888 in CLAIM_LEDGER_DB doesn't exist in METRICS_DB."""
    findings = get_findings()
    f07 = [f for f in findings if f.check_id == "SCV-5V-07"]
    assert len(f07) == 1
    assert "M888" in str(f07[0].sample_failing_rows)


def test_5v_10_orphan_claim_id():
    """SCV-5V-10: MAR-9999 in DRIVER_DB doesn't exist in CLAIM_LEDGER_DB."""
    findings = get_findings()
    f10 = [f for f in findings if f.check_id == "SCV-5V-10"]
    assert len(f10) == 1
    assert "MAR-9999" in str(f10[0].sample_failing_rows)


def test_5v_11_missing_segment_key():
    """SCV-5V-11: CASH-0001 linked_metric_rows missing segment_key."""
    findings = get_findings()
    f11 = [f for f in findings if f.check_id == "SCV-5V-11"]
    assert len(f11) == 1
    assert "segment_key" in str(f11[0].sample_failing_rows)


def test_5v_12_data_claim_empty_links():
    """SCV-5V-12: REV-0002 is [From Data] but has empty metric links."""
    findings = get_findings()
    f12 = [f for f in findings if f.check_id == "SCV-5V-12"]
    assert len(f12) == 1
    assert "REV-0002" in str(f12[0].sample_failing_rows)


def test_5v_13_empty_why_it_matters():
    """SCV-5V-13: VERIFY_NEXT_DB has empty why_it_matters."""
    findings = get_findings()
    f13 = [f for f in findings if f.check_id == "SCV-5V-13"]
    assert len(f13) == 1
    assert f13[0].column_name == "why_it_matters"


def test_5v_14_orphan_file_id():
    """SCV-5V-14: PNL_FACT references F999 not in INPUT_INDEX_DB."""
    findings = get_findings()
    f14 = [f for f in findings if f.check_id == "SCV-5V-14"]
    assert len(f14) == 1
    assert "F999" in str(f14[0].sample_failing_rows)


def test_5v_03_run_id_consistent():
    """SCV-5V-03 should pass — all tables share the same run_id."""
    findings = get_findings()
    f03 = [f for f in findings if f.check_id == "SCV-5V-03"]
    assert len(f03) == 0

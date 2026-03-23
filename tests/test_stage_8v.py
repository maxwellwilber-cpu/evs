"""
tests/test_stage_8v.py
Pytest tests for Stage 8V checks against the sample data.

8V runs for L2/L3. Our sample data is L2, so this is the production path.

Run from evs/ directory:
    /usr/bin/python3 -m pytest tests/test_stage_8v.py -v
"""

from core.loader import load_data_pack
from validators.stage_8v import run_stage_8v, ALL_CHECKS


def get_findings():
    """Load sample data and run 8V checks."""
    pack = load_data_pack("data")
    return run_stage_8v(pack)


def test_8v_runs_all_checks():
    """Verify all 36 active 8V checks are registered."""
    assert len(ALL_CHECKS) == 36


def test_8v_finding_count():
    """8V should produce 13 findings against sample data."""
    findings = get_findings()
    assert len(findings) == 13


def test_8v_blocker_count():
    """8V should produce 12 BLOCKERs."""
    findings = get_findings()
    blockers = [f for f in findings if f.severity == "BLOCKER"]
    assert len(blockers) == 12


def test_8v_warn_count():
    """8V should produce 1 WARN."""
    findings = get_findings()
    warns = [f for f in findings if f.severity == "WARN"]
    assert len(warns) == 1


def test_8v_02_duplicate_pk():
    """SCV-8V-02: METRICS_DB duplicate PK."""
    findings = get_findings()
    f02 = [f for f in findings if f.check_id == "SCV-8V-02"]
    assert len(f02) == 1
    assert f02[0].table_name == "METRICS_DB"


def test_8v_07_orphan_metric():
    """SCV-8V-07: M888 in claims doesn't exist in METRICS_DB (RI-3)."""
    findings = get_findings()
    f07 = [f for f in findings if f.check_id == "SCV-8V-07"]
    assert len(f07) == 1
    assert "M888" in str(f07[0].sample_failing_rows)


def test_8v_10_orphan_claim():
    """SCV-8V-10: MAR-9999 in drivers doesn't exist in claims (RI-6)."""
    findings = get_findings()
    f10 = [f for f in findings if f.check_id == "SCV-8V-10"]
    assert len(f10) == 1
    assert "MAR-9999" in str(f10[0].sample_failing_rows)


def test_8v_21_empty_rec_claim_links():
    """SCV-8V-21: MAR-001 recommendation has empty linked_claim_ids.
    This is a planted error — every recommendation must link to claims.
    """
    findings = get_findings()
    f21 = [f for f in findings if f.check_id == "SCV-8V-21"]
    assert len(f21) == 1
    assert f21[0].table_name == "RECOMMENDATIONS_DB"
    assert "MAR-001" in str(f21[0].sample_failing_rows)


def test_8v_35_nonstandard_category():
    """SCV-8V-35: 'Odd Category Name' is not in recommended set (WARN).
    This is a planted error.
    """
    findings = get_findings()
    f35 = [f for f in findings if f.check_id == "SCV-8V-35"]
    assert len(f35) == 1
    assert f35[0].severity == "WARN"
    assert "Odd Category Name" in f35[0].message


def test_8v_19_missing_segment_key():
    """SCV-8V-19: CASH-0001 metric_rows missing segment_key (parallel to 5V-11)."""
    findings = get_findings()
    f19 = [f for f in findings if f.check_id == "SCV-8V-19"]
    assert len(f19) == 1
    assert "segment_key" in str(f19[0].sample_failing_rows)


def test_8v_23_orphan_file_id():
    """SCV-8V-23: F999 in PNL_FACT not in INPUT_INDEX_DB (RI-17)."""
    findings = get_findings()
    f23 = [f for f in findings if f.check_id == "SCV-8V-23"]
    assert len(f23) == 1
    assert "F999" in str(f23[0].sample_failing_rows)


def test_8v_30_duplicate_ri17():
    """SCV-8V-30: Same as 8V-23 — spec duplicate reported under both IDs."""
    findings = get_findings()
    f30 = [f for f in findings if f.check_id == "SCV-8V-30"]
    assert len(f30) == 1
    assert "F999" in str(f30[0].sample_failing_rows)


def test_8v_01_no_layer_violations():
    """SCV-8V-01 should pass — L2 required tables are present."""
    findings = get_findings()
    f01 = [f for f in findings if f.check_id == "SCV-8V-01"]
    assert len(f01) == 0


def test_8v_03_run_id_consistent():
    """SCV-8V-03 should pass — all tables share the same run_id."""
    findings = get_findings()
    f03 = [f for f in findings if f.check_id == "SCV-8V-03"]
    assert len(f03) == 0

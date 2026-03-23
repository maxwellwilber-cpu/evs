"""
validators/stage_2v.py
Stage 2V — Post-Normalization Validation (9 active checks)

These checks run FIRST and validate that the foundational tables
(INPUT_INDEX_DB, NORMALIZATION_LOG_DB, PNL_FACT, BS_FACT) exist,
have correct schemas, and pass basic type/enum validation.

Think of 2V as: "Did the pipeline produce the right tables with
the right columns and valid basic types?"

Source: EVS Spec Section 5, Stage 2V (SCV-2V-01 through SCV-2V-09)
Note: SCV-2V-10 is retired (VCR-1.2).
"""

import re
import json
from core.types import ValidationFinding
from core.registry import (
    SCHEMA_REGISTRY, ENUMS, ID_PATTERNS,
    get_expected_columns, get_column_def,
)

# ---------------------------------------------------------------------------
# The 2V-scoped tables — these are the only tables 2V checks inspect.
# Other tables (METRICS_DB, CLAIM_LEDGER_DB, etc.) are checked in 3V+.
# ---------------------------------------------------------------------------
STAGE = "2V"


# ===========================================================================
# HELPER FUNCTIONS (used by multiple checks)
# ===========================================================================

def _get_proceed_status(pack):
    """Extract proceed_status from NORMALIZATION_LOG_DB, or None if missing."""
    norm = pack.get_table("NORMALIZATION_LOG_DB")
    if norm is None or norm.empty:
        return None
    return norm.iloc[0].get("proceed_status", "")


def _is_valid_json_list(value):
    """Check if a string is a valid JSON array (list)."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list)
    except (json.JSONDecodeError, TypeError):
        return False


def _check_regex(value, pattern_key):
    """Check if a value matches a regex pattern from ID_PATTERNS."""
    pattern = ID_PATTERNS.get(pattern_key)
    if not pattern:
        return True  # no pattern defined = no check needed
    return bool(re.match(pattern, str(value)))


def _make_finding(pack, check_id, severity, table_name, column_name,
                  fail_count, message, hint, blocker_class=None,
                  samples=None):
    """Shortcut to create a ValidationFinding with consistent run_id."""
    return ValidationFinding(
        run_id=pack.run_id or "UNKNOWN",
        stage_id=STAGE,
        check_id=check_id,
        severity=severity,
        table_name=table_name,
        column_name=column_name,
        fail_count=fail_count,
        sample_failing_rows=samples or [],
        message=message,
        remediation_hint=hint,
        blocker_class=blocker_class,
    )


# ===========================================================================
# CHECK FUNCTIONS — one per SCV check, each returns a list of findings
# ===========================================================================

def check_2v_01(pack):
    """SCV-2V-01 [SCHEMA] BLOCKER
    INPUT_INDEX_DB and NORMALIZATION_LOG_DB MUST be present.
    """
    findings = []

    for table_name in ["INPUT_INDEX_DB", "NORMALIZATION_LOG_DB"]:
        if not pack.has_table(table_name):
            findings.append(_make_finding(
                pack, "SCV-2V-01", "BLOCKER", table_name, "*",
                fail_count=1,
                message=f"{table_name} is missing — required at all stages.",
                hint=f"Ensure the pipeline emits {table_name}.",
                blocker_class="GENERATION_ERROR",
            ))

    return findings


def check_2v_02(pack):
    """SCV-2V-02 [LOGIC] BLOCKER
    proceed_status must be valid enum.
    If STOP → PNL_FACT must NOT be present.
    """
    findings = []
    status = _get_proceed_status(pack)

    if status is None:
        # Can't check if NORMALIZATION_LOG_DB is missing (2V-01 catches that)
        return findings

    # Check proceed_status is valid enum
    valid = ENUMS["proceed_status"]
    if status not in valid:
        findings.append(_make_finding(
            pack, "SCV-2V-02", "BLOCKER", "NORMALIZATION_LOG_DB",
            "proceed_status", fail_count=1,
            message=f"proceed_status '{status}' is not a valid enum. "
                    f"Allowed: {sorted(valid)}",
            hint="Set proceed_status to STOP, PROCEED-WITH-LIMITS, or PROCEED.",
            blocker_class="GENERATION_ERROR",
        ))
        return findings  # can't check further with invalid status

    # If STOP, PNL_FACT must not exist
    if status == "STOP" and pack.has_table("PNL_FACT"):
        findings.append(_make_finding(
            pack, "SCV-2V-02", "BLOCKER", "PNL_FACT", "*",
            fail_count=1,
            message="proceed_status=STOP but PNL_FACT was emitted. "
                    "When STOP, no downstream tables should be present.",
            hint="Remove PNL_FACT when proceed_status=STOP.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


def check_2v_03(pack):
    """SCV-2V-03 [SCHEMA] BLOCKER
    INPUT_INDEX_DB contract validation:
      - file_id unique per run_id
      - report_type in {PnL, BalanceSheet, CashFlow, Other}
      - date_range_start/end valid YYYY-MM-DD
      - accounting_basis in {cash, accrual, unknown}
    """
    findings = []
    df = pack.get_table("INPUT_INDEX_DB")
    if df is None or df.empty:
        return findings

    # Check file_id uniqueness
    dupes = df[df.duplicated(subset=["file_id"], keep=False)]
    if not dupes.empty:
        dupe_ids = dupes["file_id"].unique().tolist()
        findings.append(_make_finding(
            pack, "SCV-2V-03", "BLOCKER", "INPUT_INDEX_DB", "file_id",
            fail_count=len(dupe_ids),
            message=f"Duplicate file_id values: {dupe_ids}",
            hint="Each file_id must be unique within a run.",
            blocker_class="GENERATION_ERROR",
            samples=[{"file_id": fid} for fid in dupe_ids[:3]],
        ))

    # Check report_type enum
    valid_rt = ENUMS["report_type"]
    bad_rt = df[~df["report_type"].isin(valid_rt)]
    if not bad_rt.empty:
        bad_vals = bad_rt["report_type"].unique().tolist()
        findings.append(_make_finding(
            pack, "SCV-2V-03", "BLOCKER", "INPUT_INDEX_DB", "report_type",
            fail_count=len(bad_rt),
            message=f"Invalid report_type values: {bad_vals}. "
                    f"Allowed: {sorted(valid_rt)}",
            hint="Use PnL, BalanceSheet, CashFlow, or Other.",
            blocker_class="GENERATION_ERROR",
            samples=[{"file_id": r["file_id"], "report_type": r["report_type"]}
                     for _, r in bad_rt.head(3).iterrows()],
        ))

    # Check date formats
    date_pattern = ID_PATTERNS["date_ymd"]
    for col in ["date_range_start", "date_range_end"]:
        bad_dates = df[~df[col].apply(lambda v: bool(re.match(date_pattern, str(v))))]
        if not bad_dates.empty:
            findings.append(_make_finding(
                pack, "SCV-2V-03", "BLOCKER", "INPUT_INDEX_DB", col,
                fail_count=len(bad_dates),
                message=f"Invalid {col} format. Expected YYYY-MM-DD.",
                hint=f"Fix {col} to match YYYY-MM-DD format.",
                blocker_class="GENERATION_ERROR",
                samples=[{"file_id": r["file_id"], col: r[col]}
                         for _, r in bad_dates.head(3).iterrows()],
            ))

    # Check accounting_basis enum
    valid_ab = ENUMS["accounting_basis"]
    bad_ab = df[~df["accounting_basis"].isin(valid_ab)]
    if not bad_ab.empty:
        findings.append(_make_finding(
            pack, "SCV-2V-03", "BLOCKER", "INPUT_INDEX_DB", "accounting_basis",
            fail_count=len(bad_ab),
            message=f"Invalid accounting_basis. Allowed: {sorted(valid_ab)}",
            hint="Use cash, accrual, or unknown.",
            blocker_class="GENERATION_ERROR",
            samples=[{"file_id": r["file_id"], "accounting_basis": r["accounting_basis"]}
                     for _, r in bad_ab.head(3).iterrows()],
        ))

    return findings


def check_2v_04(pack):
    """SCV-2V-04 [SCHEMA] BLOCKER
    NORMALIZATION_LOG_DB contract validation:
      - mapping_confidence_grade in {A, B, C}
      - unclassified_pct numeric in [0, 1]
      - unclassified_pct_unit = ratio_0_to_1
      - integrity_checks_passed in {Y, N}
      - contra_revenue_candidates_present in {Y, N}
      - List fields serialized as valid JSON
      - retry_count present, retry_log present
    """
    findings = []
    df = pack.get_table("NORMALIZATION_LOG_DB")
    if df is None or df.empty:
        return findings

    row = df.iloc[0]  # single-row table

    # mapping_confidence_grade
    valid_mcg = ENUMS["mapping_confidence_grade"]
    if row.get("mapping_confidence_grade") not in valid_mcg:
        findings.append(_make_finding(
            pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
            "mapping_confidence_grade", fail_count=1,
            message=f"Invalid mapping_confidence_grade: '{row.get('mapping_confidence_grade')}'. "
                    f"Allowed: {sorted(valid_mcg)}",
            hint="Set to A, B, or C.",
            blocker_class="GENERATION_ERROR",
        ))

    # unclassified_pct — must be numeric in [0, 1]
    try:
        pct = float(row.get("unclassified_pct", ""))
        if not (0 <= pct <= 1):
            findings.append(_make_finding(
                pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
                "unclassified_pct", fail_count=1,
                message=f"unclassified_pct={pct} is outside [0, 1].",
                hint="Value must be between 0 and 1 inclusive.",
                blocker_class="GENERATION_ERROR",
            ))
    except (ValueError, TypeError):
        findings.append(_make_finding(
            pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
            "unclassified_pct", fail_count=1,
            message=f"unclassified_pct is not numeric: '{row.get('unclassified_pct')}'",
            hint="Must be a decimal number in [0, 1].",
            blocker_class="GENERATION_ERROR",
        ))

    # unclassified_pct_unit
    if row.get("unclassified_pct_unit") != "ratio_0_to_1":
        findings.append(_make_finding(
            pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
            "unclassified_pct_unit", fail_count=1,
            message=f"unclassified_pct_unit must be 'ratio_0_to_1', "
                    f"got '{row.get('unclassified_pct_unit')}'",
            hint="Set to ratio_0_to_1.",
            blocker_class="GENERATION_ERROR",
        ))

    # Y/N enum checks
    for col, enum_key in [
        ("integrity_checks_passed", "yn_flag"),
        ("contra_revenue_candidates_present", "yn_flag"),
    ]:
        valid = ENUMS[enum_key]
        if row.get(col) not in valid:
            findings.append(_make_finding(
                pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
                col, fail_count=1,
                message=f"{col}='{row.get(col)}' is not valid. Allowed: {sorted(valid)}",
                hint=f"Set {col} to Y or N.",
                blocker_class="GENERATION_ERROR",
            ))

    # JSON list fields
    json_list_cols = [
        "critical_category_coverage_flags",
        "missing_months",
    ]
    for col in json_list_cols:
        val = row.get(col, "")
        if not _is_valid_json_list(val):
            findings.append(_make_finding(
                pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
                col, fail_count=1,
                message=f"{col} is not valid JSON list: '{val}'",
                hint=f"Must be a JSON array, e.g. [] or [\"item\"].",
                blocker_class="GENERATION_ERROR",
            ))

    # retry_count — must be present and integer
    try:
        int(row.get("retry_count", ""))
    except (ValueError, TypeError):
        findings.append(_make_finding(
            pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
            "retry_count", fail_count=1,
            message=f"retry_count is not a valid integer: '{row.get('retry_count')}'",
            hint="Must be an integer (e.g., 0).",
            blocker_class="GENERATION_ERROR",
        ))

    # retry_log — must be valid JSON list of objects
    retry_log = row.get("retry_log", "")
    if not _is_valid_json_list(retry_log):
        findings.append(_make_finding(
            pack, "SCV-2V-04", "BLOCKER", "NORMALIZATION_LOG_DB",
            "retry_log", fail_count=1,
            message=f"retry_log is not valid JSON list: '{retry_log}'",
            hint="Must be a JSON array of objects, e.g. [].",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


def check_2v_05(pack):
    """SCV-2V-05 [SCHEMA] BLOCKER
    If proceed_status != STOP → PNL_FACT MUST be present.
    Validate PNL_FACT columns: period_key YYYY-MM, amount numeric non-null,
    account_bucket valid enum, account_category non-null,
    segment_key non-null, source_file_id regex.
    """
    findings = []
    status = _get_proceed_status(pack)

    # If status is STOP or missing, PNL_FACT is not required
    if status is None or status == "STOP":
        return findings

    # PNL_FACT must be present when not STOP
    df = pack.get_table("PNL_FACT")
    if df is None or df.empty:
        findings.append(_make_finding(
            pack, "SCV-2V-05", "BLOCKER", "PNL_FACT", "*",
            fail_count=1,
            message="proceed_status != STOP but PNL_FACT is missing.",
            hint="Pipeline must emit PNL_FACT when proceeding.",
            blocker_class="GENERATION_ERROR",
        ))
        return findings

    # period_key format
    bad_pk = df[~df["period_key"].apply(lambda v: bool(re.match(ID_PATTERNS["date_ym"], str(v))))]
    if not bad_pk.empty:
        findings.append(_make_finding(
            pack, "SCV-2V-05", "BLOCKER", "PNL_FACT", "period_key",
            fail_count=len(bad_pk),
            message="Invalid period_key format. Expected YYYY-MM.",
            hint="Fix period_key to match YYYY-MM (e.g., 2025-01).",
            blocker_class="GENERATION_ERROR",
            samples=[{"period_key": r["period_key"]} for _, r in bad_pk.head(3).iterrows()],
        ))

    # amount — must be numeric and non-null
    bad_amt = []
    for idx, r in df.iterrows():
        val = r.get("amount", "")
        if val == "":
            bad_amt.append({"row": idx, "amount": val, "reason": "null/empty"})
        else:
            try:
                float(val)
            except (ValueError, TypeError):
                bad_amt.append({"row": idx, "amount": val, "reason": "not numeric"})
    if bad_amt:
        findings.append(_make_finding(
            pack, "SCV-2V-05", "BLOCKER", "PNL_FACT", "amount",
            fail_count=len(bad_amt),
            message=f"{len(bad_amt)} rows have non-numeric or null amount.",
            hint="amount must be a non-null numeric value.",
            blocker_class="GENERATION_ERROR",
            samples=bad_amt[:3],
        ))

    # account_bucket — valid enum
    valid_ab = ENUMS["account_bucket"]
    bad_ab = df[~df["account_bucket"].isin(valid_ab)]
    if not bad_ab.empty:
        findings.append(_make_finding(
            pack, "SCV-2V-05", "BLOCKER", "PNL_FACT", "account_bucket",
            fail_count=len(bad_ab),
            message=f"Invalid account_bucket values: {bad_ab['account_bucket'].unique().tolist()}",
            hint=f"Allowed: {sorted(valid_ab)}",
            blocker_class="GENERATION_ERROR",
            samples=[{"account_bucket": r["account_bucket"]}
                     for _, r in bad_ab.head(3).iterrows()],
        ))

    # Non-null checks for required string fields
    for col in ["account_category", "segment_key"]:
        nulls = df[df[col].apply(lambda v: str(v).strip() == "")]
        if not nulls.empty:
            findings.append(_make_finding(
                pack, "SCV-2V-05", "BLOCKER", "PNL_FACT", col,
                fail_count=len(nulls),
                message=f"{len(nulls)} rows have null/empty {col}.",
                hint=f"{col} is required and cannot be empty.",
                blocker_class="GENERATION_ERROR",
            ))

    # source_file_id regex
    pattern = ID_PATTERNS["source_file_id"]
    bad_fid = df[~df["source_file_id"].apply(lambda v: bool(re.match(pattern, str(v))))]
    if not bad_fid.empty:
        findings.append(_make_finding(
            pack, "SCV-2V-05", "BLOCKER", "PNL_FACT", "source_file_id",
            fail_count=len(bad_fid),
            message=f"source_file_id does not match F### format.",
            hint="source_file_id must match regex ^F\\d{3}$.",
            blocker_class="GENERATION_ERROR",
            samples=[{"source_file_id": r["source_file_id"]}
                     for _, r in bad_fid.head(3).iterrows()],
        ))

    return findings


def check_2v_06(pack):
    """SCV-2V-06 [SCHEMA] WARN/BLOCKER
    If INPUT_INDEX_DB has report_type=BalanceSheet → BS_FACT SHOULD exist.
    If BS_FACT present, validate its schema:
      period_key YYYY-MM, amount numeric, line_item non-null,
      segment_key non-null, source_file_id regex.
    Severity: WARN for absence, BLOCKER for schema violations.
    """
    findings = []

    # Check if BS source files are declared
    input_df = pack.get_table("INPUT_INDEX_DB")
    has_bs_input = False
    if input_df is not None and not input_df.empty:
        has_bs_input = (input_df["report_type"] == "BalanceSheet").any()

    bs_df = pack.get_table("BS_FACT")

    # If BS input declared but BS_FACT missing → WARN
    if has_bs_input and (bs_df is None or bs_df.empty):
        findings.append(_make_finding(
            pack, "SCV-2V-06", "WARN", "BS_FACT", "*",
            fail_count=1,
            message="INPUT_INDEX_DB declares a BalanceSheet file but BS_FACT is absent.",
            hint="Pipeline should emit BS_FACT when BS source data is provided.",
        ))
        return findings

    # If BS_FACT not present at all, nothing more to check
    if bs_df is None or bs_df.empty:
        return findings

    # BS_FACT is present — validate its schema (BLOCKER for violations)

    # period_key format
    bad_pk = bs_df[~bs_df["period_key"].apply(lambda v: bool(re.match(ID_PATTERNS["date_ym"], str(v))))]
    if not bad_pk.empty:
        findings.append(_make_finding(
            pack, "SCV-2V-06", "BLOCKER", "BS_FACT", "period_key",
            fail_count=len(bad_pk),
            message="Invalid period_key format in BS_FACT.",
            hint="period_key must match YYYY-MM.",
            blocker_class="GENERATION_ERROR",
        ))

    # amount — numeric (negatives permitted for BS)
    for idx, r in bs_df.iterrows():
        val = r.get("amount", "")
        if val == "":
            findings.append(_make_finding(
                pack, "SCV-2V-06", "BLOCKER", "BS_FACT", "amount",
                fail_count=1,
                message=f"Null amount in BS_FACT row {idx}.",
                hint="amount must be numeric (negatives permitted for BS).",
                blocker_class="GENERATION_ERROR",
            ))
            break
        try:
            float(val)
        except (ValueError, TypeError):
            findings.append(_make_finding(
                pack, "SCV-2V-06", "BLOCKER", "BS_FACT", "amount",
                fail_count=1,
                message=f"Non-numeric amount '{val}' in BS_FACT.",
                hint="amount must be numeric.",
                blocker_class="GENERATION_ERROR",
            ))
            break

    # Non-null checks
    for col in ["line_item", "segment_key"]:
        nulls = bs_df[bs_df[col].apply(lambda v: str(v).strip() == "")]
        if not nulls.empty:
            findings.append(_make_finding(
                pack, "SCV-2V-06", "BLOCKER", "BS_FACT", col,
                fail_count=len(nulls),
                message=f"{len(nulls)} rows have null/empty {col} in BS_FACT.",
                hint=f"{col} is required.",
                blocker_class="GENERATION_ERROR",
            ))

    # source_file_id regex
    pattern = ID_PATTERNS["source_file_id"]
    bad_fid = bs_df[~bs_df["source_file_id"].apply(lambda v: bool(re.match(pattern, str(v))))]
    if not bad_fid.empty:
        findings.append(_make_finding(
            pack, "SCV-2V-06", "BLOCKER", "BS_FACT", "source_file_id",
            fail_count=len(bad_fid),
            message="source_file_id does not match F### format in BS_FACT.",
            hint="Must match regex ^F\\d{3}$.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


def check_2v_07(pack):
    """SCV-2V-07 [LOGIC] WARN/BLOCKER
    Period coherence: if PNL_FACT and BS_FACT both exist, their
    period_key ranges should overlap.
    WARN for partial overlap. BLOCKER for zero common periods.
    """
    findings = []

    pnl_df = pack.get_table("PNL_FACT")
    bs_df = pack.get_table("BS_FACT")

    # Both must exist to check coherence
    if pnl_df is None or pnl_df.empty or bs_df is None or bs_df.empty:
        return findings

    pnl_periods = set(pnl_df["period_key"].unique())
    bs_periods = set(bs_df["period_key"].unique())
    common = pnl_periods & bs_periods

    if len(common) == 0:
        # Zero overlap → BLOCKER
        findings.append(_make_finding(
            pack, "SCV-2V-07", "BLOCKER", "PNL_FACT,BS_FACT", "period_key",
            fail_count=1,
            message=f"Zero period overlap. PNL periods: {sorted(pnl_periods)}, "
                    f"BS periods: {sorted(bs_periods)}.",
            hint="PNL_FACT and BS_FACT should share at least one period.",
            blocker_class="DATA_ERROR",
        ))
    elif common != pnl_periods or common != bs_periods:
        # Partial overlap → WARN
        pnl_only = pnl_periods - bs_periods
        bs_only = bs_periods - pnl_periods
        findings.append(_make_finding(
            pack, "SCV-2V-07", "WARN", "PNL_FACT,BS_FACT", "period_key",
            fail_count=1,
            message=f"Partial period overlap. Common: {sorted(common)}. "
                    f"PNL-only: {sorted(pnl_only)}. BS-only: {sorted(bs_only)}.",
            hint="Consider aligning period ranges between PNL and BS.",
        ))

    return findings


def check_2v_08(pack):
    """SCV-2V-08 [SCHEMA] BLOCKER
    Every 2V table must match SCHEMA_REGISTRY columns exactly.
    No missing required columns. No extra/unknown columns.
    """
    findings = []
    status = _get_proceed_status(pack)

    # Determine which 2V tables to check
    tables_to_check = ["INPUT_INDEX_DB", "NORMALIZATION_LOG_DB"]
    if status != "STOP" and pack.has_table("PNL_FACT"):
        tables_to_check.append("PNL_FACT")
    if pack.has_table("BS_FACT"):
        tables_to_check.append("BS_FACT")

    for table_name in tables_to_check:
        df = pack.get_table(table_name)
        if df is None:
            continue

        expected = get_expected_columns(table_name)
        actual = list(df.columns)

        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]

        if missing:
            findings.append(_make_finding(
                pack, "SCV-2V-08", "BLOCKER", table_name, "*",
                fail_count=len(missing),
                message=f"Missing required columns: {missing}",
                hint=f"Add missing columns to {table_name}.",
                blocker_class="GENERATION_ERROR",
                samples=[{"missing_column": c} for c in missing[:3]],
            ))

        if extra:
            findings.append(_make_finding(
                pack, "SCV-2V-08", "BLOCKER", table_name, "*",
                fail_count=len(extra),
                message=f"Extra/unknown columns: {extra}",
                hint=f"Remove undeclared columns from {table_name}.",
                blocker_class="GENERATION_ERROR",
                samples=[{"extra_column": c} for c in extra[:3]],
            ))

    return findings


def check_2v_09(pack):
    """SCV-2V-09 [SCHEMA] BLOCKER
    No 2V table may have duplicate column headers.
    """
    findings = []
    status = _get_proceed_status(pack)

    tables_to_check = ["INPUT_INDEX_DB", "NORMALIZATION_LOG_DB"]
    if status != "STOP" and pack.has_table("PNL_FACT"):
        tables_to_check.append("PNL_FACT")
    if pack.has_table("BS_FACT"):
        tables_to_check.append("BS_FACT")

    for table_name in tables_to_check:
        df = pack.get_table(table_name)
        if df is None:
            continue

        cols = list(df.columns)
        seen = set()
        dupes = set()
        for c in cols:
            if c in seen:
                dupes.add(c)
            seen.add(c)

        if dupes:
            findings.append(_make_finding(
                pack, "SCV-2V-09", "BLOCKER", table_name, "*",
                fail_count=len(dupes),
                message=f"Duplicate column headers: {sorted(dupes)}",
                hint=f"Remove duplicate column names from {table_name}.",
                blocker_class="GENERATION_ERROR",
                samples=[{"duplicate_column": c} for c in sorted(dupes)[:3]],
            ))

    return findings


# ===========================================================================
# MAIN ENTRY POINT — runs all 9 active 2V checks
# ===========================================================================

# Map of all active checks in execution order
ALL_CHECKS = [
    ("SCV-2V-01", check_2v_01),
    ("SCV-2V-02", check_2v_02),
    ("SCV-2V-03", check_2v_03),
    ("SCV-2V-04", check_2v_04),
    ("SCV-2V-05", check_2v_05),
    ("SCV-2V-06", check_2v_06),
    ("SCV-2V-07", check_2v_07),
    ("SCV-2V-08", check_2v_08),
    ("SCV-2V-09", check_2v_09),
]


def run_stage_2v(pack):
    """Run all 9 active Stage 2V checks against a DataPack.

    Args:
        pack: A DataPack from core.loader

    Returns:
        List of ValidationFinding objects (empty if everything passes)
    """
    all_findings = []

    for check_id, check_fn in ALL_CHECKS:
        results = check_fn(pack)
        all_findings.extend(results)

    return all_findings

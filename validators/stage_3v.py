"""
validators/stage_3v.py
Stage 3V — Post-Metric Engine Validation (11 active checks)

These checks validate METRICS_DB after the metric computation engine
runs. They confirm PK uniqueness, approved metric IDs, value/status
gating, JSON list compliance, and the PNL_FACT sign convention.

Think of 3V as: "Did the metric engine produce valid, compliant metrics,
and do the numbers obey the pipeline's business rules?"

Source: EVS Spec Section 5, Stage 3V (SCV-3V-01 through SCV-3V-14)
Retired: SCV-3V-08, SCV-3V-12, SCV-3V-13, SCV-3V-15 (VCR-1.2)
"""

import re
import json
from core.types import ValidationFinding
from core.registry import (
    SCHEMA_REGISTRY, ENUMS, ID_PATTERNS, PRIMARY_KEYS,
    APPROVED_METRIC_IDS, get_expected_columns,
)

STAGE = "3V"

# ---------------------------------------------------------------------------
# 3V-scoped tables — checked by the JSON list validators (3V-06, 3V-07)
# ---------------------------------------------------------------------------
TABLES_3V_SCOPE = [
    "INPUT_INDEX_DB", "NORMALIZATION_LOG_DB", "PNL_FACT", "BS_FACT", "METRICS_DB",
]


# ===========================================================================
# HELPERS
# ===========================================================================

def _get_proceed_status(pack):
    """Extract proceed_status from NORMALIZATION_LOG_DB, or None."""
    norm = pack.get_table("NORMALIZATION_LOG_DB")
    if norm is None or norm.empty:
        return None
    return norm.iloc[0].get("proceed_status", "")


def _is_valid_json_list(value):
    """Check if a string is a valid JSON array."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list)
    except (json.JSONDecodeError, TypeError):
        return False


def _is_json_list_of_strings(value):
    """Check if value is a JSON array where every element is a string."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            return False
        return all(isinstance(item, str) for item in parsed)
    except (json.JSONDecodeError, TypeError):
        return False


def _is_json_list_of_objects(value):
    """Check if value is a JSON array where every element is a dict."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            return False
        return all(isinstance(item, dict) for item in parsed)
    except (json.JSONDecodeError, TypeError):
        return False


def _make_finding(pack, check_id, severity, table_name, column_name,
                  fail_count, message, hint, blocker_class=None,
                  samples=None):
    """Shortcut to create a ValidationFinding."""
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
# CHECK FUNCTIONS
# ===========================================================================

def check_3v_01(pack):
    """SCV-3V-01 [LOGIC] BLOCKER
    If proceed_status in {PROCEED, PROCEED-WITH-LIMITS} → METRICS_DB must exist.
    If proceed_status = STOP → METRICS_DB must NOT exist.
    """
    findings = []
    status = _get_proceed_status(pack)
    if status is None:
        return findings

    has_metrics = pack.has_table("METRICS_DB")

    if status in ("PROCEED", "PROCEED-WITH-LIMITS") and not has_metrics:
        findings.append(_make_finding(
            pack, "SCV-3V-01", "BLOCKER", "METRICS_DB", "*",
            fail_count=1,
            message=f"proceed_status={status} but METRICS_DB is missing.",
            hint="Pipeline must emit METRICS_DB when proceeding.",
            blocker_class="GENERATION_ERROR",
        ))

    if status == "STOP" and has_metrics:
        findings.append(_make_finding(
            pack, "SCV-3V-01", "BLOCKER", "METRICS_DB", "*",
            fail_count=1,
            message="proceed_status=STOP but METRICS_DB is present.",
            hint="Do not emit METRICS_DB when proceed_status=STOP.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


def check_3v_02(pack):
    """SCV-3V-02 [SCHEMA] BLOCKER
    METRICS_DB schema/types validation:
      period_key YYYY-MM, unit in enum, status in enum, confidence in enum.
    """
    findings = []
    df = pack.get_table("METRICS_DB")
    if df is None or df.empty:
        return findings

    # period_key format
    date_ym = ID_PATTERNS["date_ym"]
    bad_pk = df[~df["period_key"].apply(lambda v: bool(re.match(date_ym, str(v))))]
    if not bad_pk.empty:
        findings.append(_make_finding(
            pack, "SCV-3V-02", "BLOCKER", "METRICS_DB", "period_key",
            fail_count=len(bad_pk),
            message="Invalid period_key format. Expected YYYY-MM.",
            hint="Fix period_key to YYYY-MM format.",
            blocker_class="GENERATION_ERROR",
            samples=[{"metric_id": r["metric_id"], "period_key": r["period_key"]}
                     for _, r in bad_pk.head(3).iterrows()],
        ))

    # Enum checks: unit, status, confidence
    enum_checks = [
        ("unit", "outcome_unit"),
        ("status", "metric_status"),
        ("confidence", "confidence"),
    ]
    for col, enum_key in enum_checks:
        valid = ENUMS[enum_key]
        bad = df[~df[col].isin(valid)]
        if not bad.empty:
            bad_vals = bad[col].unique().tolist()
            findings.append(_make_finding(
                pack, "SCV-3V-02", "BLOCKER", "METRICS_DB", col,
                fail_count=len(bad),
                message=f"Invalid {col} values: {bad_vals}. Allowed: {sorted(valid)}",
                hint=f"Fix {col} to use valid enum values.",
                blocker_class="GENERATION_ERROR",
                samples=[{"metric_id": r["metric_id"], col: r[col]}
                         for _, r in bad.head(3).iterrows()],
            ))

    return findings


def check_3v_03(pack):
    """SCV-3V-03 [SCHEMA] BLOCKER
    METRICS_DB PK (run_id, metric_id, period_key, segment_key) must be unique.
    """
    findings = []
    df = pack.get_table("METRICS_DB")
    if df is None or df.empty:
        return findings

    pk_cols = list(PRIMARY_KEYS["METRICS_DB"])
    dupes = df[df.duplicated(subset=pk_cols, keep=False)]

    if not dupes.empty:
        # Get the distinct duplicate PK combos
        dupe_keys = dupes[pk_cols].drop_duplicates().to_dict("records")
        findings.append(_make_finding(
            pack, "SCV-3V-03", "BLOCKER", "METRICS_DB", "*",
            fail_count=len(dupes),
            message=f"Duplicate primary keys found. {len(dupes)} rows share "
                    f"{len(dupe_keys)} duplicate PK combination(s).",
            hint="Remove or fix duplicate rows so each (run_id, metric_id, "
                 "period_key, segment_key) is unique.",
            blocker_class="GENERATION_ERROR",
            samples=[{k: row[k] for k in pk_cols} for row in dupe_keys[:3]],
        ))

    return findings


def check_3v_04(pack):
    """SCV-3V-04 [LOGIC] BLOCKER
    metric_id must exist in Approved Metrics Dictionary.
    formula_reference must be non-null for Calculated metrics.
    """
    findings = []
    df = pack.get_table("METRICS_DB")
    if df is None or df.empty:
        return findings

    # Unapproved metric IDs
    bad_ids = df[~df["metric_id"].isin(APPROVED_METRIC_IDS)]
    if not bad_ids.empty:
        unknown = bad_ids["metric_id"].unique().tolist()
        findings.append(_make_finding(
            pack, "SCV-3V-04", "BLOCKER", "METRICS_DB", "metric_id",
            fail_count=len(bad_ids),
            message=f"Unapproved metric_id(s): {unknown}. "
                    f"Not in the Approved Metrics Dictionary.",
            hint="Only use metric IDs from the approved list (M001-M051).",
            blocker_class="GENERATION_ERROR",
            samples=[{"metric_id": mid} for mid in unknown[:3]],
        ))

    # formula_reference must be non-null for Calculated metrics
    calculated = df[df["status"] == "Calculated"]
    empty_formula = calculated[calculated["formula_reference"].apply(
        lambda v: str(v).strip() == ""
    )]
    if not empty_formula.empty:
        findings.append(_make_finding(
            pack, "SCV-3V-04", "BLOCKER", "METRICS_DB", "formula_reference",
            fail_count=len(empty_formula),
            message=f"{len(empty_formula)} Calculated metrics have empty formula_reference.",
            hint="Every Calculated metric must have a non-empty formula_reference.",
            blocker_class="GENERATION_ERROR",
            samples=[{"metric_id": r["metric_id"], "status": r["status"]}
                     for _, r in empty_formula.head(3).iterrows()],
        ))

    return findings


def check_3v_05(pack):
    """SCV-3V-05 [RI] BLOCKER
    Every file_id in METRICS_DB.file_ids_used must exist in INPUT_INDEX_DB.file_id.
    file_ids_used must be valid json_list_str.
    """
    findings = []
    metrics_df = pack.get_table("METRICS_DB")
    input_df = pack.get_table("INPUT_INDEX_DB")
    if metrics_df is None or metrics_df.empty:
        return findings
    if input_df is None or input_df.empty:
        return findings

    valid_file_ids = set(input_df["file_id"].unique())
    bad_parse = []
    orphan_refs = []

    for idx, row in metrics_df.iterrows():
        val = row.get("file_ids_used", "")

        # Check valid JSON list
        if not _is_json_list_of_strings(val):
            bad_parse.append({
                "metric_id": row["metric_id"],
                "file_ids_used": val,
            })
            continue

        # Check each file_id exists in INPUT_INDEX_DB
        file_ids = json.loads(val)
        for fid in file_ids:
            if fid not in valid_file_ids:
                orphan_refs.append({
                    "metric_id": row["metric_id"],
                    "orphan_file_id": fid,
                })

    if bad_parse:
        findings.append(_make_finding(
            pack, "SCV-3V-05", "BLOCKER", "METRICS_DB", "file_ids_used",
            fail_count=len(bad_parse),
            message=f"{len(bad_parse)} rows have invalid file_ids_used JSON.",
            hint="file_ids_used must be a JSON array of strings, e.g. [\"F001\"].",
            blocker_class="INTEGRITY_ERROR",
            samples=bad_parse[:3],
        ))

    if orphan_refs:
        findings.append(_make_finding(
            pack, "SCV-3V-05", "BLOCKER", "METRICS_DB", "file_ids_used",
            fail_count=len(orphan_refs),
            message=f"{len(orphan_refs)} file_id references not found in INPUT_INDEX_DB.",
            hint="Every file_id in file_ids_used must exist in INPUT_INDEX_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphan_refs[:3],
        ))

    return findings


def check_3v_06(pack):
    """SCV-3V-06 [SCHEMA] BLOCKER
    All json_list_str fields in 3V-scoped tables must be non-null,
    parse as JSON array of strings, empty = [].
    """
    findings = []

    for table_name in TABLES_3V_SCOPE:
        df = pack.get_table(table_name)
        if df is None or df.empty:
            continue

        # Get json_list_str columns from the registry
        schema = SCHEMA_REGISTRY.get(table_name, [])
        jls_cols = [col["name"] for col in schema if col["dtype"] == "json_list_str"]

        for col_name in jls_cols:
            if col_name not in df.columns:
                continue

            bad_rows = []
            for idx, row in df.iterrows():
                val = row.get(col_name, "")
                if not _is_json_list_of_strings(val):
                    bad_rows.append({
                        "row_index": idx,
                        col_name: val,
                    })

            if bad_rows:
                findings.append(_make_finding(
                    pack, "SCV-3V-06", "BLOCKER", table_name, col_name,
                    fail_count=len(bad_rows),
                    message=f"{len(bad_rows)} rows have invalid json_list_str "
                            f"in {table_name}.{col_name}.",
                    hint="Must be a JSON array of strings, e.g. [] or [\"item\"].",
                    blocker_class="GENERATION_ERROR",
                    samples=bad_rows[:3],
                ))

    return findings


def check_3v_07(pack):
    """SCV-3V-07 [SCHEMA] BLOCKER
    All json_list_obj fields in 3V-scoped tables must be non-null,
    parse as JSON array of objects, empty = [].
    """
    findings = []

    for table_name in TABLES_3V_SCOPE:
        df = pack.get_table(table_name)
        if df is None or df.empty:
            continue

        schema = SCHEMA_REGISTRY.get(table_name, [])
        jlo_cols = [col["name"] for col in schema if col["dtype"] == "json_list_obj"]

        for col_name in jlo_cols:
            if col_name not in df.columns:
                continue

            bad_rows = []
            for idx, row in df.iterrows():
                val = row.get(col_name, "")
                if not _is_json_list_of_objects(val):
                    bad_rows.append({
                        "row_index": idx,
                        col_name: val,
                    })

            if bad_rows:
                findings.append(_make_finding(
                    pack, "SCV-3V-07", "BLOCKER", table_name, col_name,
                    fail_count=len(bad_rows),
                    message=f"{len(bad_rows)} rows have invalid json_list_obj "
                            f"in {table_name}.{col_name}.",
                    hint="Must be a JSON array of objects, e.g. [] or [{...}].",
                    blocker_class="GENERATION_ERROR",
                    samples=bad_rows[:3],
                ))

    return findings


def check_3v_09(pack):
    """SCV-3V-09 [LOGIC] BLOCKER
    PNL_FACT.amount must be >= 0 (sign convention standardization).
    """
    findings = []
    df = pack.get_table("PNL_FACT")
    if df is None or df.empty:
        return findings

    negative_rows = []
    for idx, row in df.iterrows():
        val = row.get("amount", "")
        try:
            amt = float(val)
            if amt < 0:
                negative_rows.append({
                    "account_name_normalized": row.get("account_name_normalized", ""),
                    "period_key": row.get("period_key", ""),
                    "amount": val,
                })
        except (ValueError, TypeError):
            pass  # non-numeric amounts caught by 2V-05

    if negative_rows:
        findings.append(_make_finding(
            pack, "SCV-3V-09", "BLOCKER", "PNL_FACT", "amount",
            fail_count=len(negative_rows),
            message=f"{len(negative_rows)} PNL_FACT rows have negative amount. "
                    f"Sign convention requires amount >= 0.",
            hint="Standardize sign convention: all PNL_FACT amounts must be non-negative. "
                 "Use account_bucket to determine debit/credit direction.",
            blocker_class="GENERATION_ERROR",
            samples=negative_rows[:3],
        ))

    return findings


def check_3v_10(pack):
    """SCV-3V-10 [LOGIC] BLOCKER
    NORMALIZATION_LOG_DB.unclassified_pct must be in [0,1]
    AND unclassified_pct_unit must be ratio_0_to_1.
    """
    findings = []
    df = pack.get_table("NORMALIZATION_LOG_DB")
    if df is None or df.empty:
        return findings

    row = df.iloc[0]

    # unclassified_pct range
    try:
        pct = float(row.get("unclassified_pct", ""))
        if not (0 <= pct <= 1):
            findings.append(_make_finding(
                pack, "SCV-3V-10", "BLOCKER", "NORMALIZATION_LOG_DB",
                "unclassified_pct", fail_count=1,
                message=f"unclassified_pct={pct} is outside [0, 1].",
                hint="Value must be between 0 and 1 inclusive.",
                blocker_class="GENERATION_ERROR",
            ))
    except (ValueError, TypeError):
        findings.append(_make_finding(
            pack, "SCV-3V-10", "BLOCKER", "NORMALIZATION_LOG_DB",
            "unclassified_pct", fail_count=1,
            message=f"unclassified_pct is not numeric: '{row.get('unclassified_pct')}'",
            hint="Must be a decimal in [0, 1].",
            blocker_class="GENERATION_ERROR",
        ))

    # unit check
    unit = row.get("unclassified_pct_unit", "")
    if unit != "ratio_0_to_1":
        findings.append(_make_finding(
            pack, "SCV-3V-10", "BLOCKER", "NORMALIZATION_LOG_DB",
            "unclassified_pct_unit", fail_count=1,
            message=f"unclassified_pct_unit='{unit}', expected 'ratio_0_to_1'.",
            hint="Set to ratio_0_to_1.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


def check_3v_11(pack):
    """SCV-3V-11 [LOGIC] BLOCKER
    If status=Calculated → value MUST be non-null numeric.
    If status=NotCalculated → value may be null but calc_notes MUST explain why.
    """
    findings = []
    df = pack.get_table("METRICS_DB")
    if df is None or df.empty:
        return findings

    # Calculated metrics with null/empty value
    calculated = df[df["status"] == "Calculated"]
    calc_null_value = []
    for idx, row in calculated.iterrows():
        val = row.get("value", "")
        if str(val).strip() == "":
            calc_null_value.append({
                "metric_id": row["metric_id"],
                "status": "Calculated",
                "value": val,
            })
        else:
            try:
                float(val)
            except (ValueError, TypeError):
                calc_null_value.append({
                    "metric_id": row["metric_id"],
                    "status": "Calculated",
                    "value": val,
                    "reason": "not numeric",
                })

    if calc_null_value:
        findings.append(_make_finding(
            pack, "SCV-3V-11", "BLOCKER", "METRICS_DB", "value",
            fail_count=len(calc_null_value),
            message=f"{len(calc_null_value)} Calculated metrics have null or "
                    f"non-numeric value.",
            hint="Calculated metrics must have a non-null numeric value.",
            blocker_class="GENERATION_ERROR",
            samples=calc_null_value[:3],
        ))

    # NotCalculated metrics with empty calc_notes
    not_calculated = df[df["status"] == "NotCalculated"]
    empty_notes = []
    for idx, row in not_calculated.iterrows():
        notes = str(row.get("calc_notes", "")).strip()
        if notes == "":
            empty_notes.append({
                "metric_id": row["metric_id"],
                "status": "NotCalculated",
                "calc_notes": "(empty)",
            })

    if empty_notes:
        findings.append(_make_finding(
            pack, "SCV-3V-11", "BLOCKER", "METRICS_DB", "calc_notes",
            fail_count=len(empty_notes),
            message=f"{len(empty_notes)} NotCalculated metrics have empty calc_notes.",
            hint="NotCalculated metrics must have calc_notes explaining why "
                 "the value could not be computed.",
            blocker_class="GENERATION_ERROR",
            samples=empty_notes[:3],
        ))

    return findings


def check_3v_14(pack):
    """SCV-3V-14 [LOGIC] WARN
    VALIDATION_REPORT_DB.sample_failing_rows must contain <= 3 objects.
    """
    findings = []
    df = pack.get_table("VALIDATION_REPORT_DB")
    if df is None or df.empty:
        return findings

    for idx, row in df.iterrows():
        val = row.get("sample_failing_rows", "")
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list) and len(parsed) > 3:
                findings.append(_make_finding(
                    pack, "SCV-3V-14", "WARN", "VALIDATION_REPORT_DB",
                    "sample_failing_rows", fail_count=1,
                    message=f"sample_failing_rows has {len(parsed)} objects "
                            f"(max 3). Check: {row.get('check_id', '?')}",
                    hint="Limit sample_failing_rows to 3 objects maximum.",
                ))
        except (json.JSONDecodeError, TypeError):
            pass  # bad JSON caught by 3V-07

    return findings


# ===========================================================================
# MAIN ENTRY POINT
# ===========================================================================

ALL_CHECKS = [
    ("SCV-3V-01", check_3v_01),
    ("SCV-3V-02", check_3v_02),
    ("SCV-3V-03", check_3v_03),
    ("SCV-3V-04", check_3v_04),
    ("SCV-3V-05", check_3v_05),
    ("SCV-3V-06", check_3v_06),
    ("SCV-3V-07", check_3v_07),
    ("SCV-3V-09", check_3v_09),
    ("SCV-3V-10", check_3v_10),
    ("SCV-3V-11", check_3v_11),
    ("SCV-3V-14", check_3v_14),
]


def run_stage_3v(pack):
    """Run all 11 active Stage 3V checks against a DataPack.

    Args:
        pack: A DataPack from core.loader

    Returns:
        List of ValidationFinding objects
    """
    all_findings = []

    for check_id, check_fn in ALL_CHECKS:
        results = check_fn(pack)
        all_findings.extend(results)

    return all_findings

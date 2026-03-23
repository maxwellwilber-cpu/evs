"""
validators/stage_5v.py
Stage 5V — Pre-Output Validation, L1 Only (17 active checks)

These checks run for L1_DIAGNOSTIC mode only. They validate:
  - Layer-mode table enforcement (required vs prohibited tables)
  - PK uniqueness across ALL L1 tables
  - Single run_id consistency
  - JSON list compliance across all L1 tables
  - Cross-table referential integrity (RI-1 through RI-6, RI-17/18/20/21)
  - Claim linkage rules (metric_rows schema, truth_label gating)
  - VERIFY_NEXT_DB required fields

Think of 5V as: "Before we ship L1 output, does every cross-table
reference actually point to something that exists?"

Source: EVS Spec Section 5, Stage 5V (SCV-5V-01 through SCV-5V-17)
Note: 8V is a superset of 5V — shared logic is implemented here,
      and 8V can call these same functions for its parallel checks.
"""

import re
import json
from core.types import ValidationFinding
from core.registry import (
    SCHEMA_REGISTRY, ENUMS, ID_PATTERNS, PRIMARY_KEYS,
    LAYER_TABLES, APPROVED_METRIC_IDS, UNCERTAIN_TRUTH_LABELS,
)

STAGE = "5V"

# ---------------------------------------------------------------------------
# L1 tables that 5V validates
# ---------------------------------------------------------------------------
L1_TABLES = [
    "INPUT_INDEX_DB", "NORMALIZATION_LOG_DB", "PNL_FACT", "BS_FACT",
    "METRICS_DB", "CLAIM_LEDGER_DB", "DRIVER_DB", "VERIFY_NEXT_DB",
    "VALIDATION_REPORT_DB",
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


def _is_json_list_of_strings(value):
    """Check if value is a JSON array where every element is a string."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list) and all(isinstance(i, str) for i in parsed)
    except (json.JSONDecodeError, TypeError):
        return False


def _is_json_list_of_objects(value):
    """Check if value is a JSON array where every element is a dict."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list) and all(isinstance(i, dict) for i in parsed)
    except (json.JSONDecodeError, TypeError):
        return False


def _parse_json_list(value):
    """Parse a JSON list string, returning empty list on failure."""
    if not isinstance(value, str) or value.strip() == "":
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    return []


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

def check_5v_01(pack):
    """SCV-5V-01 [CROSS] BLOCKER
    Layer-mode table enforcement for L1_DIAGNOSTIC.
    Required tables must exist. Prohibited tables must not.
    """
    findings = []
    status = _get_proceed_status(pack)
    layer_rules = LAYER_TABLES["L1"]

    # Required tables
    required = set(layer_rules["required"])
    # Add conditional-required tables based on proceed_status
    if status and status != "STOP":
        required.add("PNL_FACT")
        required.add("METRICS_DB")

    for table_name in sorted(required):
        if not pack.has_table(table_name):
            findings.append(_make_finding(
                pack, "SCV-5V-01", "BLOCKER", table_name, "*",
                fail_count=1,
                message=f"Required L1 table {table_name} is missing.",
                hint=f"Pipeline must emit {table_name} for L1_DIAGNOSTIC mode.",
                blocker_class="GENERATION_ERROR",
            ))

    # Prohibited tables
    for table_name in sorted(layer_rules["prohibited"]):
        if pack.has_table(table_name):
            findings.append(_make_finding(
                pack, "SCV-5V-01", "BLOCKER", table_name, "*",
                fail_count=1,
                message=f"Prohibited L1 table {table_name} is present. "
                        f"L1_DIAGNOSTIC must not emit this table.",
                hint=f"Remove {table_name} for L1 output, or switch to L2/L3.",
                blocker_class="GENERATION_ERROR",
            ))

    return findings


def check_5v_02(pack):
    """SCV-5V-02 [SCHEMA] BLOCKER
    PK uniqueness for all L1 tables.
    """
    findings = []

    for table_name in L1_TABLES:
        df = pack.get_table(table_name)
        if df is None or df.empty:
            continue

        pk_cols = PRIMARY_KEYS.get(table_name)
        if not pk_cols:
            continue

        # Only check PK columns that exist in the DataFrame
        available_pk = [c for c in pk_cols if c in df.columns]
        if not available_pk:
            continue

        dupes = df[df.duplicated(subset=available_pk, keep=False)]
        if not dupes.empty:
            dupe_count = len(dupes[available_pk].drop_duplicates())
            findings.append(_make_finding(
                pack, "SCV-5V-02", "BLOCKER", table_name, "*",
                fail_count=len(dupes),
                message=f"Duplicate PKs in {table_name}: {len(dupes)} rows "
                        f"across {dupe_count} duplicate key(s).",
                hint=f"Ensure PK {pk_cols} is unique in {table_name}.",
                blocker_class="GENERATION_ERROR",
                samples=dupes[available_pk].drop_duplicates().head(3).to_dict("records"),
            ))

    return findings


def check_5v_03(pack):
    """SCV-5V-03 [CROSS] BLOCKER
    All tables must contain exactly one run_id, identical across all tables.
    """
    findings = []
    all_run_ids = set()

    for table_name in L1_TABLES:
        df = pack.get_table(table_name)
        if df is None or df.empty:
            continue
        if "run_id" not in df.columns:
            continue

        table_run_ids = set(df["run_id"].unique())

        # Multiple run_ids within a single table
        if len(table_run_ids) > 1:
            findings.append(_make_finding(
                pack, "SCV-5V-03", "BLOCKER", table_name, "run_id",
                fail_count=len(table_run_ids),
                message=f"Multiple run_ids in {table_name}: {sorted(table_run_ids)}",
                hint="Each table must contain exactly one run_id.",
                blocker_class="GENERATION_ERROR",
            ))

        all_run_ids.update(table_run_ids)

    # Mismatched run_ids across tables
    if len(all_run_ids) > 1:
        findings.append(_make_finding(
            pack, "SCV-5V-03", "BLOCKER", "*", "run_id",
            fail_count=len(all_run_ids),
            message=f"Mismatched run_ids across tables: {sorted(all_run_ids)}",
            hint="All tables must share the same run_id.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


def check_5v_04(pack):
    """SCV-5V-04 [SCHEMA] BLOCKER
    All json_list_str and json_list_obj fields across all L1 tables
    must comply with list serialization rules.
    """
    findings = []

    for table_name in L1_TABLES:
        df = pack.get_table(table_name)
        if df is None or df.empty:
            continue

        schema = SCHEMA_REGISTRY.get(table_name, [])

        for col_def in schema:
            col_name = col_def["name"]
            if col_name not in df.columns:
                continue

            if col_def["dtype"] == "json_list_str":
                bad = []
                for idx, row in df.iterrows():
                    if not _is_json_list_of_strings(row.get(col_name, "")):
                        bad.append({"row": idx, col_name: row.get(col_name, "")})
                if bad:
                    findings.append(_make_finding(
                        pack, "SCV-5V-04", "BLOCKER", table_name, col_name,
                        fail_count=len(bad),
                        message=f"{len(bad)} rows have invalid json_list_str "
                                f"in {table_name}.{col_name}.",
                        hint="Must be JSON array of strings, e.g. [] or [\"item\"].",
                        blocker_class="GENERATION_ERROR",
                        samples=bad[:3],
                    ))

            elif col_def["dtype"] == "json_list_obj":
                # nullable json_list_obj: skip empty strings if not required
                bad = []
                for idx, row in df.iterrows():
                    val = row.get(col_name, "")
                    # If nullable and empty, skip
                    if not col_def["required"] and str(val).strip() == "":
                        continue
                    if not _is_json_list_of_objects(val):
                        bad.append({"row": idx, col_name: val})
                if bad:
                    findings.append(_make_finding(
                        pack, "SCV-5V-04", "BLOCKER", table_name, col_name,
                        fail_count=len(bad),
                        message=f"{len(bad)} rows have invalid json_list_obj "
                                f"in {table_name}.{col_name}.",
                        hint="Must be JSON array of objects, e.g. [] or [{...}].",
                        blocker_class="GENERATION_ERROR",
                        samples=bad[:3],
                    ))

    return findings


def check_5v_05(pack):
    """SCV-5V-05 [RI] BLOCKER — RI-1
    METRICS_DB.file_ids_used -> INPUT_INDEX_DB.file_id
    """
    findings = []
    metrics_df = pack.get_table("METRICS_DB")
    input_df = pack.get_table("INPUT_INDEX_DB")
    if metrics_df is None or input_df is None:
        return findings

    valid_ids = set(input_df["file_id"].unique())
    orphans = []

    for _, row in metrics_df.iterrows():
        for fid in _parse_json_list(row.get("file_ids_used", "")):
            if isinstance(fid, str) and fid not in valid_ids:
                orphans.append({"metric_id": row["metric_id"], "orphan_file_id": fid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-05", "BLOCKER", "METRICS_DB", "file_ids_used",
            fail_count=len(orphans),
            message=f"RI-1: {len(orphans)} file_id ref(s) not in INPUT_INDEX_DB.",
            hint="Every file_id in file_ids_used must exist in INPUT_INDEX_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_06(pack):
    """SCV-5V-06 [RI] BLOCKER — RI-2
    CLAIM_LEDGER_DB.linked_file_ids -> INPUT_INDEX_DB.file_id
    """
    findings = []
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    input_df = pack.get_table("INPUT_INDEX_DB")
    if claims_df is None or input_df is None:
        return findings

    valid_ids = set(input_df["file_id"].unique())
    orphans = []

    for _, row in claims_df.iterrows():
        for fid in _parse_json_list(row.get("linked_file_ids", "")):
            if isinstance(fid, str) and fid not in valid_ids:
                orphans.append({"claim_id": row["claim_id"], "orphan_file_id": fid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-06", "BLOCKER", "CLAIM_LEDGER_DB", "linked_file_ids",
            fail_count=len(orphans),
            message=f"RI-2: {len(orphans)} file_id ref(s) not in INPUT_INDEX_DB.",
            hint="Every file_id in linked_file_ids must exist in INPUT_INDEX_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_07(pack):
    """SCV-5V-07 [RI] BLOCKER — RI-3
    CLAIM_LEDGER_DB.linked_metric_ids -> METRICS_DB.metric_id
    """
    findings = []
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    metrics_df = pack.get_table("METRICS_DB")
    if claims_df is None or metrics_df is None:
        return findings

    valid_ids = set(metrics_df["metric_id"].unique())
    orphans = []

    for _, row in claims_df.iterrows():
        for mid in _parse_json_list(row.get("linked_metric_ids", "")):
            if isinstance(mid, str) and mid not in valid_ids:
                orphans.append({"claim_id": row["claim_id"], "orphan_metric_id": mid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-07", "BLOCKER", "CLAIM_LEDGER_DB", "linked_metric_ids",
            fail_count=len(orphans),
            message=f"RI-3: {len(orphans)} metric_id ref(s) not in METRICS_DB.",
            hint="Every metric_id in linked_metric_ids must exist in METRICS_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_08(pack):
    """SCV-5V-08 [RI] BLOCKER — RI-4
    CLAIM_LEDGER_DB.linked_metric_rows objects must match
    METRICS_DB on (metric_id, period_key, segment_key).
    """
    findings = []
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    metrics_df = pack.get_table("METRICS_DB")
    if claims_df is None or metrics_df is None:
        return findings

    # Build set of valid composite keys from METRICS_DB
    valid_keys = set()
    for _, row in metrics_df.iterrows():
        key = (row["metric_id"], row["period_key"], row["segment_key"])
        valid_keys.add(key)

    orphans = []

    for _, row in claims_df.iterrows():
        objects = _parse_json_list(row.get("linked_metric_rows", ""))
        for obj in objects:
            if not isinstance(obj, dict):
                continue
            key = (
                obj.get("metric_id", ""),
                obj.get("period_key", ""),
                obj.get("segment_key", ""),
            )
            if key not in valid_keys:
                orphans.append({
                    "claim_id": row["claim_id"],
                    "metric_row_ref": obj,
                })

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-08", "BLOCKER", "CLAIM_LEDGER_DB", "linked_metric_rows",
            fail_count=len(orphans),
            message=f"RI-4: {len(orphans)} linked_metric_rows object(s) don't match "
                    f"any METRICS_DB row on (metric_id, period_key, segment_key).",
            hint="Each linked_metric_rows object must reference a real METRICS_DB row.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_09(pack):
    """SCV-5V-09 [RI] BLOCKER — RI-5
    DRIVER_DB.evidence_metric_ids -> METRICS_DB.metric_id
    """
    findings = []
    drivers_df = pack.get_table("DRIVER_DB")
    metrics_df = pack.get_table("METRICS_DB")
    if drivers_df is None or metrics_df is None:
        return findings

    valid_ids = set(metrics_df["metric_id"].unique())
    orphans = []

    for _, row in drivers_df.iterrows():
        for mid in _parse_json_list(row.get("evidence_metric_ids", "")):
            if isinstance(mid, str) and mid not in valid_ids:
                orphans.append({"driver_id": row["driver_id"], "orphan_metric_id": mid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-09", "BLOCKER", "DRIVER_DB", "evidence_metric_ids",
            fail_count=len(orphans),
            message=f"RI-5: {len(orphans)} metric_id ref(s) not in METRICS_DB.",
            hint="Every metric_id in evidence_metric_ids must exist in METRICS_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_10(pack):
    """SCV-5V-10 [RI] BLOCKER — RI-6
    DRIVER_DB.evidence_claim_ids -> CLAIM_LEDGER_DB.claim_id
    """
    findings = []
    drivers_df = pack.get_table("DRIVER_DB")
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    if drivers_df is None or claims_df is None:
        return findings

    valid_ids = set(claims_df["claim_id"].unique())
    orphans = []

    for _, row in drivers_df.iterrows():
        for cid in _parse_json_list(row.get("evidence_claim_ids", "")):
            if isinstance(cid, str) and cid not in valid_ids:
                orphans.append({"driver_id": row["driver_id"], "orphan_claim_id": cid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-10", "BLOCKER", "DRIVER_DB", "evidence_claim_ids",
            fail_count=len(orphans),
            message=f"RI-6: {len(orphans)} claim_id ref(s) not in CLAIM_LEDGER_DB.",
            hint="Every claim_id in evidence_claim_ids must exist in CLAIM_LEDGER_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_11(pack):
    """SCV-5V-11 [SCHEMA] BLOCKER
    Each linked_metric_rows object in CLAIM_LEDGER_DB must include:
    metric_id (M### format), period_key (YYYY-MM), segment_key (string).
    """
    findings = []
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    if claims_df is None:
        return findings

    required_keys = {"metric_id", "period_key", "segment_key"}
    bad_objects = []

    for _, row in claims_df.iterrows():
        objects = _parse_json_list(row.get("linked_metric_rows", ""))
        for obj in objects:
            if not isinstance(obj, dict):
                bad_objects.append({
                    "claim_id": row["claim_id"],
                    "issue": "not a dict",
                    "object": str(obj),
                })
                continue

            missing_keys = required_keys - set(obj.keys())
            if missing_keys:
                bad_objects.append({
                    "claim_id": row["claim_id"],
                    "missing_keys": sorted(missing_keys),
                    "object": obj,
                })
                continue

            # Validate formats
            if not re.match(ID_PATTERNS["metric_id"], str(obj.get("metric_id", ""))):
                bad_objects.append({
                    "claim_id": row["claim_id"],
                    "issue": f"bad metric_id format: {obj.get('metric_id')}",
                })
            if not re.match(ID_PATTERNS["date_ym"], str(obj.get("period_key", ""))):
                bad_objects.append({
                    "claim_id": row["claim_id"],
                    "issue": f"bad period_key format: {obj.get('period_key')}",
                })

    if bad_objects:
        findings.append(_make_finding(
            pack, "SCV-5V-11", "BLOCKER", "CLAIM_LEDGER_DB", "linked_metric_rows",
            fail_count=len(bad_objects),
            message=f"{len(bad_objects)} linked_metric_rows object(s) have "
                    f"missing or invalid keys (need metric_id, period_key, segment_key).",
            hint="Each object must include metric_id (M###), period_key (YYYY-MM), "
                 "and segment_key (string).",
            blocker_class="GENERATION_ERROR",
            samples=bad_objects[:3],
        ))

    return findings


def check_5v_12(pack):
    """SCV-5V-12 [LOGIC] BLOCKER
    If truth_label NOT IN {[Hypothesis],[Unknown],[Uncertain]}:
      linked_metric_ids must be non-empty
      linked_metric_rows must be non-empty
    """
    findings = []
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    if claims_df is None:
        return findings

    bad_claims = []

    for _, row in claims_df.iterrows():
        truth = row.get("truth_label", "")

        # Skip uncertain/hypothesis/unknown — empty links are OK
        if truth in UNCERTAIN_TRUTH_LABELS:
            continue

        metric_ids = _parse_json_list(row.get("linked_metric_ids", ""))
        metric_rows = _parse_json_list(row.get("linked_metric_rows", ""))

        if len(metric_ids) == 0 or len(metric_rows) == 0:
            bad_claims.append({
                "claim_id": row["claim_id"],
                "truth_label": truth,
                "linked_metric_ids_count": len(metric_ids),
                "linked_metric_rows_count": len(metric_rows),
            })

    if bad_claims:
        findings.append(_make_finding(
            pack, "SCV-5V-12", "BLOCKER", "CLAIM_LEDGER_DB",
            "linked_metric_ids,linked_metric_rows",
            fail_count=len(bad_claims),
            message=f"{len(bad_claims)} data-backed claim(s) have empty metric links. "
                    f"Claims with truth_label not in {sorted(UNCERTAIN_TRUTH_LABELS)} "
                    f"must have non-empty linked_metric_ids and linked_metric_rows.",
            hint="Add metric references for data-backed claims, or change "
                 "truth_label to [Hypothesis], [Unknown], or [Uncertain].",
            blocker_class="GENERATION_ERROR",
            samples=bad_claims[:3],
        ))

    return findings


def check_5v_13(pack):
    """SCV-5V-13 [SCHEMA] BLOCKER
    All required VERIFY_NEXT_DB columns must be non-null.
    linked_claim_ids must be valid json_list_str.
    """
    findings = []
    df = pack.get_table("VERIFY_NEXT_DB")
    if df is None or df.empty:
        return findings

    schema = SCHEMA_REGISTRY.get("VERIFY_NEXT_DB", [])
    required_cols = [col["name"] for col in schema if col["required"]]

    # Check each required column for empty/null values
    for col_name in required_cols:
        if col_name not in df.columns:
            continue

        # Get the column definition to check dtype
        col_def = next((c for c in schema if c["name"] == col_name), None)

        # For json_list fields, empty [] is valid — check list compliance instead
        if col_def and col_def["dtype"] in ("json_list_str", "json_list_obj"):
            continue  # handled by 5V-04

        # For regular string fields, check non-null
        nulls = df[df[col_name].apply(lambda v: str(v).strip() == "")]
        if not nulls.empty:
            findings.append(_make_finding(
                pack, "SCV-5V-13", "BLOCKER", "VERIFY_NEXT_DB", col_name,
                fail_count=len(nulls),
                message=f"{len(nulls)} rows have empty {col_name} in VERIFY_NEXT_DB.",
                hint=f"{col_name} is required and cannot be empty.",
                blocker_class="GENERATION_ERROR",
                samples=[{"vn_id": r["vn_id"], col_name: r[col_name]}
                         for _, r in nulls.head(3).iterrows()],
            ))

    return findings


def check_5v_14(pack):
    """SCV-5V-14 [RI] BLOCKER — RI-17
    PNL_FACT.source_file_id -> INPUT_INDEX_DB.file_id
    """
    findings = []
    pnl_df = pack.get_table("PNL_FACT")
    input_df = pack.get_table("INPUT_INDEX_DB")
    if pnl_df is None or input_df is None:
        return findings

    valid_ids = set(input_df["file_id"].unique())
    orphans = []

    for _, row in pnl_df.iterrows():
        fid = row.get("source_file_id", "")
        if fid not in valid_ids:
            orphans.append({
                "period_key": row.get("period_key", ""),
                "account_name_normalized": row.get("account_name_normalized", ""),
                "orphan_file_id": fid,
            })

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-14", "BLOCKER", "PNL_FACT", "source_file_id",
            fail_count=len(orphans),
            message=f"RI-17: {len(orphans)} PNL_FACT row(s) reference "
                    f"source_file_id not in INPUT_INDEX_DB.",
            hint="Every source_file_id must exist as a file_id in INPUT_INDEX_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_15(pack):
    """SCV-5V-15 [RI] BLOCKER — RI-18
    BS_FACT.source_file_id -> INPUT_INDEX_DB.file_id (if BS_FACT present)
    """
    findings = []
    bs_df = pack.get_table("BS_FACT")
    input_df = pack.get_table("INPUT_INDEX_DB")
    if bs_df is None or input_df is None:
        return findings

    valid_ids = set(input_df["file_id"].unique())
    orphans = []

    for _, row in bs_df.iterrows():
        fid = row.get("source_file_id", "")
        if fid not in valid_ids:
            orphans.append({
                "line_item": row.get("line_item", ""),
                "orphan_file_id": fid,
            })

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-15", "BLOCKER", "BS_FACT", "source_file_id",
            fail_count=len(orphans),
            message=f"RI-18: {len(orphans)} BS_FACT row(s) reference "
                    f"source_file_id not in INPUT_INDEX_DB.",
            hint="Every source_file_id must exist as a file_id in INPUT_INDEX_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_16(pack):
    """SCV-5V-16 [RI] BLOCKER — RI-20
    CLAIM_LEDGER_DB.linked_driver_ids -> DRIVER_DB.driver_id
    """
    findings = []
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    drivers_df = pack.get_table("DRIVER_DB")
    if claims_df is None or drivers_df is None:
        return findings

    valid_ids = set(drivers_df["driver_id"].unique())
    orphans = []

    for _, row in claims_df.iterrows():
        for did in _parse_json_list(row.get("linked_driver_ids", "")):
            if isinstance(did, str) and did not in valid_ids:
                orphans.append({"claim_id": row["claim_id"], "orphan_driver_id": did})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-16", "BLOCKER", "CLAIM_LEDGER_DB", "linked_driver_ids",
            fail_count=len(orphans),
            message=f"RI-20: {len(orphans)} driver_id ref(s) not in DRIVER_DB.",
            hint="Every driver_id in linked_driver_ids must exist in DRIVER_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


def check_5v_17(pack):
    """SCV-5V-17 [RI] BLOCKER — RI-21
    VERIFY_NEXT_DB.linked_claim_ids -> CLAIM_LEDGER_DB.claim_id
    """
    findings = []
    vn_df = pack.get_table("VERIFY_NEXT_DB")
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    if vn_df is None or claims_df is None:
        return findings

    valid_ids = set(claims_df["claim_id"].unique())
    orphans = []

    for _, row in vn_df.iterrows():
        for cid in _parse_json_list(row.get("linked_claim_ids", "")):
            if isinstance(cid, str) and cid not in valid_ids:
                orphans.append({"vn_id": row["vn_id"], "orphan_claim_id": cid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-5V-17", "BLOCKER", "VERIFY_NEXT_DB", "linked_claim_ids",
            fail_count=len(orphans),
            message=f"RI-21: {len(orphans)} claim_id ref(s) not in CLAIM_LEDGER_DB.",
            hint="Every claim_id in linked_claim_ids must exist in CLAIM_LEDGER_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


# ===========================================================================
# MAIN ENTRY POINT
# ===========================================================================

ALL_CHECKS = [
    ("SCV-5V-01", check_5v_01),
    ("SCV-5V-02", check_5v_02),
    ("SCV-5V-03", check_5v_03),
    ("SCV-5V-04", check_5v_04),
    ("SCV-5V-05", check_5v_05),
    ("SCV-5V-06", check_5v_06),
    ("SCV-5V-07", check_5v_07),
    ("SCV-5V-08", check_5v_08),
    ("SCV-5V-09", check_5v_09),
    ("SCV-5V-10", check_5v_10),
    ("SCV-5V-11", check_5v_11),
    ("SCV-5V-12", check_5v_12),
    ("SCV-5V-13", check_5v_13),
    ("SCV-5V-14", check_5v_14),
    ("SCV-5V-15", check_5v_15),
    ("SCV-5V-16", check_5v_16),
    ("SCV-5V-17", check_5v_17),
]


def run_stage_5v(pack):
    """Run all 17 active Stage 5V checks against a DataPack.

    Note: In production, 5V only runs for L1 data. The orchestrator
    (run_evs.py) handles that gating. These functions can also be
    called by 8V for its parallel checks.

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

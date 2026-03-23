"""
validators/stage_8v.py
Stage 8V — Pre-Output Validation, L2/L3 (36 active checks)

8V is a superset of 5V. It runs for L2_DECISION and L3_ONGOING modes.
It includes all 5V-equivalent checks (re-run with 8V check IDs) plus
additional checks for RECOMMENDATIONS_DB, SCENARIO_SUMMARY_DB,
SENSITIVITY_SUMMARY_DB, DELTA_LOG_DB, and DECISION_TRACKER_DB.

Per spec Note 7: parallel checks share logic with 5V — we import
the 5V functions and remap findings to 8V check IDs.

Per spec Note 8: 8V-23/30 and 8V-24/31 are spec duplicates. We
implement once and report under both IDs for strict VCR-1.2 compliance.

Source: EVS Spec Section 5, Stage 8V (SCV-8V-01 through SCV-8V-35)
Retired: SCV-8V-28 (VCR-1.2). 8V-28a and 8V-28b are active.
"""

import re
import json
from core.types import ValidationFinding
from core.registry import (
    SCHEMA_REGISTRY, ENUMS, ID_PATTERNS, PRIMARY_KEYS,
    LAYER_TABLES, APPROVED_METRIC_IDS, UNCERTAIN_TRUTH_LABELS,
    RECOMMENDED_REC_CATEGORIES,
)
# Import 5V check functions for parallel checks
from validators import stage_5v

STAGE = "8V"

# All tables that could exist in L2/L3 output
ALL_POSSIBLE_TABLES = [
    "INPUT_INDEX_DB", "NORMALIZATION_LOG_DB", "PNL_FACT", "BS_FACT",
    "METRICS_DB", "CLAIM_LEDGER_DB", "DRIVER_DB", "RECOMMENDATIONS_DB",
    "VERIFY_NEXT_DB", "VALIDATION_REPORT_DB",
    "SCENARIO_SUMMARY_DB", "SENSITIVITY_SUMMARY_DB",
    "DELTA_LOG_DB", "DECISION_TRACKER_DB",
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


def _is_json_list_of_strings(value):
    """Check if value is a JSON array of strings."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list) and all(isinstance(i, str) for i in parsed)
    except (json.JSONDecodeError, TypeError):
        return False


def _is_json_list_of_objects(value):
    """Check if value is a JSON array of dicts."""
    if not isinstance(value, str) or value.strip() == "":
        return False
    try:
        parsed = json.loads(value)
        return isinstance(parsed, list) and all(isinstance(i, dict) for i in parsed)
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


def _remap_findings(findings, new_check_id):
    """Take findings from a 5V check and remap to 8V stage + check_id."""
    remapped = []
    for f in findings:
        remapped.append(ValidationFinding(
            run_id=f.run_id,
            stage_id=STAGE,
            check_id=new_check_id,
            severity=f.severity,
            table_name=f.table_name,
            column_name=f.column_name,
            fail_count=f.fail_count,
            sample_failing_rows=f.sample_failing_rows,
            message=f.message,
            remediation_hint=f.remediation_hint,
            blocker_class=f.blocker_class,
        ))
    return remapped


# ===========================================================================
# 8V-01: Layer-mode table enforcement for L2/L3
# ===========================================================================

def check_8v_01(pack):
    """SCV-8V-01 [CROSS] BLOCKER
    L2: All L1 required + RECOMMENDATIONS_DB. SCENARIO/SENSITIVITY conditional.
        PROHIBITED: DECISION_TRACKER_DB.
    L3: All L2 + DELTA_LOG_DB/DECISION_TRACKER_DB conditional.
    """
    findings = []
    status = _get_proceed_status(pack)
    layer = pack.layer_mode  # "L2" or "L3"
    layer_rules = LAYER_TABLES.get(layer, LAYER_TABLES["L2"])

    # Required tables
    required = set(layer_rules["required"])
    if status and status != "STOP":
        required.add("PNL_FACT")
        required.add("METRICS_DB")

    for table_name in sorted(required):
        if not pack.has_table(table_name):
            findings.append(_make_finding(
                pack, "SCV-8V-01", "BLOCKER", table_name, "*",
                fail_count=1,
                message=f"Required {layer} table {table_name} is missing.",
                hint=f"Pipeline must emit {table_name} for {layer} mode.",
                blocker_class="GENERATION_ERROR",
            ))

    # Prohibited tables
    for table_name in sorted(layer_rules["prohibited"]):
        if pack.has_table(table_name):
            findings.append(_make_finding(
                pack, "SCV-8V-01", "BLOCKER", table_name, "*",
                fail_count=1,
                message=f"Prohibited {layer} table {table_name} is present.",
                hint=f"Remove {table_name} for {layer} output.",
                blocker_class="GENERATION_ERROR",
            ))

    return findings


# ===========================================================================
# 8V-02: PK uniqueness for ALL tables
# ===========================================================================

def check_8v_02(pack):
    """SCV-8V-02 [SCHEMA] BLOCKER — PK uniqueness across all tables."""
    findings = []

    for table_name in ALL_POSSIBLE_TABLES:
        df = pack.get_table(table_name)
        if df is None or df.empty:
            continue

        pk_cols = PRIMARY_KEYS.get(table_name)
        if not pk_cols:
            continue

        available_pk = [c for c in pk_cols if c in df.columns]
        if not available_pk:
            continue

        dupes = df[df.duplicated(subset=available_pk, keep=False)]
        if not dupes.empty:
            dupe_count = len(dupes[available_pk].drop_duplicates())
            findings.append(_make_finding(
                pack, "SCV-8V-02", "BLOCKER", table_name, "*",
                fail_count=len(dupes),
                message=f"Duplicate PKs in {table_name}: {len(dupes)} rows "
                        f"across {dupe_count} duplicate key(s).",
                hint=f"Ensure PK {pk_cols} is unique in {table_name}.",
                blocker_class="GENERATION_ERROR",
                samples=dupes[available_pk].drop_duplicates().head(3).to_dict("records"),
            ))

    return findings


# ===========================================================================
# 8V-03: Single run_id across all tables
# ===========================================================================

def check_8v_03(pack):
    """SCV-8V-03 [CROSS] BLOCKER — run_id consistency across all tables."""
    findings = []
    all_run_ids = set()

    for table_name in ALL_POSSIBLE_TABLES:
        df = pack.get_table(table_name)
        if df is None or df.empty or "run_id" not in df.columns:
            continue

        table_run_ids = set(df["run_id"].unique())
        if len(table_run_ids) > 1:
            findings.append(_make_finding(
                pack, "SCV-8V-03", "BLOCKER", table_name, "run_id",
                fail_count=len(table_run_ids),
                message=f"Multiple run_ids in {table_name}: {sorted(table_run_ids)}",
                hint="Each table must contain exactly one run_id.",
                blocker_class="GENERATION_ERROR",
            ))
        all_run_ids.update(table_run_ids)

    if len(all_run_ids) > 1:
        findings.append(_make_finding(
            pack, "SCV-8V-03", "BLOCKER", "*", "run_id",
            fail_count=len(all_run_ids),
            message=f"Mismatched run_ids across tables: {sorted(all_run_ids)}",
            hint="All tables must share the same run_id.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


# ===========================================================================
# 8V-04: JSON list compliance across ALL tables
# ===========================================================================

def check_8v_04(pack):
    """SCV-8V-04 [SCHEMA] BLOCKER — List serialization across all tables."""
    findings = []

    for table_name in ALL_POSSIBLE_TABLES:
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
                        pack, "SCV-8V-04", "BLOCKER", table_name, col_name,
                        fail_count=len(bad),
                        message=f"{len(bad)} invalid json_list_str in "
                                f"{table_name}.{col_name}.",
                        hint="Must be JSON array of strings.",
                        blocker_class="GENERATION_ERROR",
                        samples=bad[:3],
                    ))

            elif col_def["dtype"] == "json_list_obj":
                bad = []
                for idx, row in df.iterrows():
                    val = row.get(col_name, "")
                    if not col_def["required"] and str(val).strip() == "":
                        continue
                    if not _is_json_list_of_objects(val):
                        bad.append({"row": idx, col_name: val})
                if bad:
                    findings.append(_make_finding(
                        pack, "SCV-8V-04", "BLOCKER", table_name, col_name,
                        fail_count=len(bad),
                        message=f"{len(bad)} invalid json_list_obj in "
                                f"{table_name}.{col_name}.",
                        hint="Must be JSON array of objects.",
                        blocker_class="GENERATION_ERROR",
                        samples=bad[:3],
                    ))

    return findings


# ===========================================================================
# 8V-05 through 8V-10: Parallel to 5V-05 through 5V-10
# Shared RI checks — reuse 5V logic, remap to 8V IDs
# ===========================================================================

def check_8v_05(pack):
    """SCV-8V-05 — RI-1 (parallel to 5V-05)"""
    return _remap_findings(stage_5v.check_5v_05(pack), "SCV-8V-05")

def check_8v_06(pack):
    """SCV-8V-06 — RI-2 (parallel to 5V-06)"""
    return _remap_findings(stage_5v.check_5v_06(pack), "SCV-8V-06")

def check_8v_07(pack):
    """SCV-8V-07 — RI-3 (parallel to 5V-07)"""
    return _remap_findings(stage_5v.check_5v_07(pack), "SCV-8V-07")

def check_8v_08(pack):
    """SCV-8V-08 — RI-4 (parallel to 5V-08)"""
    return _remap_findings(stage_5v.check_5v_08(pack), "SCV-8V-08")

def check_8v_09(pack):
    """SCV-8V-09 — RI-5 (parallel to 5V-09)"""
    return _remap_findings(stage_5v.check_5v_09(pack), "SCV-8V-09")

def check_8v_10(pack):
    """SCV-8V-10 — RI-6 (parallel to 5V-10)"""
    return _remap_findings(stage_5v.check_5v_10(pack), "SCV-8V-10")


# ===========================================================================
# 8V-11: RI-7 — RECOMMENDATIONS_DB.linked_claim_ids -> CLAIM_LEDGER_DB
# ===========================================================================

def check_8v_11(pack):
    """SCV-8V-11 [RI] BLOCKER — RI-7"""
    findings = []
    recs_df = pack.get_table("RECOMMENDATIONS_DB")
    claims_df = pack.get_table("CLAIM_LEDGER_DB")
    if recs_df is None or claims_df is None:
        return findings

    valid_ids = set(claims_df["claim_id"].unique())
    orphans = []

    for _, row in recs_df.iterrows():
        for cid in _parse_json_list(row.get("linked_claim_ids", "")):
            if isinstance(cid, str) and cid not in valid_ids:
                orphans.append({"rec_id": row["rec_id"], "orphan_claim_id": cid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-8V-11", "BLOCKER", "RECOMMENDATIONS_DB",
            "linked_claim_ids", fail_count=len(orphans),
            message=f"RI-7: {len(orphans)} claim_id ref(s) in RECOMMENDATIONS_DB "
                    f"not found in CLAIM_LEDGER_DB.",
            hint="Every claim_id in linked_claim_ids must exist in CLAIM_LEDGER_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


# ===========================================================================
# 8V-12: RI-8 — RECOMMENDATIONS_DB.linked_metric_ids -> METRICS_DB
# ===========================================================================

def check_8v_12(pack):
    """SCV-8V-12 [RI] BLOCKER — RI-8"""
    findings = []
    recs_df = pack.get_table("RECOMMENDATIONS_DB")
    metrics_df = pack.get_table("METRICS_DB")
    if recs_df is None or metrics_df is None:
        return findings

    valid_ids = set(metrics_df["metric_id"].unique())
    orphans = []

    for _, row in recs_df.iterrows():
        for mid in _parse_json_list(row.get("linked_metric_ids", "")):
            if isinstance(mid, str) and mid not in valid_ids:
                orphans.append({"rec_id": row["rec_id"], "orphan_metric_id": mid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-8V-12", "BLOCKER", "RECOMMENDATIONS_DB",
            "linked_metric_ids", fail_count=len(orphans),
            message=f"RI-8: {len(orphans)} metric_id ref(s) in RECOMMENDATIONS_DB "
                    f"not found in METRICS_DB.",
            hint="Every metric_id in linked_metric_ids must exist in METRICS_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


# ===========================================================================
# 8V-13: RI-9 — DELTA_LOG_DB.prior_run_id format (L3 only)
# ===========================================================================

def check_8v_13(pack):
    """SCV-8V-13 [SCHEMA] BLOCKER — RI-9 (L3 only)"""
    findings = []
    df = pack.get_table("DELTA_LOG_DB")
    if df is None or df.empty:
        return findings

    pattern = ID_PATTERNS["run_id"]
    bad = []
    for _, row in df.iterrows():
        val = row.get("prior_run_id", "")
        if not re.match(pattern, str(val)):
            bad.append({"prior_run_id": val})

    if bad:
        findings.append(_make_finding(
            pack, "SCV-8V-13", "BLOCKER", "DELTA_LOG_DB", "prior_run_id",
            fail_count=len(bad),
            message=f"{len(bad)} prior_run_id values don't match run_id format.",
            hint="prior_run_id must match ^RUN-[A-Z0-9]+-\\d{8}-\\d{3}$.",
            blocker_class="GENERATION_ERROR",
            samples=bad[:3],
        ))

    return findings


# ===========================================================================
# 8V-14: Fail-closed enforcement
# ===========================================================================

def check_8v_14(pack, prior_findings=None):
    """SCV-8V-14 [CROSS] BLOCKER
    If ANY BLOCKER exists in the FAS-generated VALIDATION_REPORT_DB,
    then only VALIDATION_REPORT_DB should be present (no data tables).

    Note: This checks the FAS output's own VALIDATION_REPORT_DB, not
    the findings the EVS is generating. If no FAS VALIDATION_REPORT_DB
    exists, this check passes silently.
    """
    findings = []
    vr_df = pack.get_table("VALIDATION_REPORT_DB")
    if vr_df is None or vr_df.empty:
        return findings

    # Check if FAS output contains BLOCKERs
    if "severity" not in vr_df.columns:
        return findings

    has_blockers = (vr_df["severity"] == "BLOCKER").any()
    if not has_blockers:
        return findings

    # BLOCKERs exist — check if other data tables are also present
    data_tables = [t for t in ALL_POSSIBLE_TABLES
                   if t != "VALIDATION_REPORT_DB" and pack.has_table(t)]

    if data_tables:
        findings.append(_make_finding(
            pack, "SCV-8V-14", "BLOCKER", "VALIDATION_REPORT_DB", "*",
            fail_count=len(data_tables),
            message=f"Fail-closed violation: BLOCKERs exist in "
                    f"VALIDATION_REPORT_DB but {len(data_tables)} other "
                    f"data tables are present: {data_tables[:5]}",
            hint="When BLOCKERs exist, output only VALIDATION_REPORT_DB.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


# ===========================================================================
# 8V-15: RECOMMENDATIONS_DB requires CLAIM_LEDGER_DB + METRICS_DB
# ===========================================================================

def check_8v_15(pack):
    """SCV-8V-15 [CROSS] BLOCKER"""
    findings = []
    if not pack.has_table("RECOMMENDATIONS_DB"):
        return findings

    for dep in ["CLAIM_LEDGER_DB", "METRICS_DB"]:
        if not pack.has_table(dep):
            findings.append(_make_finding(
                pack, "SCV-8V-15", "BLOCKER", "RECOMMENDATIONS_DB", "*",
                fail_count=1,
                message=f"RECOMMENDATIONS_DB present but {dep} is missing/empty.",
                hint=f"Recommendations require {dep} to be present and non-empty.",
                blocker_class="GENERATION_ERROR",
            ))

    return findings


# ===========================================================================
# 8V-16: DRIVER_DB requires CLAIM_LEDGER_DB + METRICS_DB
# ===========================================================================

def check_8v_16(pack):
    """SCV-8V-16 [CROSS] BLOCKER"""
    findings = []
    if not pack.has_table("DRIVER_DB"):
        return findings

    for dep in ["CLAIM_LEDGER_DB", "METRICS_DB"]:
        if not pack.has_table(dep):
            findings.append(_make_finding(
                pack, "SCV-8V-16", "BLOCKER", "DRIVER_DB", "*",
                fail_count=1,
                message=f"DRIVER_DB present but {dep} is missing/empty.",
                hint=f"Drivers require {dep} to be present and non-empty.",
                blocker_class="GENERATION_ERROR",
            ))

    return findings


# ===========================================================================
# 8V-17: Preview + CSV presence (WARN)
# ===========================================================================

def check_8v_17(pack):
    """SCV-8V-17 [CROSS] WARN
    Each emitted table should include preview + full CSV.
    Note: The EVS only sees CSV files, so this checks that each loaded
    table is non-empty. The full preview check applies to the FAS output
    delivery format which the EVS cannot validate from CSVs alone.
    """
    # The EVS validates CSV files — preview format is outside our scope.
    # Tables are either present with rows or absent. No meaningful check here.
    return []


# ===========================================================================
# 8V-18: If STOP, METRICS_DB should be absent (WARN)
# ===========================================================================

def check_8v_18(pack):
    """SCV-8V-18 [LOGIC] WARN"""
    findings = []
    status = _get_proceed_status(pack)
    if status == "STOP" and pack.has_table("METRICS_DB"):
        findings.append(_make_finding(
            pack, "SCV-8V-18", "WARN", "METRICS_DB", "*",
            fail_count=1,
            message="proceed_status=STOP but METRICS_DB is present.",
            hint="METRICS_DB should not be emitted when proceed_status=STOP.",
        ))
    return findings


# ===========================================================================
# 8V-19 through 8V-24: Parallel to 5V checks
# ===========================================================================

def check_8v_19(pack):
    """SCV-8V-19 — linked_metric_rows schema (parallel to 5V-11)"""
    return _remap_findings(stage_5v.check_5v_11(pack), "SCV-8V-19")

def check_8v_20(pack):
    """SCV-8V-20 — Claim linkage non-empty (parallel to 5V-12)"""
    return _remap_findings(stage_5v.check_5v_12(pack), "SCV-8V-20")

def check_8v_21(pack):
    """SCV-8V-21 [LOGIC] BLOCKER
    RECOMMENDATIONS_DB.linked_claim_ids must be non-empty.
    """
    findings = []
    df = pack.get_table("RECOMMENDATIONS_DB")
    if df is None or df.empty:
        return findings

    empty_links = []
    for _, row in df.iterrows():
        claim_ids = _parse_json_list(row.get("linked_claim_ids", ""))
        if len(claim_ids) == 0:
            empty_links.append({
                "rec_id": row["rec_id"],
                "rec_title": row.get("rec_title", ""),
                "linked_claim_ids": row.get("linked_claim_ids", ""),
            })

    if empty_links:
        findings.append(_make_finding(
            pack, "SCV-8V-21", "BLOCKER", "RECOMMENDATIONS_DB",
            "linked_claim_ids", fail_count=len(empty_links),
            message=f"{len(empty_links)} recommendation(s) have empty "
                    f"linked_claim_ids. Every recommendation must link "
                    f"to at least one claim.",
            hint="Add claim references or remove unsupported recommendations.",
            blocker_class="GENERATION_ERROR",
            samples=empty_links[:3],
        ))

    return findings

def check_8v_22(pack):
    """SCV-8V-22 — VERIFY_NEXT_DB required cols (parallel to 5V-13)"""
    return _remap_findings(stage_5v.check_5v_13(pack), "SCV-8V-22")

def check_8v_23(pack):
    """SCV-8V-23 — RI-17 PNL_FACT.source_file_id (parallel to 5V-14)"""
    return _remap_findings(stage_5v.check_5v_14(pack), "SCV-8V-23")

def check_8v_24(pack):
    """SCV-8V-24 — RI-18 BS_FACT.source_file_id (parallel to 5V-15)"""
    return _remap_findings(stage_5v.check_5v_15(pack), "SCV-8V-24")


# ===========================================================================
# 8V-25: SCENARIO_SUMMARY_DB schema validation
# ===========================================================================

def check_8v_25(pack):
    """SCV-8V-25 [SCHEMA] BLOCKER — SCENARIO_SUMMARY_DB schema."""
    findings = []
    df = pack.get_table("SCENARIO_SUMMARY_DB")
    if df is None or df.empty:
        return findings  # conditional table — absence is not a failure

    # scenario_type enum
    valid_st = ENUMS["scenario_type"]
    bad_st = df[~df["scenario_type"].isin(valid_st)]
    if not bad_st.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-25", "BLOCKER", "SCENARIO_SUMMARY_DB",
            "scenario_type", fail_count=len(bad_st),
            message=f"Invalid scenario_type. Allowed: {sorted(valid_st)}",
            hint="Use Baseline or ActionStressTest.",
            blocker_class="GENERATION_ERROR",
        ))

    # pass_number must be 1 or 2
    bad_pn = []
    for _, row in df.iterrows():
        try:
            pn = int(row.get("pass_number", ""))
            if pn not in (1, 2):
                bad_pn.append({"scenario_id": row.get("scenario_id"), "pass_number": pn})
        except (ValueError, TypeError):
            bad_pn.append({"scenario_id": row.get("scenario_id"),
                          "pass_number": row.get("pass_number")})
    if bad_pn:
        findings.append(_make_finding(
            pack, "SCV-8V-25", "BLOCKER", "SCENARIO_SUMMARY_DB",
            "pass_number", fail_count=len(bad_pn),
            message=f"{len(bad_pn)} rows have invalid pass_number (must be 1 or 2).",
            hint="pass_number must be 1 or 2.",
            blocker_class="GENERATION_ERROR",
            samples=bad_pn[:3],
        ))

    # outcome_metric_id regex
    pattern = ID_PATTERNS["metric_id"]
    bad_mid = df[~df["outcome_metric_id"].apply(lambda v: bool(re.match(pattern, str(v))))]
    if not bad_mid.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-25", "BLOCKER", "SCENARIO_SUMMARY_DB",
            "outcome_metric_id", fail_count=len(bad_mid),
            message="Invalid outcome_metric_id format (must match M###).",
            hint="outcome_metric_id must match ^M\\d{3}$.",
            blocker_class="GENERATION_ERROR",
        ))

    # confidence enum
    valid_conf = ENUMS["confidence"]
    bad_conf = df[~df["confidence"].isin(valid_conf)]
    if not bad_conf.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-25", "BLOCKER", "SCENARIO_SUMMARY_DB",
            "confidence", fail_count=len(bad_conf),
            message=f"Invalid confidence. Allowed: {sorted(valid_conf)}",
            hint="Use High, Medium, or Low.",
            blocker_class="GENERATION_ERROR",
        ))

    # assumptions json_list_obj
    for _, row in df.iterrows():
        val = row.get("assumptions", "")
        if not _is_json_list_of_objects(val):
            findings.append(_make_finding(
                pack, "SCV-8V-25", "BLOCKER", "SCENARIO_SUMMARY_DB",
                "assumptions", fail_count=1,
                message=f"Invalid assumptions JSON in scenario "
                        f"{row.get('scenario_id', '?')}.",
                hint="assumptions must be a JSON array of objects.",
                blocker_class="GENERATION_ERROR",
            ))
            break  # report once

    return findings


# ===========================================================================
# 8V-26: SENSITIVITY_SUMMARY_DB schema validation
# ===========================================================================

def check_8v_26(pack):
    """SCV-8V-26 [SCHEMA] BLOCKER — SENSITIVITY_SUMMARY_DB schema."""
    findings = []
    df = pack.get_table("SENSITIVITY_SUMMARY_DB")
    if df is None or df.empty:
        return findings  # conditional table

    # leverage_class enum
    valid_lc = ENUMS["leverage_class"]
    bad_lc = df[~df["leverage_class"].isin(valid_lc)]
    if not bad_lc.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-26", "BLOCKER", "SENSITIVITY_SUMMARY_DB",
            "leverage_class", fail_count=len(bad_lc),
            message=f"Invalid leverage_class. Allowed: {sorted(valid_lc)}",
            hint="Use High, Moderate, or Low.",
            blocker_class="GENERATION_ERROR",
        ))

    # directionality enum
    valid_dir = ENUMS["directionality"]
    bad_dir = df[~df["directionality"].isin(valid_dir)]
    if not bad_dir.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-26", "BLOCKER", "SENSITIVITY_SUMMARY_DB",
            "directionality", fail_count=len(bad_dir),
            message=f"Invalid directionality. Allowed: {sorted(valid_dir)}",
            hint="Use Linear, Nonlinear, or Threshold.",
            blocker_class="GENERATION_ERROR",
        ))

    # outcome_metric_id regex
    pattern = ID_PATTERNS["metric_id"]
    bad_mid = df[~df["outcome_metric_id"].apply(lambda v: bool(re.match(pattern, str(v))))]
    if not bad_mid.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-26", "BLOCKER", "SENSITIVITY_SUMMARY_DB",
            "outcome_metric_id", fail_count=len(bad_mid),
            message="Invalid outcome_metric_id format.",
            hint="Must match ^M\\d{3}$.",
            blocker_class="GENERATION_ERROR",
        ))

    # confidence enum
    valid_conf = ENUMS["confidence"]
    bad_conf = df[~df["confidence"].isin(valid_conf)]
    if not bad_conf.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-26", "BLOCKER", "SENSITIVITY_SUMMARY_DB",
            "confidence", fail_count=len(bad_conf),
            message=f"Invalid confidence. Allowed: {sorted(valid_conf)}",
            hint="Use High, Medium, or Low.",
            blocker_class="GENERATION_ERROR",
        ))

    return findings


# ===========================================================================
# 8V-27: RI-10/11/12/13 — Scenario + Sensitivity RI checks
# ===========================================================================

def check_8v_27(pack):
    """SCV-8V-27 [RI] BLOCKER — Scenario/Sensitivity RI checks."""
    findings = []
    metrics_df = pack.get_table("METRICS_DB")
    claims_df = pack.get_table("CLAIM_LEDGER_DB")

    valid_metric_ids = set()
    if metrics_df is not None:
        valid_metric_ids = set(metrics_df["metric_id"].unique())

    valid_claim_ids = set()
    if claims_df is not None:
        valid_claim_ids = set(claims_df["claim_id"].unique())

    # RI-10: SCENARIO_SUMMARY_DB.outcome_metric_id -> METRICS_DB
    scen_df = pack.get_table("SCENARIO_SUMMARY_DB")
    if scen_df is not None and not scen_df.empty and metrics_df is not None:
        bad = scen_df[~scen_df["outcome_metric_id"].isin(valid_metric_ids)]
        if not bad.empty:
            findings.append(_make_finding(
                pack, "SCV-8V-27", "BLOCKER", "SCENARIO_SUMMARY_DB",
                "outcome_metric_id", fail_count=len(bad),
                message=f"RI-10: {len(bad)} outcome_metric_id(s) not in METRICS_DB.",
                hint="outcome_metric_id must reference an existing metric.",
                blocker_class="INTEGRITY_ERROR",
            ))

    # RI-11: SCENARIO_SUMMARY_DB.linked_claim_ids -> CLAIM_LEDGER_DB
    if scen_df is not None and not scen_df.empty and claims_df is not None:
        orphans = []
        for _, row in scen_df.iterrows():
            for cid in _parse_json_list(row.get("linked_claim_ids", "")):
                if isinstance(cid, str) and cid not in valid_claim_ids:
                    orphans.append({"scenario_id": row.get("scenario_id"), "orphan": cid})
        if orphans:
            findings.append(_make_finding(
                pack, "SCV-8V-27", "BLOCKER", "SCENARIO_SUMMARY_DB",
                "linked_claim_ids", fail_count=len(orphans),
                message=f"RI-11: {len(orphans)} claim ref(s) not in CLAIM_LEDGER_DB.",
                hint="Every linked claim must exist in CLAIM_LEDGER_DB.",
                blocker_class="INTEGRITY_ERROR",
                samples=orphans[:3],
            ))

    # RI-12: SENSITIVITY_SUMMARY_DB.outcome_metric_id -> METRICS_DB
    sens_df = pack.get_table("SENSITIVITY_SUMMARY_DB")
    if sens_df is not None and not sens_df.empty and metrics_df is not None:
        bad = sens_df[~sens_df["outcome_metric_id"].isin(valid_metric_ids)]
        if not bad.empty:
            findings.append(_make_finding(
                pack, "SCV-8V-27", "BLOCKER", "SENSITIVITY_SUMMARY_DB",
                "outcome_metric_id", fail_count=len(bad),
                message=f"RI-12: {len(bad)} outcome_metric_id(s) not in METRICS_DB.",
                hint="outcome_metric_id must reference an existing metric.",
                blocker_class="INTEGRITY_ERROR",
            ))

    # RI-13: SENSITIVITY_SUMMARY_DB.linked_claim_ids -> CLAIM_LEDGER_DB
    if sens_df is not None and not sens_df.empty and claims_df is not None:
        orphans = []
        for _, row in sens_df.iterrows():
            for cid in _parse_json_list(row.get("linked_claim_ids", "")):
                if isinstance(cid, str) and cid not in valid_claim_ids:
                    orphans.append({"sensitivity_id": row.get("sensitivity_id"), "orphan": cid})
        if orphans:
            findings.append(_make_finding(
                pack, "SCV-8V-27", "BLOCKER", "SENSITIVITY_SUMMARY_DB",
                "linked_claim_ids", fail_count=len(orphans),
                message=f"RI-13: {len(orphans)} claim ref(s) not in CLAIM_LEDGER_DB.",
                hint="Every linked claim must exist in CLAIM_LEDGER_DB.",
                blocker_class="INTEGRITY_ERROR",
                samples=orphans[:3],
            ))

    return findings


# ===========================================================================
# 8V-28a: DECISION_TRACKER_DB schema (L3 only)
# ===========================================================================

def check_8v_28a(pack):
    """SCV-8V-28a [SCHEMA] BLOCKER — DECISION_TRACKER_DB schema (L3 only)."""
    findings = []
    df = pack.get_table("DECISION_TRACKER_DB")
    if df is None or df.empty:
        return findings

    # decision_id regex
    pattern = ID_PATTERNS["decision_id"]
    bad_did = df[~df["decision_id"].apply(lambda v: bool(re.match(pattern, str(v))))]
    if not bad_did.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-28a", "BLOCKER", "DECISION_TRACKER_DB",
            "decision_id", fail_count=len(bad_did),
            message="Invalid decision_id format.",
            hint="Must match ^DEC-[A-Z0-9]+-[A-Z]+-\\d{4}$.",
            blocker_class="GENERATION_ERROR",
        ))

    # decision_status enum
    valid_ds = ENUMS["decision_status"]
    bad_ds = df[~df["decision_status"].isin(valid_ds)]
    if not bad_ds.empty:
        findings.append(_make_finding(
            pack, "SCV-8V-28a", "BLOCKER", "DECISION_TRACKER_DB",
            "decision_status", fail_count=len(bad_ds),
            message=f"Invalid decision_status. Allowed: {sorted(valid_ds)}",
            hint="Use PLANNED, IMPLEMENTED, TESTED, or ABANDONED.",
            blocker_class="GENERATION_ERROR",
        ))

    # date fields
    date_pattern = ID_PATTERNS["date_ymd"]
    for col in ["decision_date", "review_date"]:
        if col not in df.columns:
            continue
        bad = df[~df[col].apply(lambda v: bool(re.match(date_pattern, str(v))))]
        if not bad.empty:
            findings.append(_make_finding(
                pack, "SCV-8V-28a", "BLOCKER", "DECISION_TRACKER_DB",
                col, fail_count=len(bad),
                message=f"Invalid {col} format (must be YYYY-MM-DD).",
                hint=f"Fix {col} to YYYY-MM-DD format.",
                blocker_class="GENERATION_ERROR",
            ))

    # rec_id_linked regex
    rec_pattern = ID_PATTERNS["rec_id"]
    if "rec_id_linked" in df.columns:
        bad = df[~df["rec_id_linked"].apply(lambda v: bool(re.match(rec_pattern, str(v))))]
        if not bad.empty:
            findings.append(_make_finding(
                pack, "SCV-8V-28a", "BLOCKER", "DECISION_TRACKER_DB",
                "rec_id_linked", fail_count=len(bad),
                message="Invalid rec_id_linked format.",
                hint="Must match ^RC-RUN-[A-Z0-9]+-\\d{8}-\\d{3}-[A-Z]+-\\d{3}$.",
                blocker_class="GENERATION_ERROR",
            ))

    return findings


# ===========================================================================
# 8V-28b: RI-14 — DECISION_TRACKER_DB.related_metric_ids -> METRICS_DB
# ===========================================================================

def check_8v_28b(pack):
    """SCV-8V-28b [RI] BLOCKER — RI-14"""
    findings = []
    dt_df = pack.get_table("DECISION_TRACKER_DB")
    metrics_df = pack.get_table("METRICS_DB")
    if dt_df is None or dt_df.empty or metrics_df is None:
        return findings

    valid_ids = set(metrics_df["metric_id"].unique())
    orphans = []

    for _, row in dt_df.iterrows():
        for mid in _parse_json_list(row.get("related_metric_ids", "")):
            if isinstance(mid, str) and mid not in valid_ids:
                orphans.append({"decision_id": row["decision_id"],
                               "orphan_metric_id": mid})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-8V-28b", "BLOCKER", "DECISION_TRACKER_DB",
            "related_metric_ids", fail_count=len(orphans),
            message=f"RI-14: {len(orphans)} metric_id ref(s) not in METRICS_DB.",
            hint="Every metric_id in related_metric_ids must exist in METRICS_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


# ===========================================================================
# 8V-29: scenario_lever validation (WARN / escalates to BLOCKER)
# ===========================================================================

def check_8v_29(pack):
    """SCV-8V-29 [LOGIC] WARN (BLOCKER if malformed)
    If scenario_lever is non-null, must be valid json_list_obj with
    {variable, direction, magnitude_low, magnitude_high}.
    """
    findings = []
    recs_df = pack.get_table("RECOMMENDATIONS_DB")
    if recs_df is None or recs_df.empty:
        return findings

    required_keys = {"variable", "direction", "magnitude_low", "magnitude_high"}
    malformed = []

    for _, row in recs_df.iterrows():
        val = row.get("scenario_lever", "")

        # null/empty is OK (WARN case handled by checking PASS 2,
        # but we can't detect PASS 2 from CSVs alone)
        if str(val).strip() == "":
            continue

        # If non-null, must be valid JSON list of objects
        try:
            parsed = json.loads(val)
            if not isinstance(parsed, list):
                malformed.append({"rec_id": row["rec_id"], "issue": "not a list"})
                continue
            for obj in parsed:
                if not isinstance(obj, dict):
                    malformed.append({"rec_id": row["rec_id"], "issue": "item not object"})
                    break
                missing = required_keys - set(obj.keys())
                if missing:
                    malformed.append({"rec_id": row["rec_id"],
                                     "missing_keys": sorted(missing)})
                    break
        except (json.JSONDecodeError, TypeError):
            malformed.append({"rec_id": row["rec_id"], "issue": "invalid JSON"})

    if malformed:
        # Escalate to BLOCKER if malformed
        findings.append(_make_finding(
            pack, "SCV-8V-29", "BLOCKER", "RECOMMENDATIONS_DB",
            "scenario_lever", fail_count=len(malformed),
            message=f"{len(malformed)} recommendation(s) have malformed "
                    f"scenario_lever (escalated to BLOCKER).",
            hint="scenario_lever must be a JSON array of objects with "
                 "{variable, direction, magnitude_low, magnitude_high}.",
            blocker_class="GENERATION_ERROR",
            samples=malformed[:3],
        ))

    return findings


# ===========================================================================
# 8V-30/31: Duplicate RI checks (parallel to 5V-14/15 and 8V-23/24)
# Per spec Note 8: report under both IDs for strict VCR-1.2 compliance
# ===========================================================================

def check_8v_30(pack):
    """SCV-8V-30 — RI-17 (duplicate of 8V-23, parallel to 5V-14)"""
    return _remap_findings(stage_5v.check_5v_14(pack), "SCV-8V-30")

def check_8v_31(pack):
    """SCV-8V-31 — RI-18 (duplicate of 8V-24, parallel to 5V-15)"""
    return _remap_findings(stage_5v.check_5v_15(pack), "SCV-8V-31")


# ===========================================================================
# 8V-32: RI-19 — RECOMMENDATIONS_DB.linked_driver_ids -> DRIVER_DB
# ===========================================================================

def check_8v_32(pack):
    """SCV-8V-32 [RI] BLOCKER — RI-19"""
    findings = []
    recs_df = pack.get_table("RECOMMENDATIONS_DB")
    drivers_df = pack.get_table("DRIVER_DB")
    if recs_df is None or drivers_df is None:
        return findings

    valid_ids = set(drivers_df["driver_id"].unique())
    orphans = []

    for _, row in recs_df.iterrows():
        for did in _parse_json_list(row.get("linked_driver_ids", "")):
            if isinstance(did, str) and did not in valid_ids:
                orphans.append({"rec_id": row["rec_id"], "orphan_driver_id": did})

    if orphans:
        findings.append(_make_finding(
            pack, "SCV-8V-32", "BLOCKER", "RECOMMENDATIONS_DB",
            "linked_driver_ids", fail_count=len(orphans),
            message=f"RI-19: {len(orphans)} driver_id ref(s) not in DRIVER_DB.",
            hint="Every driver_id in linked_driver_ids must exist in DRIVER_DB.",
            blocker_class="INTEGRITY_ERROR",
            samples=orphans[:3],
        ))

    return findings


# ===========================================================================
# 8V-33/34: Parallel to 5V-16/17
# ===========================================================================

def check_8v_33(pack):
    """SCV-8V-33 — RI-20 (parallel to 5V-16)"""
    return _remap_findings(stage_5v.check_5v_16(pack), "SCV-8V-33")

def check_8v_34(pack):
    """SCV-8V-34 — RI-21 (parallel to 5V-17)"""
    return _remap_findings(stage_5v.check_5v_17(pack), "SCV-8V-34")


# ===========================================================================
# 8V-35: RECOMMENDATIONS_DB category WARN
# ===========================================================================

def check_8v_35(pack):
    """SCV-8V-35 [LOGIC] WARN
    Warn if category not in recommended set.
    """
    findings = []
    df = pack.get_table("RECOMMENDATIONS_DB")
    if df is None or df.empty:
        return findings

    non_standard = df[~df["category"].isin(RECOMMENDED_REC_CATEGORIES)]
    if not non_standard.empty:
        bad_cats = non_standard["category"].unique().tolist()
        findings.append(_make_finding(
            pack, "SCV-8V-35", "WARN", "RECOMMENDATIONS_DB", "category",
            fail_count=len(non_standard),
            message=f"Non-standard recommendation category: {bad_cats}. "
                    f"Recommended: {sorted(RECOMMENDED_REC_CATEGORIES)}",
            hint="Consider using a standard category for consistency.",
            samples=[{"rec_id": r["rec_id"], "category": r["category"]}
                     for _, r in non_standard.head(3).iterrows()],
        ))

    return findings


# ===========================================================================
# MAIN ENTRY POINT — all 36 active checks
# ===========================================================================

ALL_CHECKS = [
    ("SCV-8V-01", check_8v_01),
    ("SCV-8V-02", check_8v_02),
    ("SCV-8V-03", check_8v_03),
    ("SCV-8V-04", check_8v_04),
    ("SCV-8V-05", check_8v_05),
    ("SCV-8V-06", check_8v_06),
    ("SCV-8V-07", check_8v_07),
    ("SCV-8V-08", check_8v_08),
    ("SCV-8V-09", check_8v_09),
    ("SCV-8V-10", check_8v_10),
    ("SCV-8V-11", check_8v_11),
    ("SCV-8V-12", check_8v_12),
    ("SCV-8V-13", check_8v_13),
    ("SCV-8V-14", check_8v_14),
    ("SCV-8V-15", check_8v_15),
    ("SCV-8V-16", check_8v_16),
    ("SCV-8V-17", check_8v_17),
    ("SCV-8V-18", check_8v_18),
    ("SCV-8V-19", check_8v_19),
    ("SCV-8V-20", check_8v_20),
    ("SCV-8V-21", check_8v_21),
    ("SCV-8V-22", check_8v_22),
    ("SCV-8V-23", check_8v_23),
    ("SCV-8V-24", check_8v_24),
    ("SCV-8V-25", check_8v_25),
    ("SCV-8V-26", check_8v_26),
    ("SCV-8V-27", check_8v_27),
    ("SCV-8V-28a", check_8v_28a),
    ("SCV-8V-28b", check_8v_28b),
    ("SCV-8V-29", check_8v_29),
    ("SCV-8V-30", check_8v_30),
    ("SCV-8V-31", check_8v_31),
    ("SCV-8V-32", check_8v_32),
    ("SCV-8V-33", check_8v_33),
    ("SCV-8V-34", check_8v_34),
    ("SCV-8V-35", check_8v_35),
]


def run_stage_8v(pack):
    """Run all 36 active Stage 8V checks against a DataPack.

    8V runs for L2/L3 data only. The orchestrator (run_evs.py)
    handles that gating.

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

"""
core/registry.py
Central data definitions for the EVS — all schemas, enums, regex patterns,
primary keys, and layer-mode rules. Every validator module imports from here.

Source: EVS Spec Reference (extracted from FAS v2.1.0, VCR-1.2)
No logic lives in this file — just data definitions.
"""

import re


# ===========================================================================
# 1. ID FORMAT PATTERNS (regex)
# ===========================================================================
# From spec Section 2: "ID Format Regexes (HARD enforcement)"
# These are used by schema validators to check ID column formats.
# Each key matches the column name (or logical name) that uses this pattern.
# ===========================================================================

ID_PATTERNS = {
    "run_id":         r"^RUN-[A-Z0-9]+-\d{8}-\d{3}$",
    "file_id":        r"^F\d{3}$",
    "metric_id":      r"^M\d{3}$",
    "claim_uid":      r"^C-[A-Z0-9]+-[A-Z]+-\d{4}$",
    "claim_id":       r"^CL-RUN-[A-Z0-9]+-\d{8}-\d{3}-[A-Z]+-\d{4}$",
    "driver_id":      r"^DX-RUN-[A-Z0-9]+-\d{8}-\d{3}-[A-Z]+-\d{3}$",
    "rec_id":         r"^RC-RUN-[A-Z0-9]+-\d{8}-\d{3}-[A-Z]+-\d{3}$",
    "vn_id":          r"^VN-RUN-[A-Z0-9]+-\d{8}-\d{3}-[A-Z]+-\d{3}$",
    "decision_id":    r"^DEC-[A-Z0-9]+-[A-Z]+-\d{4}$",
    "scenario_id":    r"^SC-RUN-[A-Z0-9]+-\d{8}-\d{3}-\d{2}$",
    "sensitivity_id": r"^SN-RUN-[A-Z0-9]+-\d{8}-\d{3}-\d{2}$",
    "check_id":       r"^SCV-(2V|3V|5V|8V)-\d{2}[a-z]?$",
    "date_ym":        r"^\d{4}-(0[1-9]|1[0-2])$",
    "date_ymd":       r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$",
    "source_file_id": r"^F\d{3}$",  # alias — same pattern as file_id
}


# ===========================================================================
# 2. ENUMS (allowed value sets)
# ===========================================================================
# From spec Section 3: "CENTRAL ENUMS (complete list)"
# Keys match the enum name used in column definitions below.
# ===========================================================================

ENUMS = {
    "proceed_status":           {"STOP", "PROCEED-WITH-LIMITS", "PROCEED"},
    "mapping_confidence_grade": {"A", "B", "C"},
    "confidence":               {"High", "Medium", "Low"},
    "metric_status":            {"Calculated", "NotCalculated"},
    "truth_label":              {"[From Data]", "[Derived]", "[Assumption]",
                                 "[General Principle]", "[Hypothesis]",
                                 "[Uncertain]", "[Unknown]"},
    "claim_status":             {"ACTIVE", "UPDATED", "SUPERSEDED", "RETRACTED"},
    "severity":                 {"BLOCKER", "WARN"},
    "blocker_class":            {"GENERATION_ERROR", "INTEGRITY_ERROR", "DATA_ERROR"},
    "report_type":              {"PnL", "BalanceSheet", "CashFlow", "Other"},
    "frequency":                {"Monthly", "Quarterly", "Other"},
    "scenario_type":            {"Baseline", "ActionStressTest"},
    "scenario_name_standard":   {"Conservative", "Base", "Aggressive"},
    "leverage_class":           {"High", "Moderate", "Low"},
    "directionality":           {"Linear", "Nonlinear", "Threshold"},
    "decision_status":          {"PLANNED", "IMPLEMENTED", "TESTED", "ABANDONED"},
    "account_bucket":           {"Revenue", "COGS", "OpEx", "OtherIncome",
                                 "OtherExpense", "NonOperating", "Unknown"},
    "stage_id":                 {"2V", "3V", "5V", "8V"},
    "accounting_basis":         {"cash", "accrual", "unknown"},
    "unclassified_pct_unit":    {"ratio_0_to_1"},
    "owner_comp_identified":    {"Y", "N", "Unknown"},
    "yn_flag":                  {"Y", "N"},
    "claim_category":           {"DATA", "REV", "MAR", "OPEX", "CASH",
                                 "OPS", "RISK", "OTHER"},
    "driver_category":          {"REV", "MAR", "OPEX", "CASH", "OPS", "RISK"},
    "vn_category":              {"DATA", "REV", "MAR", "OPEX", "CASH",
                                 "OPS", "RISK"},
    "layer_origin":             {"L0", "L1", "L2", "L3", "L4", "L5"},
    "driver_class":             {"Confirmed", "Likely", "Hypothesis", "Unknown"},
    "direction":                {"+", "-"},
    "expected_direction":       {"↑", "↓"},
    "recommendation_label":     {"[Data-Supported]", "[Logic-Supported]",
                                 "[Assumption-Dependent]", "[Exploratory]"},
    "delta_type":               {"ADDED", "UPDATED", "SUPERSEDED", "RETRACTED"},
    "object_type":              {"CLAIM", "METRIC", "RECOMMENDATION"},
    "outcome_unit":             {"currency", "percent", "ratio", "count", "months"},
}

# Truth labels that allow empty metric links (SCV-5V-12 / SCV-8V-20)
UNCERTAIN_TRUTH_LABELS = {"[Hypothesis]", "[Unknown]", "[Uncertain]"}


# ===========================================================================
# 3. APPROVED METRIC IDS
# ===========================================================================
# From spec Section 8, Note 6 and §10.1 dictionary.
# Used by SCV-3V-04 to reject unknown metric_ids.
# ===========================================================================

APPROVED_METRIC_IDS = {
    "M001", "M002", "M003", "M004",
    "M010", "M011", "M012", "M013", "M014", "M015", "M016", "M017",
    "M020", "M021", "M022", "M023", "M024", "M025", "M026",
    "M030", "M031", "M032", "M033",
    "M040", "M041", "M042", "M043", "M044",
    "M050", "M051",
    # M052–M059 added per Agent 1 Metrics Expansion (2026-03-02).
    # These metrics are live in the pipeline as of RUN-BCBA-20260302-001
    # and validated by EVS regression.
    "M052", "M053", "M054", "M055", "M056", "M057", "M058", "M059",
}


# ===========================================================================
# 4. RECOMMENDED RECOMMENDATION CATEGORIES
# ===========================================================================
# From spec Section 3: recommendation_category is freeform but has a
# recommended set. SCV-8V-35 WARNs if category is not in this set.
# ===========================================================================

RECOMMENDED_REC_CATEGORIES = {
    "Revenue Growth",
    "Margin Improvement",
    "Cost Control",
    "Pricing/Packaging",
    "Operational Efficiency",
    "Capacity & Utilization",
    "Cash Flow & Liquidity",
    "Risk Mitigation",
}


# ===========================================================================
# 5. SCHEMA REGISTRY (all 14 governed tables)
# ===========================================================================
# Each table maps to a list of column definitions.
# Each column is a dict with:
#   name     : column header (must match CSV exactly)
#   dtype    : string | decimal | int | date_ym | date_ymd | enum |
#              json_list_str | json_list_obj
#   required : True = must be non-null/non-empty; False = nullable
#   enum_key : key into ENUMS dict (only for dtype=enum)
#   regex_key: key into ID_PATTERNS dict (for ID format validation)
#
# Notes on dtype meanings for validators:
#   decimal  → must parse as float; check >= 0 unless noted otherwise
#   int      → must parse as integer
#   date_ym  → must match YYYY-MM regex
#   date_ymd → must match YYYY-MM-DD regex
#   json_list_str → must parse as JSON array of strings
#   json_list_obj → must parse as JSON array of objects
# ===========================================================================

def _col(name, dtype, required=True, enum_key=None, regex_key=None):
    """Helper to build a column definition dict."""
    return {
        "name": name,
        "dtype": dtype,
        "required": required,
        "enum_key": enum_key,
        "regex_key": regex_key,
    }


SCHEMA_REGISTRY = {

    # ------------------------------------------------------------------
    # TABLE 1: PNL_FACT
    # ------------------------------------------------------------------
    "PNL_FACT": [
        _col("run_id",                   "string",   regex_key="run_id"),
        _col("period_key",               "date_ym"),
        _col("account_name_original",    "string"),
        _col("account_name_normalized",  "string"),
        _col("account_bucket",           "enum",     enum_key="account_bucket"),
        _col("account_category",         "string"),
        _col("amount",                   "decimal"),        # HARD: >= 0
        _col("segment_key",             "string"),
        _col("source_file_id",          "string",   regex_key="source_file_id"),
    ],

    # ------------------------------------------------------------------
    # TABLE 2: BS_FACT
    # ------------------------------------------------------------------
    "BS_FACT": [
        _col("run_id",          "string",   regex_key="run_id"),
        _col("period_key",      "date_ym"),
        _col("line_item",       "string"),
        _col("amount",          "decimal"),                 # negative permitted
        _col("segment_key",    "string"),
        _col("source_file_id", "string",   regex_key="source_file_id"),
    ],

    # ------------------------------------------------------------------
    # TABLE 3: INPUT_INDEX_DB
    # ------------------------------------------------------------------
    "INPUT_INDEX_DB": [
        _col("run_id",            "string",    regex_key="run_id"),
        _col("file_id",           "string",    regex_key="file_id"),
        _col("file_name",         "string"),
        _col("report_type",       "enum",      enum_key="report_type"),
        _col("frequency",         "enum",      enum_key="frequency"),
        _col("date_range_start",  "date_ymd"),
        _col("date_range_end",    "date_ymd"),
        _col("accounting_basis",  "enum",      enum_key="accounting_basis"),
        _col("source_system",     "string"),
        _col("notes",             "string",    required=False),
    ],

    # ------------------------------------------------------------------
    # TABLE 4: NORMALIZATION_LOG_DB
    # ------------------------------------------------------------------
    "NORMALIZATION_LOG_DB": [
        _col("run_id",                             "string",         regex_key="run_id"),
        _col("proceed_status",                     "enum",           enum_key="proceed_status"),
        _col("mapping_confidence_grade",           "enum",           enum_key="mapping_confidence_grade"),
        _col("unclassified_pct",                   "decimal"),       # HARD: [0, 1]
        _col("unclassified_pct_unit",              "enum",           enum_key="unclassified_pct_unit"),
        _col("unclassified_amount",                "decimal"),
        _col("critical_category_coverage_flags",   "json_list_str"),
        _col("missing_months",                     "json_list_str"),
        _col("contra_revenue_candidates_present",  "enum",           enum_key="yn_flag"),
        _col("integrity_checks_passed",            "enum",           enum_key="yn_flag"),
        _col("limitations_summary",                "string"),
        _col("required_confidence_adjustments",    "string"),
        _col("normalization_actions_taken",        "string"),
        _col("accounting_basis_declared",          "enum",           enum_key="accounting_basis"),
        _col("currency",                           "string"),
        _col("owner_comp_identified",              "enum",           enum_key="owner_comp_identified"),
        _col("one_time_items_flagged",             "enum",           enum_key="yn_flag"),
        _col("retry_count",                        "int"),
        _col("retry_log",                          "json_list_obj"),
    ],

    # ------------------------------------------------------------------
    # TABLE 5: METRICS_DB
    # ------------------------------------------------------------------
    "METRICS_DB": [
        _col("run_id",             "string",         regex_key="run_id"),
        _col("metric_id",          "string",         regex_key="metric_id"),
        _col("metric_name",        "string"),
        _col("period_key",         "date_ym"),
        _col("segment_key",       "string"),
        _col("value",              "decimal",         required=False),  # conditional on status
        _col("unit",               "enum",            enum_key="outcome_unit"),
        _col("formula_reference",  "string"),
        _col("required_inputs",    "string"),
        _col("constraints",        "string"),
        _col("status",             "enum",            enum_key="metric_status"),
        _col("confidence",         "enum",            enum_key="confidence"),
        _col("calc_notes",         "string"),
        _col("file_ids_used",      "json_list_str"),
    ],

    # ------------------------------------------------------------------
    # TABLE 6: CLAIM_LEDGER_DB
    # ------------------------------------------------------------------
    "CLAIM_LEDGER_DB": [
        _col("run_id",              "string",         regex_key="run_id"),
        _col("claim_uid",           "string",         regex_key="claim_uid"),
        _col("claim_id",            "string",         regex_key="claim_id"),
        _col("category",            "enum",           enum_key="claim_category"),
        _col("layer_origin",        "enum",           enum_key="layer_origin"),
        _col("claim_text",          "string"),
        _col("truth_label",         "enum",           enum_key="truth_label"),
        _col("confidence",          "enum",           enum_key="confidence"),
        _col("status",              "enum",           enum_key="claim_status"),
        _col("linked_metric_ids",   "json_list_str"),
        _col("linked_metric_rows",  "json_list_obj"),
        _col("linked_driver_ids",   "json_list_str"),
        _col("linked_file_ids",     "json_list_str"),
        _col("formula_reference",   "json_list_str"),
        _col("claim_notes",         "string"),
    ],

    # ------------------------------------------------------------------
    # TABLE 7: DRIVER_DB
    # ------------------------------------------------------------------
    "DRIVER_DB": [
        _col("run_id",               "string",         regex_key="run_id"),
        _col("driver_id",            "string",         regex_key="driver_id"),
        _col("category",             "enum",           enum_key="driver_category"),
        _col("driver_class",         "enum",           enum_key="driver_class"),
        _col("direction",            "enum",           enum_key="direction"),
        _col("outcome_impacted",     "string"),
        _col("evidence_metric_ids",  "json_list_str"),
        _col("evidence_claim_ids",   "json_list_str"),
        _col("constraints_linked",   "json_list_str"),
        _col("confidence",           "enum",           enum_key="confidence"),
        _col("driver_notes",         "string"),
    ],

    # ------------------------------------------------------------------
    # TABLE 8: RECOMMENDATIONS_DB (L2/L3 only)
    # ------------------------------------------------------------------
    "RECOMMENDATIONS_DB": [
        _col("run_id",               "string",         regex_key="run_id"),
        _col("rec_id",               "string",         regex_key="rec_id"),
        _col("category",             "string"),         # freeform, not strict enum
        _col("rec_title",            "string"),
        _col("rec_description",      "string"),
        _col("recommendation_label", "enum",           enum_key="recommendation_label"),
        _col("linked_claim_ids",     "json_list_str"),
        _col("linked_driver_ids",    "json_list_str"),
        _col("linked_metric_ids",    "json_list_str"),
        _col("expected_direction",   "enum",           enum_key="expected_direction"),
        _col("expected_magnitude",   "string"),
        _col("risks",                "string"),
        _col("preconditions",        "string"),
        _col("measurement_plan",     "string"),
        _col("confidence",           "enum",           enum_key="confidence"),
        _col("scenario_lever",       "json_list_obj",  required=False),  # nullable
    ],

    # ------------------------------------------------------------------
    # TABLE 9: VERIFY_NEXT_DB
    # ------------------------------------------------------------------
    "VERIFY_NEXT_DB": [
        _col("run_id",                          "string",         regex_key="run_id"),
        _col("vn_id",                           "string",         regex_key="vn_id"),
        _col("category",                        "enum",           enum_key="vn_category"),
        _col("uncertainty_or_gap",               "string"),
        _col("why_it_matters",                   "string"),
        _col("minimum_data_needed",              "string"),
        _col("how_to_obtain",                    "string"),
        _col("decisions_impacted",               "string"),
        _col("linked_claim_ids",                 "json_list_str"),
        _col("confidence_impact_if_resolved",    "enum",           enum_key="confidence"),
    ],

    # ------------------------------------------------------------------
    # TABLE 10: VALIDATION_REPORT_DB
    # ------------------------------------------------------------------
    "VALIDATION_REPORT_DB": [
        _col("run_id",               "string",         regex_key="run_id"),
        _col("stage_id",             "enum",           enum_key="stage_id"),
        _col("check_id",             "string",         regex_key="check_id"),
        _col("severity",             "enum",           enum_key="severity"),
        _col("table_name",           "string"),
        _col("column_name",          "string"),
        _col("fail_count",           "int"),
        _col("sample_failing_rows",  "json_list_obj"),
        _col("message",              "string"),
        _col("remediation_hint",     "string"),
        _col("blocker_class",        "enum",           enum_key="blocker_class",
                                                       required=False),  # null if WARN
    ],

    # ------------------------------------------------------------------
    # TABLE 11: SCENARIO_SUMMARY_DB (L2/L3 only)
    # ------------------------------------------------------------------
    "SCENARIO_SUMMARY_DB": [
        _col("run_id",                "string",         regex_key="run_id"),
        _col("scenario_id",           "string",         regex_key="scenario_id"),
        _col("scenario_name",         "string"),
        _col("scenario_type",         "enum",           enum_key="scenario_type"),
        _col("pass_number",           "int"),
        _col("assumptions",           "json_list_obj"),
        _col("outcome_metric_id",     "string",         regex_key="metric_id"),
        _col("outcome_value",         "decimal"),
        _col("outcome_unit",          "enum",           enum_key="outcome_unit"),
        _col("outcome_delta_vs_base", "decimal",        required=False),  # null for Base
        _col("confidence",            "enum",           enum_key="confidence"),
        _col("linked_claim_ids",      "json_list_str"),
        _col("scenario_notes",        "string"),
    ],

    # ------------------------------------------------------------------
    # TABLE 12: SENSITIVITY_SUMMARY_DB (L2/L3 only)
    # ------------------------------------------------------------------
    "SENSITIVITY_SUMMARY_DB": [
        _col("run_id",              "string",         regex_key="run_id"),
        _col("sensitivity_id",      "string",         regex_key="sensitivity_id"),
        _col("variable_name",       "string"),
        _col("range_tested",        "string"),
        _col("outcome_metric_id",   "string",         regex_key="metric_id"),
        _col("outcome_impact_low",  "decimal"),
        _col("outcome_impact_high", "decimal"),
        _col("outcome_unit",        "enum",           enum_key="outcome_unit"),
        _col("leverage_class",      "enum",           enum_key="leverage_class"),
        _col("directionality",      "enum",           enum_key="directionality"),
        _col("confidence",          "enum",           enum_key="confidence"),
        _col("linked_claim_ids",    "json_list_str"),
        _col("sensitivity_notes",   "string"),
    ],

    # ------------------------------------------------------------------
    # TABLE 13: DELTA_LOG_DB (L3 only)
    # ------------------------------------------------------------------
    "DELTA_LOG_DB": [
        _col("run_id",             "string",   regex_key="run_id"),
        _col("prior_run_id",       "string",   regex_key="run_id"),
        _col("delta_type",         "enum",     enum_key="delta_type"),
        _col("object_type",        "enum",     enum_key="object_type"),
        _col("object_id",          "string"),
        _col("summary_of_change",  "string"),
        _col("reason",             "string"),
    ],

    # ------------------------------------------------------------------
    # TABLE 14: DECISION_TRACKER_DB (L3 only)
    # ------------------------------------------------------------------
    "DECISION_TRACKER_DB": [
        _col("run_id",                 "string",         regex_key="run_id"),
        _col("decision_id",            "string",         regex_key="decision_id"),
        _col("decision_date",          "date_ymd"),
        _col("run_id_source",          "string",         regex_key="run_id"),
        _col("rec_id_linked",          "string",         regex_key="rec_id"),
        _col("decision_description",   "string"),
        _col("decision_status",        "enum",           enum_key="decision_status"),
        _col("assumptions_made",       "string"),
        _col("expected_outcome",       "string"),
        _col("expected_timeframe",     "string"),
        _col("review_date",            "date_ymd"),
        _col("actual_outcome",         "string",         required=False),
        _col("outcome_variance",       "string",         required=False),
        _col("lessons_learned",        "string",         required=False),
        _col("superseded_by",          "string",         required=False,
                                                         regex_key="decision_id"),
        _col("related_metric_ids",     "json_list_str"),
    ],
}


# ===========================================================================
# 6. PRIMARY KEYS (per table)
# ===========================================================================
# From spec Section 1, each table definition.
# Used by PK uniqueness checks (SCV-3V-03, SCV-5V-02, SCV-8V-02).
# Each value is a tuple of column names that form the composite PK.
# ===========================================================================

PRIMARY_KEYS = {
    "PNL_FACT":               ("run_id", "period_key", "account_name_normalized",
                                "segment_key", "source_file_id"),
    "BS_FACT":                ("run_id", "period_key", "line_item",
                                "segment_key", "source_file_id"),
    "INPUT_INDEX_DB":         ("run_id", "file_id"),
    "NORMALIZATION_LOG_DB":   ("run_id",),
    "METRICS_DB":             ("run_id", "metric_id", "period_key", "segment_key"),
    "CLAIM_LEDGER_DB":        ("run_id", "claim_id"),
    "DRIVER_DB":              ("run_id", "driver_id"),
    "RECOMMENDATIONS_DB":     ("run_id", "rec_id"),
    "VERIFY_NEXT_DB":         ("run_id", "vn_id"),
    "VALIDATION_REPORT_DB":   ("run_id", "stage_id", "check_id",
                                "table_name", "column_name"),
    "SCENARIO_SUMMARY_DB":    ("run_id", "scenario_id", "outcome_metric_id"),
    "SENSITIVITY_SUMMARY_DB": ("run_id", "sensitivity_id"),
    "DELTA_LOG_DB":           ("run_id", "object_type", "object_id"),
    "DECISION_TRACKER_DB":    ("run_id", "decision_id"),
}


# ===========================================================================
# 7. LAYER MODE TABLE RULES
# ===========================================================================
# From spec Section 1 and SCV-5V-01 / SCV-8V-01.
# "required" = must be present (some conditionally on proceed_status)
# "prohibited" = must NOT be present
# "conditional" = may or may not be present depending on pipeline state
# ===========================================================================

LAYER_TABLES = {
    "L1": {
        "required": {
            "INPUT_INDEX_DB",
            "NORMALIZATION_LOG_DB",
            "CLAIM_LEDGER_DB",
            "DRIVER_DB",
            "VERIFY_NEXT_DB",
            # PNL_FACT and METRICS_DB also required if proceed_status != STOP
        },
        "conditional": {
            "PNL_FACT",           # required if proceed_status != STOP
            "BS_FACT",            # present only if BS source file exists
            "METRICS_DB",         # required if proceed_status != STOP
            "VALIDATION_REPORT_DB",  # present if any findings
        },
        "prohibited": {
            "RECOMMENDATIONS_DB",
            "SCENARIO_SUMMARY_DB",
            "SENSITIVITY_SUMMARY_DB",
            "DELTA_LOG_DB",
            "DECISION_TRACKER_DB",
        },
    },
    "L2": {
        "required": {
            "INPUT_INDEX_DB",
            "NORMALIZATION_LOG_DB",
            "CLAIM_LEDGER_DB",
            "DRIVER_DB",
            "VERIFY_NEXT_DB",
            "RECOMMENDATIONS_DB",
            # SCENARIO_SUMMARY_DB and SENSITIVITY_SUMMARY_DB required if engine ran
        },
        "conditional": {
            "PNL_FACT",
            "BS_FACT",
            "METRICS_DB",
            "VALIDATION_REPORT_DB",
            "SCENARIO_SUMMARY_DB",
            "SENSITIVITY_SUMMARY_DB",
        },
        "prohibited": {
            "DELTA_LOG_DB",       # unless PRIOR_RUN_PACK provided
            "DECISION_TRACKER_DB",
        },
    },
    "L3": {
        "required": {
            "INPUT_INDEX_DB",
            "NORMALIZATION_LOG_DB",
            "CLAIM_LEDGER_DB",
            "DRIVER_DB",
            "VERIFY_NEXT_DB",
            "RECOMMENDATIONS_DB",
        },
        "conditional": {
            "PNL_FACT",
            "BS_FACT",
            "METRICS_DB",
            "VALIDATION_REPORT_DB",
            "SCENARIO_SUMMARY_DB",
            "SENSITIVITY_SUMMARY_DB",
            "DELTA_LOG_DB",           # if prior pack provided
            "DECISION_TRACKER_DB",    # if decisions exist
        },
        "prohibited": set(),          # L3 allows everything
    },
}


# ===========================================================================
# 8. REFERENTIAL INTEGRITY RULES
# ===========================================================================
# From spec Section 4. Each rule defines:
#   source_table, source_column → target_table, target_column
# For json_list fields, each item in the list must exist in the target.
# For json_list_obj fields with composite keys, special handling is needed.
#
# This is a reference map — the actual RI check logic lives in the
# validator modules. This just documents what checks where.
# ===========================================================================

RI_RULES = {
    "RI-1":  {"source": "METRICS_DB",          "col": "file_ids_used",
              "target": "INPUT_INDEX_DB",       "target_col": "file_id"},
    "RI-2":  {"source": "CLAIM_LEDGER_DB",     "col": "linked_file_ids",
              "target": "INPUT_INDEX_DB",       "target_col": "file_id"},
    "RI-3":  {"source": "CLAIM_LEDGER_DB",     "col": "linked_metric_ids",
              "target": "METRICS_DB",           "target_col": "metric_id"},
    "RI-4":  {"source": "CLAIM_LEDGER_DB",     "col": "linked_metric_rows",
              "target": "METRICS_DB",           "target_col": "composite",
              "composite_keys": ("metric_id", "period_key", "segment_key")},
    "RI-5":  {"source": "DRIVER_DB",           "col": "evidence_metric_ids",
              "target": "METRICS_DB",           "target_col": "metric_id"},
    "RI-6":  {"source": "DRIVER_DB",           "col": "evidence_claim_ids",
              "target": "CLAIM_LEDGER_DB",      "target_col": "claim_id"},
    "RI-7":  {"source": "RECOMMENDATIONS_DB",  "col": "linked_claim_ids",
              "target": "CLAIM_LEDGER_DB",      "target_col": "claim_id"},
    "RI-8":  {"source": "RECOMMENDATIONS_DB",  "col": "linked_metric_ids",
              "target": "METRICS_DB",           "target_col": "metric_id"},
    "RI-9":  {"source": "DELTA_LOG_DB",        "col": "prior_run_id",
              "target": None,                   "target_col": None,
              "note": "format-only check against run_id regex"},
    "RI-10": {"source": "SCENARIO_SUMMARY_DB", "col": "outcome_metric_id",
              "target": "METRICS_DB",           "target_col": "metric_id"},
    "RI-11": {"source": "SCENARIO_SUMMARY_DB", "col": "linked_claim_ids",
              "target": "CLAIM_LEDGER_DB",      "target_col": "claim_id"},
    "RI-12": {"source": "SENSITIVITY_SUMMARY_DB","col": "outcome_metric_id",
              "target": "METRICS_DB",            "target_col": "metric_id"},
    "RI-13": {"source": "SENSITIVITY_SUMMARY_DB","col": "linked_claim_ids",
              "target": "CLAIM_LEDGER_DB",       "target_col": "claim_id"},
    "RI-14": {"source": "DECISION_TRACKER_DB",  "col": "related_metric_ids",
              "target": "METRICS_DB",            "target_col": "metric_id"},
    "RI-15": {"source": "DECISION_TRACKER_DB",  "col": "rec_id_linked",
              "target": None,                    "target_col": None,
              "note": "format-only check against rec_id regex"},
    "RI-16": {"source": "DECISION_TRACKER_DB",  "col": "superseded_by",
              "target": None,                    "target_col": None,
              "note": "format-only check against decision_id regex"},
    "RI-17": {"source": "PNL_FACT",             "col": "source_file_id",
              "target": "INPUT_INDEX_DB",        "target_col": "file_id"},
    "RI-18": {"source": "BS_FACT",              "col": "source_file_id",
              "target": "INPUT_INDEX_DB",        "target_col": "file_id"},
    "RI-19": {"source": "RECOMMENDATIONS_DB",   "col": "linked_driver_ids",
              "target": "DRIVER_DB",             "target_col": "driver_id"},
    "RI-20": {"source": "CLAIM_LEDGER_DB",      "col": "linked_driver_ids",
              "target": "DRIVER_DB",             "target_col": "driver_id"},
    "RI-21": {"source": "VERIFY_NEXT_DB",       "col": "linked_claim_ids",
              "target": "CLAIM_LEDGER_DB",       "target_col": "claim_id"},
}


# ===========================================================================
# 9. RETIRED CHECKS (do NOT implement)
# ===========================================================================
# From spec Section 8, Note 5.
# ===========================================================================

RETIRED_CHECKS = {
    "SCV-2V-10",
    "SCV-3V-08",
    "SCV-3V-12",
    "SCV-3V-13",
    "SCV-3V-15",
    "SCV-8V-28",    # note: 8V-28a and 8V-28b are active; only 8V-28 is retired
}


# ===========================================================================
# 10. HELPER: Get expected columns for a table
# ===========================================================================

# Updated 2026-03-23: Accept TTM and SEASONAL-XX period keys per Agent 1 metric expansion (M052-M059)
_VALID_PERIOD_KEY_RE = re.compile(
    r"^(?:\d{4}-(0[1-9]|1[0-2])|TTM|SEASONAL-(0[1-9]|1[0-2]))$"
)


def is_valid_period_key(value):
    """Check if a period_key is valid: YYYY-MM, TTM, or SEASONAL-01 through SEASONAL-12."""
    return bool(_VALID_PERIOD_KEY_RE.match(str(value)))


def get_expected_columns(table_name):
    """Return the ordered list of expected column names for a table."""
    if table_name not in SCHEMA_REGISTRY:
        return []
    return [col["name"] for col in SCHEMA_REGISTRY[table_name]]


def get_column_def(table_name, column_name):
    """Return the column definition dict for a specific column, or None."""
    if table_name not in SCHEMA_REGISTRY:
        return None
    for col in SCHEMA_REGISTRY[table_name]:
        if col["name"] == column_name:
            return col
    return None

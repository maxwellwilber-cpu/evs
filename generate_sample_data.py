"""
generate_sample_data.py
Creates sample FAS output CSVs for EVS validation testing.
Uses a FICTIONAL client with made-up financial data.
No real client data is present in this file.

Each CSV uses the FAS naming convention: <RUN_ID>__<TABLE_NAME>.csv
Planted errors are documented per-table so every SCV check has something to catch.

Run from the evs/ directory:
    /usr/bin/python3 generate_sample_data.py
"""

import csv
import os

# ---------------------------------------------------------------------------
# Setup — all CSVs go into data/ subfolder
# ---------------------------------------------------------------------------
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Fictional client: Riverside Soccer Academy (DEMO)
RUN_ID = "RUN-DEMO-20260315-001"

def write_csv(table_name, headers, rows):
    """Write a CSV file using the FAS naming convention."""
    filename = f"{RUN_ID}__{table_name}.csv"
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  Created {filename:<55} ({len(rows)} rows)")
    return filepath


# ===========================================================================
# TABLE 1: INPUT_INDEX_DB  (2 valid rows, 0 errors)
# ===========================================================================

write_csv("INPUT_INDEX_DB",
    ["run_id", "file_id", "file_name", "report_type", "frequency",
     "date_range_start", "date_range_end", "accounting_basis",
     "source_system", "notes"],
    [
        [RUN_ID, "F001", "Riverside_PnL_2025.csv", "PnL", "Monthly",
         "2025-01-01", "2025-06-30", "accrual", "Xero", ""],
        [RUN_ID, "F002", "Riverside_BS_2025.csv", "BalanceSheet", "Monthly",
         "2025-01-01", "2025-06-30", "accrual", "Xero", ""],
    ]
)


# ===========================================================================
# TABLE 2: NORMALIZATION_LOG_DB  (1 valid row, 0 errors)
# ===========================================================================

write_csv("NORMALIZATION_LOG_DB",
    ["run_id", "proceed_status", "mapping_confidence_grade",
     "unclassified_pct", "unclassified_pct_unit", "unclassified_amount",
     "critical_category_coverage_flags", "missing_months",
     "contra_revenue_candidates_present", "integrity_checks_passed",
     "limitations_summary", "required_confidence_adjustments",
     "normalization_actions_taken", "accounting_basis_declared",
     "currency", "owner_comp_identified", "one_time_items_flagged",
     "retry_count", "retry_log"],
    [
        [RUN_ID, "PROCEED", "A", "0.03", "ratio_0_to_1", "2100.00",
         "[]", "[]", "N", "Y",
         "6 months of data. Standard analysis window.",
         "None required",
         "Sign convention standardized. Summary rows excluded.",
         "accrual", "USD", "N", "N", "0", "[]"],
    ]
)


# ===========================================================================
# TABLE 3: PNL_FACT  (4 valid rows + 2 error rows)
# ===========================================================================
# PLANTED ERRORS:
#   Row 5: amount = -2200.00
#          → Violates SCV-3V-09 (PNL_FACT.amount must be >= 0)
#   Row 6: source_file_id = F999
#          → Violates SCV-5V-14 / SCV-8V-23/30 (RI-17: F999 not in INPUT_INDEX_DB)
# ===========================================================================

write_csv("PNL_FACT",
    ["run_id", "period_key", "account_name_original",
     "account_name_normalized", "account_bucket", "account_category",
     "amount", "segment_key", "source_file_id"],
    [
        [RUN_ID, "2025-01", "Camp Registration Fees", "Camp Registration Fees",
         "Revenue", "Core Revenue", "61500.00", "TOTAL", "F001"],
        [RUN_ID, "2025-01", "League Enrollment", "League Enrollment",
         "Revenue", "Core Revenue", "34800.00", "TOTAL", "F001"],
        [RUN_ID, "2025-01", "Coach Salaries", "Coach Salaries",
         "OpEx", "Payroll", "42000.00", "TOTAL", "F001"],
        [RUN_ID, "2025-01", "Field Lease", "Field Lease",
         "OpEx", "Rent/Occupancy", "8500.00", "TOTAL", "F001"],
        # ERROR: negative amount → SCV-3V-09
        [RUN_ID, "2025-01", "Equipment Returns", "Equipment Returns",
         "OpEx", "Admin/Overhead", "-2200.00", "TOTAL", "F001"],
        # ERROR: orphan file_id F999 → SCV-5V-14 / RI-17
        [RUN_ID, "2025-02", "Referee Fees", "Referee Fees",
         "OpEx", "Admin/Overhead", "3100.00", "TOTAL", "F999"],
    ]
)


# ===========================================================================
# TABLE 4: BS_FACT  (2 valid rows, 0 errors)
# ===========================================================================

write_csv("BS_FACT",
    ["run_id", "period_key", "line_item", "amount",
     "segment_key", "source_file_id"],
    [
        [RUN_ID, "2025-01", "Cash", "58700.00", "TOTAL", "F002"],
        [RUN_ID, "2025-01", "CurrentAssets", "91200.00", "TOTAL", "F002"],
    ]
)


# ===========================================================================
# TABLE 5: METRICS_DB  (5 valid rows + 3 error rows)
# ===========================================================================
# PLANTED ERRORS:
#   Row 4 (M001 duplicate): Same PK as Row 1
#          → Violates SCV-3V-03 (duplicate PK)
#   Row 5 (M020): status=Calculated but value is null
#          → Violates SCV-3V-11 (Calculated must have non-null value)
#   Row 6 (M050): status=NotCalculated but calc_notes is empty
#          → Violates SCV-3V-11 (NotCalculated must explain why)
#   Row 8 (M999): metric_id not in Approved Dictionary
#          → Violates SCV-3V-04 (unknown metric_id)
# ===========================================================================

write_csv("METRICS_DB",
    ["run_id", "metric_id", "metric_name", "period_key", "segment_key",
     "value", "unit", "formula_reference", "required_inputs",
     "constraints", "status", "confidence", "calc_notes", "file_ids_used"],
    [
        [RUN_ID, "M001", "Total Revenue (Net)", "2025-01", "TOTAL",
         "96300.00", "currency",
         "REVENUE_NET = SUM(Revenue) - SUM(Contra-Revenue)",
         "PNL_FACT Revenue bucket", "None", "Calculated", "High",
         "No contra-revenue detected", '["F001"]'],
        [RUN_ID, "M013", "Operating Expenses", "2025-01", "TOTAL",
         "53700.00", "currency", "OPEX = SUM(OpEx)",
         "PNL_FACT OpEx bucket", "None", "Calculated", "High",
         "", '["F001"]'],
        [RUN_ID, "M040", "Cash Balance", "2025-01", "TOTAL",
         "58700.00", "currency", "CASH = BS_FACT(line_item=Cash)",
         "BS_FACT", "None", "Calculated", "High",
         "", '["F002"]'],
        # ERROR: duplicate PK → SCV-3V-03
        [RUN_ID, "M001", "Total Revenue (Net)", "2025-01", "TOTAL",
         "96300.00", "currency",
         "REVENUE_NET = SUM(Revenue)",
         "PNL_FACT Revenue bucket", "None", "Calculated", "High",
         "duplicate row", '["F001"]'],
        # ERROR: Calculated but value is null → SCV-3V-11
        [RUN_ID, "M020", "Labor Pct", "2025-01", "TOTAL",
         "", "percent", "LABOR_TOTAL / M001",
         "Labor categories + M001", "M001 > 0", "Calculated", "High",
         "", '["F001"]'],
        # ERROR: NotCalculated but calc_notes is empty → SCV-3V-11
        [RUN_ID, "M050", "Revenue Concentration", "2025-01", "TOTAL",
         "", "count", "TopCustomer/TotalRevenue",
         "Customer-level detail", "Data available", "NotCalculated", "Low",
         "", '["F001"]'],
        [RUN_ID, "M015", "Operating Margin %", "2025-01", "TOTAL",
         "0.44", "percent", "OM = M014 / M001",
         "M014, M001", "M001 > 0", "Calculated", "High",
         "Healthy margin", '["F001"]'],
        # ERROR: M999 not in approved dictionary → SCV-3V-04
        [RUN_ID, "M999", "Custom Metric", "2025-01", "TOTAL",
         "999.00", "currency", "custom",
         "custom", "None", "Calculated", "Medium",
         "unapproved", '["F001"]'],
    ]
)


# ===========================================================================
# TABLE 6: CLAIM_LEDGER_DB  (3 valid rows + 2 error rows)
# ===========================================================================
# PLANTED ERRORS:
#   Row 3: truth_label=[From Data] but linked_metric_ids=[]
#          → Violates SCV-5V-12 / SCV-8V-20
#   Row 4: linked_metric_rows object missing segment_key
#          → Violates SCV-5V-11 / SCV-8V-19
#   Row 5: linked_metric_ids references M888 (doesn't exist)
#          → Violates SCV-5V-07 / SCV-8V-07 (RI-3)
# ===========================================================================

write_csv("CLAIM_LEDGER_DB",
    ["run_id", "claim_uid", "claim_id", "category", "layer_origin",
     "claim_text", "truth_label", "confidence", "status",
     "linked_metric_ids", "linked_metric_rows", "linked_driver_ids",
     "linked_file_ids", "formula_reference", "claim_notes"],
    [
        [RUN_ID, "C-DEMO-REV-0001",
         "CL-RUN-DEMO-20260315-001-REV-0001", "REV", "L1",
         "Monthly revenue averages $96K", "[From Data]", "High", "ACTIVE",
         '["M001"]',
         '[{"metric_id":"M001","period_key":"2025-01","segment_key":"TOTAL"}]',
         "[]", '["F001"]', "[]", "Direct from PNL_FACT"],
        [RUN_ID, "C-DEMO-OPEX-0001",
         "CL-RUN-DEMO-20260315-001-OPEX-0001", "OPEX", "L2",
         "Operating expenses consume majority of revenue",
         "[Derived]", "High", "ACTIVE",
         '["M013","M015"]',
         '[{"metric_id":"M013","period_key":"2025-01","segment_key":"TOTAL"},{"metric_id":"M015","period_key":"2025-01","segment_key":"TOTAL"}]',
         '["DX-RUN-DEMO-20260315-001-OPEX-001"]', '["F001"]', "[]",
         "Derived from M013 and M015"],
        # ERROR: [From Data] but empty metric links → SCV-5V-12 / SCV-8V-20
        [RUN_ID, "C-DEMO-REV-0002",
         "CL-RUN-DEMO-20260315-001-REV-0002", "REV", "L1",
         "Revenue shows seasonal summer peak",
         "[From Data]", "Medium", "ACTIVE",
         "[]", "[]", "[]", '["F001"]', "[]",
         "INTENTIONAL ERROR: From Data claim with empty metric links"],
        # ERROR: linked_metric_rows missing segment_key → SCV-5V-11 / SCV-8V-19
        [RUN_ID, "C-DEMO-CASH-0001",
         "CL-RUN-DEMO-20260315-001-CASH-0001", "CASH", "L1",
         "Cash position is $58.7K", "[From Data]", "High", "ACTIVE",
         '["M040"]',
         '[{"metric_id":"M040","period_key":"2025-01"}]',
         "[]", '["F002"]', "[]", "From BS_FACT"],
        # ERROR: M888 doesn't exist in METRICS_DB → SCV-5V-07 / SCV-8V-07 (RI-3)
        [RUN_ID, "C-DEMO-MAR-0001",
         "CL-RUN-DEMO-20260315-001-MAR-0001", "MAR", "L2",
         "Margin pressure from coaching staff costs",
         "[Derived]", "Medium", "ACTIVE",
         '["M888"]',
         '[{"metric_id":"M888","period_key":"2025-01","segment_key":"TOTAL"}]',
         "[]", '["F001"]', "[]",
         "References non-existent metric"],
    ]
)


# ===========================================================================
# TABLE 7: DRIVER_DB  (2 valid rows + 1 error row)
# ===========================================================================
# PLANTED ERRORS:
#   Row 3: evidence_claim_ids references CL-...-MAR-9999 (doesn't exist)
#          → Violates SCV-5V-10 / SCV-8V-10 (RI-6)
# ===========================================================================

write_csv("DRIVER_DB",
    ["run_id", "driver_id", "category", "driver_class", "direction",
     "outcome_impacted", "evidence_metric_ids", "evidence_claim_ids",
     "constraints_linked", "confidence", "driver_notes"],
    [
        [RUN_ID, "DX-RUN-DEMO-20260315-001-OPEX-001", "OPEX",
         "Confirmed", "-", "Operating margin",
         '["M013","M015"]',
         '["CL-RUN-DEMO-20260315-001-OPEX-0001"]',
         "[]", "High", "High coaching costs relative to revenue"],
        [RUN_ID, "DX-RUN-DEMO-20260315-001-REV-001", "REV",
         "Likely", "+", "Revenue growth",
         '["M001"]',
         '["CL-RUN-DEMO-20260315-001-REV-0001"]',
         "[]", "Medium", "Camp enrollment trending up"],
        # ERROR: claim_id MAR-9999 doesn't exist → SCV-5V-10 / SCV-8V-10 (RI-6)
        [RUN_ID, "DX-RUN-DEMO-20260315-001-MAR-001", "MAR",
         "Confirmed", "-", "Gross margin",
         '["M012"]',
         '["CL-RUN-DEMO-20260315-001-MAR-9999"]',
         "[]", "High",
         "INTENTIONAL: references non-existent claim_id"],
    ]
)


# ===========================================================================
# TABLE 8: VERIFY_NEXT_DB  (1 valid row + 1 error row)
# ===========================================================================
# PLANTED ERRORS:
#   Row 2: why_it_matters is empty string
#          → Violates SCV-5V-13 / SCV-8V-22 (required non-null)
# ===========================================================================

write_csv("VERIFY_NEXT_DB",
    ["run_id", "vn_id", "category", "uncertainty_or_gap",
     "why_it_matters", "minimum_data_needed", "how_to_obtain",
     "decisions_impacted", "linked_claim_ids",
     "confidence_impact_if_resolved"],
    [
        [RUN_ID, "VN-RUN-DEMO-20260315-001-DATA-001", "DATA",
         "Owner compensation not identified",
         "Cannot produce accurate EBITDA proxy",
         "Owner salary and distribution amounts",
         "Ask owner for comp details",
         "EBITDA and profitability analysis", "[]", "High"],
        # ERROR: why_it_matters is empty → SCV-5V-13 / SCV-8V-22
        [RUN_ID, "VN-RUN-DEMO-20260315-001-CASH-001", "CASH",
         "Cash runway basis unknown", "",
         "Cash flow statement or bank statements",
         "Request from accountant",
         "Cash planning decisions", "[]", "Medium"],
    ]
)


# ===========================================================================
# TABLE 9: RECOMMENDATIONS_DB  (2 valid rows + 2 error rows)
# ===========================================================================
# PLANTED ERRORS:
#   Row 3: linked_claim_ids = [] (empty)
#          → Violates SCV-8V-21 (must be non-empty)
#   Row 4: category = "Odd Category Name"
#          → Triggers SCV-8V-35 WARN (not in recommended set)
# ===========================================================================

write_csv("RECOMMENDATIONS_DB",
    ["run_id", "rec_id", "category", "rec_title", "rec_description",
     "recommendation_label", "linked_claim_ids", "linked_driver_ids",
     "linked_metric_ids", "expected_direction", "expected_magnitude",
     "risks", "preconditions", "measurement_plan", "confidence",
     "scenario_lever"],
    [
        [RUN_ID, "RC-RUN-DEMO-20260315-001-OPEX-001",
         "Cost Control", "Optimize Coaching Staff Schedule",
         "Restructure coaching hours to reduce overtime premium pay",
         "[Data-Supported]",
         '["CL-RUN-DEMO-20260315-001-OPEX-0001"]',
         '["DX-RUN-DEMO-20260315-001-OPEX-001"]',
         '["M020","M015"]', "↑", "4-6 pts margin improvement",
         "Staff retention risk; coverage gaps",
         "Current scheduling system in place",
         "Monitor M020 and M015 monthly for 3 months", "High", ""],
        [RUN_ID, "RC-RUN-DEMO-20260315-001-REV-001",
         "Revenue Growth", "Expand Summer Camp Program",
         "Add two additional camp sessions during peak season",
         "[Logic-Supported]",
         '["CL-RUN-DEMO-20260315-001-REV-0001"]',
         '["DX-RUN-DEMO-20260315-001-REV-001"]',
         '["M001"]', "↑", "$8K-15K/mo additional revenue",
         "Field capacity; weather risk",
         "Facility availability confirmed",
         "Track M001 monthly; compare to prior summer", "Medium", ""],
        # ERROR: empty linked_claim_ids → SCV-8V-21
        [RUN_ID, "RC-RUN-DEMO-20260315-001-MAR-001",
         "Margin Improvement", "Renegotiate Equipment Supplier",
         "Review and renegotiate top equipment supplier contracts",
         "[Assumption-Dependent]",
         "[]", "[]", '["M012"]', "↑", "2-3 pts margin",
         "Supplier pushback", "Contracts up for renewal",
         "Track M012 quarterly", "Low", ""],
        # ERROR: non-standard category → SCV-8V-35 WARN
        [RUN_ID, "RC-RUN-DEMO-20260315-001-CASH-001",
         "Odd Category Name", "Improve Registration Collections",
         "Tighten collection from 45 to 30 days on camp fees",
         "[Data-Supported]",
         '["CL-RUN-DEMO-20260315-001-CASH-0001"]',
         "[]", '["M040"]', "↑", "$12K cash improvement",
         "Family relationship strain",
         "Registration system supports auto-reminders",
         "Monitor M040 and AR aging monthly", "Medium", ""],
    ]
)


# ===========================================================================
# SUMMARY
# ===========================================================================
print()
print("=" * 60)
print("SAMPLE DATA GENERATION COMPLETE")
print("=" * 60)
print()
print("  Client:       Riverside Soccer Academy (FICTIONAL)")
print("  Files created: 9 CSVs in data/")
print(f"  Run ID:        {RUN_ID}")
print("  Layer mode:    L2 (RECOMMENDATIONS_DB present,")
print("                     DELTA_LOG_DB absent)")
print()
print("  Planted errors by SCV check:")
print("    SCV-3V-03  → METRICS_DB duplicate PK")
print("    SCV-3V-04  → METRICS_DB unapproved metric M999")
print("    SCV-3V-09  → PNL_FACT negative amount")
print("    SCV-3V-11  → METRICS_DB Calculated+null, NotCalc+empty notes")
print("    SCV-5V-07  → CLAIM linked_metric_ids refs M888 (RI-3)")
print("    SCV-5V-10  → DRIVER evidence_claim_ids refs MAR-9999 (RI-6)")
print("    SCV-5V-11  → CLAIM linked_metric_rows missing segment_key")
print("    SCV-5V-12  → CLAIM [From Data] with empty metric links")
print("    SCV-5V-13  → VERIFY_NEXT empty why_it_matters")
print("    SCV-5V-14  → PNL_FACT source_file_id F999 (RI-17)")
print("    SCV-8V-21  → REC empty linked_claim_ids")
print("    SCV-8V-35  → REC non-standard category (WARN)")
print()
print("  Total: 12 planted errors across 5 tables")
print("=" * 60)

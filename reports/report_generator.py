"""
reports/report_generator.py
Collects ValidationFinding objects from all stages and writes them
to a VALIDATION_REPORT_DB CSV matching the Table 10 schema.

Also prints a human-readable summary to the terminal.

Usage:
    from reports.report_generator import generate_report
    output_path = generate_report(findings, run_id, output_dir="data")
"""

import csv
import os
from core.registry import get_expected_columns


# Column order matches VALIDATION_REPORT_DB schema (Table 10 in spec)
REPORT_COLUMNS = get_expected_columns("VALIDATION_REPORT_DB")


def generate_report(findings, run_id, output_dir="data"):
    """Write findings to a VALIDATION_REPORT_DB CSV and print summary.

    Args:
        findings:   List of ValidationFinding objects from all stages
        run_id:     The run ID (used in the filename)
        output_dir: Directory to write the CSV into

    Returns:
        Path to the written CSV file, or None if no findings
    """

    # ------------------------------------------------------------------
    # Print terminal summary first
    # ------------------------------------------------------------------
    _print_summary(findings, run_id)

    # ------------------------------------------------------------------
    # If no findings, no CSV needed
    # ------------------------------------------------------------------
    if not findings:
        print("\n  No findings — no VALIDATION_REPORT_DB generated.")
        return None

    # ------------------------------------------------------------------
    # Write CSV
    # ------------------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{run_id}__VALIDATION_REPORT_DB.csv"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        for finding in findings:
            writer.writerow(finding.to_dict())

    print(f"\n  Report written: {filepath}")
    print(f"  Rows: {len(findings)}")

    return filepath


def _print_summary(findings, run_id):
    """Print a human-readable summary of all findings to the terminal."""

    blockers = [f for f in findings if f.severity == "BLOCKER"]
    warns = [f for f in findings if f.severity == "WARN"]

    # Group by stage
    stages = {}
    for f in findings:
        stages.setdefault(f.stage_id, []).append(f)

    print()
    print("=" * 65)
    print(f"  EVS VALIDATION REPORT — {run_id}")
    print("=" * 65)
    print(f"  Total findings:  {len(findings)}")
    print(f"  BLOCKERs:        {len(blockers)}")
    print(f"  WARNs:           {len(warns)}")
    print()

    # Per-stage breakdown
    for stage_id in ["2V", "3V", "5V", "8V"]:
        stage_findings = stages.get(stage_id, [])
        if not stage_findings:
            continue

        stage_blockers = [f for f in stage_findings if f.severity == "BLOCKER"]
        stage_warns = [f for f in stage_findings if f.severity == "WARN"]

        print(f"  Stage {stage_id}: {len(stage_findings)} findings "
              f"({len(stage_blockers)} BLOCKER, {len(stage_warns)} WARN)")

        for f in stage_findings:
            severity_marker = "!!" if f.severity == "BLOCKER" else "  "
            print(f"    {severity_marker} {f.check_id:<12} {f.table_name:<25} "
                  f"{f.column_name}")
        print()

    # Final verdict
    if blockers:
        print("  VERDICT: FAIL — BLOCKERs found. Output must not ship.")
    else:
        print("  VERDICT: PASS — No BLOCKERs. WARNs are advisory.")

    print("=" * 65)

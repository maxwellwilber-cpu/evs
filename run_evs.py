"""
run_evs.py
CLI entry point for the External Validation Script (EVS).

Loads FAS output CSVs, detects layer mode, runs the correct validation
stages, and generates a VALIDATION_REPORT_DB CSV.

Usage:
    /usr/bin/python3 run_evs.py data/
    /usr/bin/python3 run_evs.py path/to/fas/output/

Stage selection (from spec Section 8, Note 3):
    Always:  2V + 3V
    L1:      + 5V
    L2/L3:   + 8V
"""

import sys
from core.loader import load_data_pack
from validators.stage_2v import run_stage_2v
from validators.stage_3v import run_stage_3v
from validators.stage_5v import run_stage_5v
from validators.stage_8v import run_stage_8v
from reports.report_generator import generate_report


def main():
    # ------------------------------------------------------------------
    # Parse CLI argument — folder path
    # ------------------------------------------------------------------
    if len(sys.argv) < 2:
        print("Usage: /usr/bin/python3 run_evs.py <folder_path>")
        print("  folder_path: directory containing FAS output CSVs")
        print("  Example: /usr/bin/python3 run_evs.py data/")
        sys.exit(1)

    folder_path = sys.argv[1]

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    print(f"Loading FAS output from: {folder_path}")
    pack = load_data_pack(folder_path)

    if pack.load_errors:
        print(f"\nLoad errors:")
        for err in pack.load_errors:
            print(f"  - {err}")
        if not pack.tables:
            print("\nNo tables loaded. Exiting.")
            sys.exit(1)

    print(f"  Run ID:     {pack.run_id}")
    print(f"  Layer mode: {pack.layer_mode}")
    print(f"  Tables:     {len(pack.tables)} loaded")

    # ------------------------------------------------------------------
    # Run validation stages based on layer mode
    # Always: 2V + 3V
    # L1:     + 5V
    # L2/L3:  + 8V
    # ------------------------------------------------------------------
    all_findings = []

    print(f"\nRunning Stage 2V (Post-Normalization)...")
    findings_2v = run_stage_2v(pack)
    all_findings.extend(findings_2v)
    print(f"  {len(findings_2v)} finding(s)")

    print(f"Running Stage 3V (Post-Metric Engine)...")
    findings_3v = run_stage_3v(pack)
    all_findings.extend(findings_3v)
    print(f"  {len(findings_3v)} finding(s)")

    if pack.layer_mode == "L1":
        print(f"Running Stage 5V (Pre-Output, L1)...")
        findings_5v = run_stage_5v(pack)
        all_findings.extend(findings_5v)
        print(f"  {len(findings_5v)} finding(s)")
    else:
        print(f"Running Stage 8V (Pre-Output, {pack.layer_mode})...")
        findings_8v = run_stage_8v(pack)
        all_findings.extend(findings_8v)
        print(f"  {len(findings_8v)} finding(s)")

    # ------------------------------------------------------------------
    # Generate report
    # ------------------------------------------------------------------
    output_path = generate_report(all_findings, pack.run_id, output_dir="output")

    # ------------------------------------------------------------------
    # Exit code: 1 if BLOCKERs found, 0 otherwise
    # ------------------------------------------------------------------
    has_blockers = any(f.severity == "BLOCKER" for f in all_findings)
    sys.exit(1 if has_blockers else 0)


if __name__ == "__main__":
    main()

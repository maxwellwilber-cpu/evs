# EVS — External Validation Script

A Python validation framework that tests AI-generated financial analysis output. Built as the coded counterpart to the [FAS (Financial Analysis System)](https://github.com/) — a 20+ node AI pipeline with 63 validation checks across 14 governed tables.

The FAS runs on Vellum's visual platform. The EVS proves the same validation thinking can be implemented in Python: data ingestion, pandas transformation, staged validation, pytest coverage, and formatted output.

**12/12 planted errors caught. 3 additional unplanted data quality issues independently identified.**
**73 validation checks across 4 pipeline stages. 43 pytest tests. 100% detection rate.**

## What It Does

The EVS reads FAS output CSVs, detects the pipeline's layer mode (L1/L2/L3), runs the correct validation stages, and produces a VALIDATION_REPORT_DB identifying every schema violation, referential integrity failure, and business logic breach.

**73 validation checks** across 4 stages, organized by pipeline gate:

| Stage | Name | Checks | What It Validates |
|-------|------|--------|-------------------|
| 2V | Post-Normalization | 9 | Table presence, column schemas, type/enum validation, period coherence |
| 3V | Post-Metric Engine | 11 | PK uniqueness, approved metric IDs, value/status gating, JSON compliance |
| 5V | Pre-Output (L1) | 17 | Layer enforcement, cross-table RI, claim linkage, run_id consistency |
| 8V | Pre-Output (L2/L3) | 36 | Superset of 5V plus recommendations, scenarios, sensitivity, decisions |

**Stage selection follows the spec:**
- Always: 2V + 3V
- L1 runs: + 5V (up to 37 checks)
- L2/L3 runs: + 8V (up to 56 checks)

## Results Against Sample Data

Sample data includes 9 FAS output tables with 12 intentionally planted errors mapped to specific SCV check IDs.

| Stage | Checks Run | Passed | BLOCKERs | WARNs |
|-------|-----------|--------|----------|-------|
| 2V | 9 | 8 | 0 | 1 |
| 3V | 11 | 7 | 5 | 0 |
| 8V | 36 | 23 | 12 | 1 |
| **L2 Total** | **56** | **38** | **17** | **2** |

All 12 planted errors caught. Additional legitimate catches identified (M012 orphan references, partial period overlap).

43 pytest tests verify every planted error is caught by the correct check ID with the correct severity.

## Project Structure

```
evs/
├── run_evs.py                  # CLI entry point
├── requirements.txt            # pandas, pytest
├── generate_sample_data.py     # Creates test CSVs with planted errors
├── data/                       # FAS output CSVs (input to EVS)
├── output/                     # EVS report output
├── core/
│   ├── registry.py             # All schemas, enums, regex, PKs, RI rules
│   ├── types.py                # ValidationFinding dataclass
│   └── loader.py               # CSV ingestion + layer mode detection
├── validators/
│   ├── stage_2v.py             # Post-Normalization (9 checks)
│   ├── stage_3v.py             # Post-Metric Engine (11 checks)
│   ├── stage_5v.py             # Pre-Output L1 (17 checks)
│   └── stage_8v.py             # Pre-Output L2/L3 (36 checks)
├── reports/
│   └── report_generator.py     # Writes VALIDATION_REPORT_DB CSV
└── tests/
    ├── test_stage_2v.py        # 7 tests
    ├── test_stage_3v.py        # 10 tests
    ├── test_stage_5v.py        # 12 tests
    └── test_stage_8v.py        # 14 tests
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Generate sample data (9 CSVs with planted errors)
python generate_sample_data.py

# Run the EVS
python run_evs.py data/

# Run tests
python -m pytest tests/ -v
```

Exit codes: 0 = PASS (no BLOCKERs), 1 = FAIL (BLOCKERs found).

## Architecture

**registry.py** — Single source of truth for all 14 table schemas, 32 enum sets, 15 ID regex patterns, 21 referential integrity rules, and 30 approved metric IDs. Every validator imports from here.

**types.py** — ValidationFinding dataclass that maps 1:1 to the VALIDATION_REPORT_DB schema. Every check returns a list of these. The report generator consumes them without knowing which check produced them.

**loader.py** — Reads CSVs, detects layer mode from table presence (RECOMMENDATIONS_DB absent = L1, present + no DELTA_LOG = L2, DELTA_LOG present = L3), packages everything into a DataPack.

**Validators** — One module per stage. Each check is an independent function. 8V reuses 5V functions for parallel checks (spec Note 7) and remaps findings to 8V check IDs.

**Report generator** — Serializes findings to CSV matching the same schema the FAS pipeline uses internally, so EVS output is directly comparable to FAS output.

## Spec Compliance

- Implements VCR-1.2 (Validation Check Registry version 1.2)
- 73 active checks, 7 retired checks excluded (SCV-2V-10, 3V-08, 3V-12, 3V-13, 3V-15, 8V-28)
- 8V-28a and 8V-28b are active (only 8V-28 is retired)
- Duplicate RI checks (8V-23/30, 8V-24/31) reported under both IDs per spec Note 8
- VALIDATION_REPORT_DB output follows Table 10 schema exactly

## Connection to the FAS

The FAS is a multi-node AI pipeline built on Vellum that performs financial analysis with chained LLM calls, validation gates, retry protocols, and fail-closed behavior. The EVS validates FAS output externally — it reads the same tables the FAS produces and applies the same check logic defined in the FAS spec.

This demonstrates that AI validation logic designed visually can be implemented as production-quality Python with proper separation of concerns, automated testing, and CI-ready architecture. The EVS validates any FAS output run — it is not hardcoded to specific data.

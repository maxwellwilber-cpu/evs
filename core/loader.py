"""
core/loader.py
Reads FAS output CSVs from a folder, detects layer mode, and packages
everything into a DataPack that validators consume.

Usage:
    from core.loader import load_data_pack
    pack = load_data_pack("data/")
    print(pack.run_id)        # "RUN-BCBA-20260301-001"
    print(pack.layer_mode)    # "L2"
    print(pack.tables.keys()) # all loaded table names
    df = pack.tables["METRICS_DB"]  # pandas DataFrame

File naming convention (from spec §8.12):
    <RUN_ID>__<TABLE_NAME>.csv
    Example: RUN-BCBA-20260301-001__METRICS_DB.csv
"""

import os
import re
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# Regex to parse FAS output filenames
# Captures: group(1) = run_id, group(2) = table_name
# ---------------------------------------------------------------------------
FILENAME_PATTERN = re.compile(
    r"^(RUN-[A-Z0-9]+-\d{8}-\d{3})__([A-Z_]+(?:_DB|_FACT)?)\.csv$"
)


@dataclass
class DataPack:
    """Container for all loaded FAS output data.

    Attributes:
        tables:      Dict mapping table_name -> pandas DataFrame
        run_id:      The single run_id found across all files
        layer_mode:  Detected layer: "L1", "L2", or "L3"
        folder_path: The source folder these were loaded from
        load_errors: Any problems encountered during loading
    """
    tables:      Dict[str, pd.DataFrame] = field(default_factory=dict)
    run_id:      Optional[str] = None
    layer_mode:  Optional[str] = None
    folder_path: str = ""
    load_errors: list = field(default_factory=list)

    def has_table(self, table_name):
        """Check if a table was loaded and has at least one row."""
        return (table_name in self.tables
                and not self.tables[table_name].empty)

    def get_table(self, table_name):
        """Return a table's DataFrame, or None if not loaded."""
        return self.tables.get(table_name)


def load_data_pack(folder_path):
    """Load all FAS output CSVs from a folder into a DataPack.

    Steps:
        1. Scan folder for files matching the naming convention
        2. Load each CSV into a pandas DataFrame (all values as strings)
        3. Validate all files share the same run_id
        4. Detect layer mode from which tables are present
        5. Return a fully populated DataPack

    Args:
        folder_path: Path to directory containing the CSV files

    Returns:
        DataPack with all tables loaded
    """
    pack = DataPack(folder_path=folder_path)

    # ------------------------------------------------------------------
    # Step 1: Verify the folder exists
    # ------------------------------------------------------------------
    if not os.path.isdir(folder_path):
        pack.load_errors.append(f"Folder not found: {folder_path}")
        return pack

    # ------------------------------------------------------------------
    # Step 2: Scan for matching CSV files
    # ------------------------------------------------------------------
    run_ids_found = set()

    for filename in sorted(os.listdir(folder_path)):
        match = FILENAME_PATTERN.match(filename)
        if not match:
            # Skip non-matching files (e.g., .DS_Store, README, etc.)
            continue

        run_id = match.group(1)
        table_name = match.group(2)
        run_ids_found.add(run_id)
        filepath = os.path.join(folder_path, filename)

        # ----------------------------------------------------------
        # Step 3: Load CSV into DataFrame
        # Read everything as strings to preserve original values.
        # Validators will handle type checking themselves.
        # keep_default_na=False prevents pandas from converting
        # empty strings to NaN — we need to distinguish "" from null.
        # ----------------------------------------------------------
        try:
            df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
            pack.tables[table_name] = df
        except Exception as e:
            pack.load_errors.append(
                f"Failed to read {filename}: {str(e)}"
            )

    # ------------------------------------------------------------------
    # Step 4: Validate run_id consistency
    # ------------------------------------------------------------------
    if len(run_ids_found) == 0:
        pack.load_errors.append(
            f"No FAS output files found in {folder_path}. "
            f"Expected files matching: RUN-*__TABLE_NAME.csv"
        )
    elif len(run_ids_found) == 1:
        pack.run_id = run_ids_found.pop()
    else:
        # Multiple run_ids found — this is a problem but we still load
        pack.run_id = sorted(run_ids_found)[0]
        pack.load_errors.append(
            f"Multiple run_ids found in filenames: {sorted(run_ids_found)}. "
            f"Using {pack.run_id}. All files should share one run_id."
        )

    # ------------------------------------------------------------------
    # Step 5: Detect layer mode
    # From spec Section 8, Note 2:
    #   RECOMMENDATIONS_DB absent              → L1
    #   RECOMMENDATIONS_DB present + no DELTA  → L2
    #   DELTA_LOG_DB present                   → L3
    # ------------------------------------------------------------------
    pack.layer_mode = _detect_layer_mode(pack)

    return pack


def _detect_layer_mode(pack):
    """Infer L1/L2/L3 from which tables are present.

    Logic (from spec Section 8 Note 2):
        - If RECOMMENDATIONS_DB is absent → L1_DIAGNOSTIC
        - If RECOMMENDATIONS_DB present but DELTA_LOG_DB absent → L2_DECISION
        - If DELTA_LOG_DB present → L3_ONGOING
    """
    has_recs = "RECOMMENDATIONS_DB" in pack.tables
    has_delta = "DELTA_LOG_DB" in pack.tables

    if has_delta:
        return "L3"
    elif has_recs:
        return "L2"
    else:
        return "L1"

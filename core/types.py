"""
core/types.py
Data classes used across all EVS modules.

The ValidationFinding dataclass is the universal return type for every
SCV check. Its fields map 1:1 to the VALIDATION_REPORT_DB schema
(Table 10 in the spec), so findings convert directly to report rows.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ValidationFinding:
    """A single validation finding — one per failed check per table.

    Every validator function returns a list of these. The report generator
    collects them all and writes them out as VALIDATION_REPORT_DB rows.

    Fields (from spec Table 10):
        run_id:              The run being validated
        stage_id:            Which gate found this (2V, 3V, 5V, 8V)
        check_id:            The SCV check ID (e.g., "SCV-3V-03")
        severity:            BLOCKER or WARN
        table_name:          Which table the finding applies to
        column_name:         Which column failed, or "*" if table-level
        fail_count:          How many rows/items failed this check
        sample_failing_rows: Up to 3 example failures (list of dicts)
        message:             Human-readable description of what failed
        remediation_hint:    Suggested fix
        blocker_class:       GENERATION_ERROR, INTEGRITY_ERROR, or DATA_ERROR
                             (required if BLOCKER, None if WARN)
    """
    run_id:              str
    stage_id:            str
    check_id:            str
    severity:            str
    table_name:          str
    column_name:         str
    fail_count:          int
    sample_failing_rows: list = field(default_factory=list)
    message:             str = ""
    remediation_hint:    str = ""
    blocker_class:       Optional[str] = None

    def to_dict(self):
        """Convert to a dict matching VALIDATION_REPORT_DB column order."""
        import json
        return {
            "run_id":              self.run_id,
            "stage_id":            self.stage_id,
            "check_id":            self.check_id,
            "severity":            self.severity,
            "table_name":          self.table_name,
            "column_name":         self.column_name,
            "fail_count":          self.fail_count,
            "sample_failing_rows": json.dumps(self.sample_failing_rows),
            "message":             self.message,
            "remediation_hint":    self.remediation_hint,
            "blocker_class":       self.blocker_class or "",
        }

# Ralph Progress Log

## Iteration 127 - 2026-02-08
- added field completeness metrics (non-empty counts + percentages) in text and JSON reports
- tracked total row counts and non-empty counts per column during roster parsing
- extended tests to cover field completeness counts

## Iteration 33 - 2026-02-08
- added summary-only mode for text reports
- added status.csv export with per-key change classification
- added lightweight test script for summary-only and status export

## Iteration 113 - 2026-02-08
- added missing key field counts to text/JSON reports for invalid rows
- introduced MainTest.java with CSV parsing and missing key coverage tests
- documented max-detail usage and test commands in README

## Iteration 112 - 2026-02-08
- added --max-detail to cap added/removed/updated detail in text and JSON outputs
- included JSON detail metadata for truncation visibility
- added smoke test script for max-detail behavior

## Iteration 100 - 2026-02-08
- added composite key support for multi-column matching
- added optional updated_rows.csv export with full before/after fields
- updated JSON output and README to document key columns + export option

## Iteration 111 - 2026-02-08
- added change rate metrics (net change and added/removed/updated/unchanged percentages) to text + JSON reports
- documented new change rate metrics in README

## Iteration 110 - 2026-02-08
- added key normalization option (none/lower/upper) for case-insensitive roster matching
- surfaced key normalization setting in text/JSON reports and README usage

## Iteration 45 - 2026-02-08
- added --ignore support with ignored/unknown field reporting in text + JSON reports
- updated README with ignore usage guidance

## Iteration 3 - 2026-02-07
- started groupscholar-roster-reconciler (Java CLI)
- implemented CSV parsing, roster diffing, and summary + change reporting
- added JSON report output option and sample roster data
- documented usage and features in README

## Iteration 63 - 2026-02-08
- added key-column validation plus column change reporting in text/JSON outputs
- improved ignore-field handling across mismatched headers and JSON metadata

## Iteration 54 - 2026-02-08
- added field change counts plus duplicate key + invalid row detail to text and JSON reports
- extended roster parsing to retain duplicate key values and invalid row numbers for diagnostics
- updated README with new report details

## Iteration 55 - 2026-02-08
- added CSV export bundle for added/removed/updated rows with optional unchanged export
- implemented CSV writer with proper escaping and deterministic ordering
- documented export options in README

# Ralph Progress Log

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

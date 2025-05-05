# Group Scholar Roster Reconciler

Group Scholar Roster Reconciler is a local-first CLI that compares two roster CSVs and produces a clean, actionable change report (adds, removals, updates, duplicates, and invalid rows). It helps ops teams quickly understand what changed between two roster snapshots and what actions to take.

## Features
- Diff two roster CSVs by a stable key (default: `email`).
- Summaries for additions, removals, updates, unchanged rows, and duplicates.
- Field-level change detail for updated records.
- Field change counts plus duplicate key and invalid row diagnostics.
- Column-level change reporting (added/removed columns).
- Change rate metrics (net change and added/removed/updated percentages).
- CSV parsing with quoted field support.
- Optional JSON report output for downstream workflows.
- Optional CSV export bundle for added/removed/updated (and unchanged) rows.
- Optional key normalization (lower/upper) to handle case mismatches.
- Optional value normalization (trim/collapse) to ignore whitespace-only changes.

## Usage

Compile:

```bash
javac -d out src/Main.java
```

Run:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email
```

Normalize key values (case-insensitive matching):

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email --key-normalize lower
```

Normalize field values to ignore whitespace-only changes:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email --value-normalize trim
```

Ignore fields when diffing:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email --ignore last_login,notes
```

Optional JSON output:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email --json report.json
```

Optional CSV export bundle:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email --export-dir out/exports
```

Include unchanged rows:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email --export-dir out/exports --export-unchanged
```

Export files written to `--export-dir`:
- `added.csv` (rows from current)
- `removed.csv` (rows from previous)
- `updated.csv` (key + field + before + after)
- `unchanged.csv` (only if `--export-unchanged` is set)

## Input Expectations
- Both CSVs should have a header row.
- The key column (default `email`) must be present in both files.
- If a row has a missing key value, it is counted as invalid.
- Use `--key-normalize lower|upper` to avoid case-only mismatches in keys.
- Use `--value-normalize trim|collapse` to ignore whitespace-only changes.
- Use `--ignore` to skip volatile fields (e.g., `last_login`) in the diff.

## Example Output (Summary)
```
Roster Reconciler Report
Previous: data/roster_prev.csv
Current: data/roster_current.csv
Key: email
Key Normalize: none
Timestamp: 2026-02-07T18:30:00

Summary:
- total_previous: 3
- total_current: 4
- added: 1
- removed: 0
- updated: 1
- unchanged: 2
- duplicate_keys_previous: 0
- duplicate_keys_current: 0
- invalid_rows_previous: 0
- invalid_rows_current: 0
- net_change: 1
- net_change_pct_previous: 33.33%
- added_pct_current: 25.00%
- removed_pct_previous: 0.00%
- updated_pct_shared: 33.33%
- unchanged_pct_shared: 66.67%

Field Change Counts:
  - status: 1
  - cohort: 1
```

## Technologies
- Java 17 (no external dependencies)

## Project Status
Active.

# Group Scholar Roster Reconciler

Group Scholar Roster Reconciler is a local-first CLI that compares two roster CSVs and produces a clean, actionable change report (adds, removals, updates, duplicates, and invalid rows). It helps ops teams quickly understand what changed between two roster snapshots and what actions to take.

## Features
- Diff two roster CSVs by a stable key (default: `email`).
- Summaries for additions, removals, updates, unchanged rows, and duplicates.
- Field-level change detail for updated records.
- CSV parsing with quoted field support.
- Optional JSON report output for downstream workflows.

## Usage

Compile:

```bash
javac -d out src/Main.java
```

Run:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email
```

Optional JSON output:

```bash
java -cp out Main --previous data/roster_prev.csv --current data/roster_current.csv --key email --json report.json
```

## Input Expectations
- Both CSVs should have a header row.
- The key column (default `email`) must be present in both files.
- If a row has a missing key value, it is counted as invalid.

## Example Output (Summary)
```
Roster Reconciler Report
Previous: data/roster_prev.csv
Current: data/roster_current.csv
Key: email
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
```

## Technologies
- Java 17 (no external dependencies)

## Project Status
Active.

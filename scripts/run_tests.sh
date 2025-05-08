#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

rm -rf out-test out/exports-test
mkdir -p out-test

javac -d out-test src/Main.java

summary_output=$(java -cp out-test Main --previous data/roster_prev.csv --current data/roster_current.csv --summary-only)

echo "$summary_output" | rg -q "Summary:"
if echo "$summary_output" | rg -q "Added \("; then
  echo "Summary-only output contains detail sections." >&2
  exit 1
fi

EXPORT_DIR="out/exports-test"
java -cp out-test Main --previous data/roster_prev.csv --current data/roster_current.csv --export-dir "$EXPORT_DIR" --export-status >/dev/null

if [[ ! -f "$EXPORT_DIR/status.csv" ]]; then
  echo "Missing status.csv export." >&2
  exit 1
fi

rg -q "darius.king@example.org,updated,region" "$EXPORT_DIR/status.csv"

echo "All tests passed."

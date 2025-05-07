#!/bin/sh
set -euo pipefail

cd "$(dirname "$0")/.."

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

cat > "$workdir/prev.csv" <<'CSV'
email,name
alex.one@example.org,Alex One
blair.two@example.org,Blair Two
casey.three@example.org,Casey Three
CSV

cat > "$workdir/cur.csv" <<'CSV'
email,name
alex.one@example.org,Alex One Updated
blair.two@example.org,Blair Two Updated
devon.four@example.org,Devon Four
ellen.five@example.org,Ellen Five
CSV

javac -d out src/Main.java

output=$(java -cp out Main --previous "$workdir/prev.csv" --current "$workdir/cur.csv" --key email --max-detail 1)

echo "$output" | grep -q "Added (2):"
echo "$output" | grep -q "Updated (2):"
echo "$output" | grep -q "showing 1 of 2"

echo "ok"

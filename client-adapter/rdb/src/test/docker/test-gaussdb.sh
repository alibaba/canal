#!/usr/bin/env bash
set -euo pipefail

test_dir="$(cd "$(dirname "$0")" && pwd)"
project="canal-gaussdb-test-$$"
compose=(docker compose -p "$project" -f "$test_dir/compose.yml")
cleanup() {
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm tests

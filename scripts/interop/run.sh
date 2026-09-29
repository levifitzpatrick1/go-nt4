#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
PYTHON=${NT4_INTEROP_PYTHON:-/home/levi/.local/state/go-nt4-rewrite/20260925-resume/ntcore-venv/bin/python}
if [[ ! -x "$PYTHON" ]]; then echo 'missing pinned external ntcore virtualenv; set NT4_INTEROP_PYTHON' >&2; exit 1; fi
"$PYTHON" -c 'import ntcore; assert ntcore.__version__ == "2026.2.2"'
export NT4_INTEROP_PYTHON="$PYTHON"
go test -tags interop -count=1 -timeout=180s -run '^TestInterop' ./...

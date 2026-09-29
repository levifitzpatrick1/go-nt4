#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${GONT4_HOST_DURATION:=300s}"
: "${GONT4_HOST_IDLE_DURATION:=30s}"
: "${GONT4_VALIDATION_OUTPUT_DIR:?set external GONT4_VALIDATION_OUTPUT_DIR}"
case "$GONT4_VALIDATION_OUTPUT_DIR" in /*) ;; *) echo 'output directory must be absolute and external to repository' >&2; exit 2 ;; esac
mkdir -p "$GONT4_VALIDATION_OUTPUT_DIR"
GONT4_VALIDATION_OUTPUT_DIR="$(cd "$GONT4_VALIDATION_OUTPUT_DIR" && pwd -P)"
case "$GONT4_VALIDATION_OUTPUT_DIR" in "$PWD"|"$PWD"/*) echo 'output directory must be external to repository' >&2; exit 2 ;; esac
export GONT4_HOST_DURATION GONT4_HOST_IDLE_DURATION GONT4_VALIDATION_OUTPUT_DIR GONT4_HOST_VALIDATE=1
printf 'UTC=%s\n' "$(date -u +%FT%TZ)" > "$GONT4_VALIDATION_OUTPUT_DIR/environment.txt"
uname -a >> "$GONT4_VALIDATION_OUTPUT_DIR/environment.txt"
go version >> "$GONT4_VALIDATION_OUTPUT_DIR/environment.txt"
if command -v lscpu >/dev/null; then lscpu >> "$GONT4_VALIDATION_OUTPUT_DIR/environment.txt"; fi
printf 'duration=%s idle=%s\n' "$GONT4_HOST_DURATION" "$GONT4_HOST_IDLE_DURATION" >> "$GONT4_VALIDATION_OUTPUT_DIR/environment.txt"
# Pipe keeps output in external artifact directory; pipefail propagates test failure.
go test -count=1 -run '^TestHostValidationCombined$' -timeout=0 -v . 2>&1 | tee "$GONT4_VALIDATION_OUTPUT_DIR/test.log"

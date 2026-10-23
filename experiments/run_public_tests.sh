#!/usr/bin/env bash
# Reproduce the public experiments used in the report.
#
#   T1-T4 exactly as specified (3 independent runs each, median reported by run_demo.py),
#   plus the before/after comparison: the same stress tests with LIVERECALL_PROFILE=basic
#   (conventional per-client FIFO queues, no semantic overload handling).
#
# Usage (from the repo root, venv active):
#   ./experiments/run_public_tests.sh            # everything (~30 min)
#   ./experiments/run_public_tests.sh t3 t3_basic # just some
#
# Results: results/<os>/<test>_runNN.{csv,json} and <test>.log (summary + medians).
# The measurement code (run_demo.py, load_clients.py, analyse_results.py) is unmodified.
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PY:-python}
OUT=${OUT:-results/$(uname -s | tr '[:upper:]' '[:lower:]')}
mkdir -p "$OUT"

params() {   # (bash 3.2 compatible: no associative arrays on macOS)
  case $1 in
    t1)       echo "--clients 20 --duration 20" ;;
    t2)       echo "--clients 100 --duration 40" ;;
    t3)       echo "--clients 100 --duration 60 --slow-percent 10 --slow-ms 500" ;;
    t4)       echo "--clients 100 --duration 60 --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5" ;;
    # Extra stress beyond the public tests (same harness, harsher parameters).
    t3_harsh) echo "--clients 100 --duration 60 --slow-percent 20 --slow-ms 1000" ;;
    late)     echo "--clients 100 --duration 40 --late-percent 20 --late-seconds 15" ;;
    *)        echo "unknown test $1" >&2; exit 2 ;;
  esac
}
ORDER=(t1 t2 t3 t4 t3_harsh late t3_basic t4_basic t3_harsh_basic)

run() {
  local name=$1 base=${1%_basic} profile=full
  [[ $name == *_basic ]] && profile=basic
  local args; args=$(params "$base")
  echo "=== $name (profile=$profile): $args"
  LIVERECALL_PROFILE=$profile "$PY" run_demo.py --runs 3 --quiet-server \
      --output-stem "$OUT/$name" $args 2>&1 \
    | grep -vE '^(client|source) ' | tee "$OUT/$name.log"
}

if [[ $# -gt 0 ]]; then ORDER=("$@"); fi
for t in "${ORDER[@]}"; do run "$t"; done
echo "Done. Summaries in $OUT/*.log"

#!/usr/bin/env bash
# T5 - network impairment experiments (Ubuntu only; needs sudo for tc).
#
# Runs the T2 workload (100 clients, 40 s, 3 independent runs) under each public
# tc/netem profile on the loopback interface, plus an overloaded-bandwidth case
# (where forwarding every revision to every client is impossible) for both the
# full design and the "basic" FIFO profile. The interface is ALWAYS reset on exit.
#
# Usage (as a normal user, from the repo root with the venv active):
#   ./experiments/run_netem_tests.sh                 # all profiles
#   ./experiments/run_netem_tests.sh delay_loss      # one profile
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PY:-python}
IFACE=${IFACE:-lo}
OUT=${OUT:-results/$(uname -s | tr '[:upper:]' '[:lower:]')}
mkdir -p "$OUT"

reset_iface() { sudo ./network_profiles/reset.sh "$IFACE" >/dev/null 2>&1 || true; }
trap reset_iface EXIT INT TERM

apply() {
  case $1 in
    baseline)       reset_iface ;;
    delay_100ms)    sudo ./network_profiles/delay_100ms.sh "$IFACE" ;;
    delay_loss)     sudo ./network_profiles/delay_loss.sh "$IFACE" ;;
    loss_3pct)      sudo ./network_profiles/loss_3pct.sh "$IFACE" ;;
    bandwidth_2mbit) sudo ./network_profiles/bandwidth_2mbit.sh "$IFACE" ;;
    # Overload: ~100 clients x ~4 updates/s x ~150 B needs ~0.5 Mbit/s; give it 250 kbit/s.
    overload_250kbit|overload_250kbit_basic)
      reset_iface
      sudo tc qdisc add dev "$IFACE" root tbf rate 250kbit burst 16kbit latency 400ms
      echo "Applied 250 kbit/s rate limit on $IFACE" ;;
    *) echo "unknown profile $1" >&2; exit 2 ;;
  esac
}

run() {
  local name=$1 profile=full
  [[ $name == *_basic ]] && profile=basic
  echo "=== t5_$name (server profile=$profile)"
  apply "$name"
  LIVERECALL_PROFILE=$profile "$PY" run_demo.py --runs 3 --quiet-server \
      --clients 100 --duration 40 --output-stem "$OUT/t5_$name" 2>&1 \
    | grep -vE '^(client|source) ' | tee "$OUT/t5_$name.log"
  reset_iface
}

ORDER=(baseline delay_100ms delay_loss loss_3pct bandwidth_2mbit overload_250kbit overload_250kbit_basic)
if [[ $# -gt 0 ]]; then ORDER=("$@"); fi
for p in "${ORDER[@]}"; do run "$p"; done
echo "Done. Interface $IFACE reset. Summaries in $OUT/t5_*.log"

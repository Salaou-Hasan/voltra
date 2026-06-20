#!/usr/bin/env bash
# 30K-CCU client fleet driver. Run on each CLIENT machine (NOT the server box).
# Splits TOTAL players into PER_PROC-sized processes with correct --id-offset
# (offsets must be unique across every process on every machine — see below),
# so players never collide. The server runs separately:
#
#   server box:   neondb-sim serve --ws-port 3777 --metrics-port 3778
#
# Usage:
#   SERVER=ws://10.0.0.1:3777 METRICS=http://10.0.0.1:3778 \
#   BASE_OFFSET=0 TOTAL=10000 PER_PROC=5000 DUR=120 ./run_clients.sh
#
# Multi-machine: give each machine a distinct BASE_OFFSET (box1=0, box2=10000,
# box3=20000) so the global player id space stays disjoint.
set -euo pipefail

SERVER="${SERVER:-ws://127.0.0.1:3777}"
METRICS="${METRICS:-http://127.0.0.1:3778}"
TOTAL="${TOTAL:-10000}"
PER_PROC="${PER_PROC:-5000}"
DUR="${DUR:-120}"
BASE_OFFSET="${BASE_OFFSET:-0}"
BIN="${BIN:-./target/release/neondb-sim}"

pids=()
for (( off=0; off<TOTAL; off+=PER_PROC )); do
  n=$(( TOTAL-off < PER_PROC ? TOTAL-off : PER_PROC ))
  "$BIN" --external --url "$SERVER" --metrics-url "$METRICS" \
    --id-offset $(( BASE_OFFSET+off )) --think-ms 200 \
    game --players "$n" --duration "$DUR" --ramp 10 &
  pids+=($!)
done
echo "launched ${#pids[@]} client procs: $TOTAL players, offsets $BASE_OFFSET..$(( BASE_OFFSET+TOTAL-PER_PROC ))"
fail=0
for p in "${pids[@]}"; do wait "$p" || fail=1; done
exit $fail

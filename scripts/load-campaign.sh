#!/usr/bin/env bash
# The Phase 2 load / chaos campaign: every scenario, one after another, on
# the dedicated test Redis (reset between runs). Writes one verdict line per
# run to $OUT/summary.tsv and each run's console output to $OUT/<name>.log;
# the full reports land in <target dir>/oxn-reports/load/.
#
#   scripts/load-campaign.sh             # everything (~45 min)
#   SOAK=2h scripts/load-campaign.sh     # the full-length soak instead of 15m
#   ONLY=throughput scripts/load-campaign.sh
set -uo pipefail

cd "$(dirname "$0")/.."
R="${REDIS:-redis://127.0.0.1:6390}"
OUT="${OUT:-${TMPDIR:-/tmp}/oxn-campaign-$(date +%Y%m%d-%H%M%S)}"
SOAK="${SOAK:-15m}"
mkdir -p "$OUT"
cargo build -p oxn-load --release -q
L="$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')/release/oxn-load"
printf "name\tverdict\tseconds\tsummary\n" > "$OUT/summary.tsv"

run() {
  local name="$1"; shift
  if [ -n "${ONLY:-}" ] && [[ "$name" != *"$ONLY"* ]]; then return; fi
  if [ "${PERSIST:-0}" = "1" ]; then
    OXN_REDIS_PERSIST=1 scripts/test-redis.sh reset >/dev/null
  else
    scripts/test-redis.sh reset >/dev/null
  fi
  echo "▶ $name"
  local t0=$SECONDS
  "$L" "$@" > "$OUT/$name.log" 2>&1
  local rc=$? dt=$((SECONDS - t0))
  local line
  line=$(grep -E "^(PASS|FAIL) " "$OUT/$name.log" | tail -1)
  [ -z "$line" ] && line="(no verdict, exit $rc)"
  printf "%s\t%s\t%s\t%s\n" "$name" "$([ $rc -eq 0 ] && echo PASS || echo FAIL)" "$dt" "$line" >> "$OUT/summary.tsv"
  echo "  $line (${dt}s)"
  grep -E "^  - " "$OUT/$name.log" | head -5
}

# The 0.2.2 baseline's commands, unchanged.
run smoke-last1000   run --redis "$R" --scenario smoke --jobs 5000 --retention last1000 --label p2-last1000
run smoke-remove     run --redis "$R" --scenario smoke --jobs 5000 --retention remove --label p2-remove
run mixed            run --redis "$R" --scenario mixed --label p2
run latency          run --redis "$R" --scenario latency --duration 40s --label p2
run shutdown         run --redis "$R" --scenario shutdown --label p2
run server-chaos     run --redis "$R" --scenario smoke --jobs 20000 --rate 2000 \
  --chaos "3s:script-flush,5s:client-kill=0.5,7s:script-flush,8s:client-kill=1.0" --label p2-server-chaos
run kill-worker      run --redis "$R" --scenario kill-worker --label p2
run network          run --redis "$R" --scenario network --label p2
run retry-storm      run --redis "$R" --scenario retry-storm --label p2
PERSIST=1 run redis-restart run --redis "$R" --scenario redis-restart \
  --redis-restart-cmd "OXN_REDIS_PERSIST=1 scripts/test-redis.sh restart" --label p2

# Phase 2 additions.
run throughput       run --redis "$R" --scenario throughput --label p2
run throughput-procs run --redis "$R" --scenario throughput --jobs 300000 --processes true --label p2-procs
run graceful-shutdown run --redis "$R" --scenario shutdown --jobs 20000 --work-us 200000 \
  --shutdown-timeout-ms 10000 --label p2-graceful
run flow-10k         flow --redis "$R" --flows 10000 --children 10 --fail-parent-rate 0.05 \
  --ignore-rate 0.05 --retry-rate 0.05 --dynamic-rate 0.1 --label p2-10k
run flow-chaos       flow --redis "$R" --flows 3000 --children 8 --work-us 20000 --fail-parent-rate 0.05 \
  --ignore-rate 0.05 --retry-rate 0.05 --dynamic-rate 0.15 --restart-every-s 3 --client-kill-every-s 4 \
  --timeout-s 240 --label p2-chaos
run schedule         schedule --redis "$R" --schedulers 50 --instances 10 --every-ms 200 --duration-s 20 \
  --restart-every-s 3 --client-kill-every-s 4 --label p2
run soak             run --redis "$R" --scenario soak --duration "$SOAK" --rate 1500 --label "p2-$SOAK"

scripts/test-redis.sh reset >/dev/null
echo
column -t -s $'\t' "$OUT/summary.tsv" | cut -c1-200
echo "logs: $OUT"

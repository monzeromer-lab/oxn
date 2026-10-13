#!/usr/bin/env bash
# Dedicated, throwaway Redis for oxn's test, load and chaos suites.
#
# Never point the load/chaos harness at a Redis other software depends on:
# it floods the server, runs SCRIPT FLUSH / CLIENT KILL and restarts it.
#
#   scripts/test-redis.sh start     # port 6390 (override with OXN_REDIS_PORT)
#   scripts/test-redis.sh stop
#   scripts/test-redis.sh restart   # used by the chaos harness
#   scripts/test-redis.sh status
#
# Then: export TEST_REDIS_URL=redis://127.0.0.1:6390
set -euo pipefail

PORT="${OXN_REDIS_PORT:-6390}"
DIR="${OXN_REDIS_DIR:-${TMPDIR:-/tmp}/oxn-redis-$PORT}"
mkdir -p "$DIR"

start() {
  if redis-cli -p "$PORT" ping >/dev/null 2>&1; then
    echo "oxn test redis already running on :$PORT"
    return
  fi
  redis-server --port "$PORT" --bind 127.0.0.1 --save "" --appendonly no \
    --dir "$DIR" --logfile "$DIR/redis.log" --pidfile "$DIR/redis.pid" \
    --daemonize yes --maxclients 20000 --latency-monitor-threshold 25
  for _ in $(seq 50); do
    redis-cli -p "$PORT" ping >/dev/null 2>&1 && { echo "oxn test redis up on :$PORT (dir $DIR)"; return; }
    sleep 0.1
  done
  echo "redis did not come up; see $DIR/redis.log" >&2
  exit 1
}

stop() {
  redis-cli -p "$PORT" shutdown nosave >/dev/null 2>&1 || true
  for _ in $(seq 50); do
    redis-cli -p "$PORT" ping >/dev/null 2>&1 || { echo "oxn test redis on :$PORT stopped"; return; }
    sleep 0.1
  done
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) redis-cli -p "$PORT" info server 2>/dev/null | grep -E "redis_version|tcp_port|uptime_in_seconds" || echo "not running on :$PORT" ;;
  *) echo "usage: $0 {start|stop|restart|status}" >&2; exit 2 ;;
esac

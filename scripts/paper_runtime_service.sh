#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$BASE_DIR"

LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

VENV_PY="$BASE_DIR/.venv/bin/python3"
PID_FILE="$LOG_DIR/paper_runtime.pid"
OUT_FILE="$LOG_DIR/paper_runtime.out"
PATTERN="app\.domains\.trading\.paper_runtime_daemon"

read_pid() {
  if [ -f "$PID_FILE" ]; then
    cat "$PID_FILE" 2>/dev/null || true
  fi
}

is_running() {
  local pid="$1"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

start() {
  local pid
  pid="$(read_pid)"
  if is_running "$pid"; then
    echo "paper runtime already running (pid $pid)"
    return 0
  fi
  if [ -f .env ]; then
    set -a
    source .env
    set +a
  fi
  nohup env PYTHONPATH=. "$VENV_PY" -u -m app.domains.trading.paper_runtime_daemon >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  echo "paper runtime started (pid $(cat "$PID_FILE"))"
}

stop() {
  local pid
  pid="$(read_pid)"
  if is_running "$pid"; then
    kill -TERM "$pid" 2>/dev/null || true
  else
    pkill -f "$PATTERN" || true
  fi
  rm -f "$PID_FILE"
  echo "paper runtime stopped"
}

status() {
  local pid
  pid="$(read_pid)"
  if is_running "$pid"; then
    echo "paper runtime is running (pid $pid)"
    return 0
  fi
  if pgrep -f "$PATTERN" >/dev/null 2>&1; then
    echo "paper runtime is running (detected by pattern)"
    return 0
  fi
  echo "paper runtime is stopped"
  return 1
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop || true; start ;;
  status) status ;;
  *) echo "Usage: $0 {start|stop|restart|status}"; exit 1 ;;
esac
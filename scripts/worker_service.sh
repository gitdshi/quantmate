#!/usr/bin/env bash
set -euo pipefail

# Start/stop/restart worker (RQ) using project's .venv
# Logs: quantmate/logs/worker.out, PID: quantmate/logs/worker.pid

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
VENV_PY="$BASE_DIR/.venv/bin/python3"
PID_FILE="$LOG_DIR/worker.pid"
OUT_FILE="$LOG_DIR/worker.out"

WORKER_PATTERN="app.worker.service.run_worker"
DEFAULT_QUEUES=(backtest optimization default)

read_pid() {
  if [ -f "$PID_FILE" ]; then
    cat "$PID_FILE" 2>/dev/null || true
  fi
}

pid_is_running() {
  local pid="$1"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

list_matching_pids() {
  ps ax -o pid= -o command= 2>/dev/null | awk -v pattern="$WORKER_PATTERN" 'index($0, pattern) { print $1 }' || true
}

resolve_python() {
  if [ -s "$VENV_PY" ] && "$VENV_PY" -V >/dev/null 2>&1; then
    echo "$VENV_PY"
    return 0
  fi

  echo "Error: QuantMate Python runtime is unavailable: $VENV_PY" >&2
  if [ -f "$BASE_DIR/.venv/pyvenv.cfg" ]; then
    echo "Detected a broken or migrated virtualenv in $BASE_DIR/.venv" >&2
    echo "Current pyvenv.cfg:" >&2
    sed 's/^/  /' "$BASE_DIR/.venv/pyvenv.cfg" >&2
  fi
  echo "Recreate the virtualenv with Python 3.12 and reinstall backend dependencies." >&2
  return 1
}

stop() {
  echo "Stopping worker..."
  pid="$(read_pid)"
  if pid_is_running "$pid"; then
    kill -TERM "$pid" 2>/dev/null || true
  fi

  for i in {1..15}; do
    if pid_is_running "$pid"; then
      sleep 1
    else
      rm -f "$PID_FILE"
      echo "Worker stopped"
      return 0
    fi
  done
  echo "Warning: Worker did not stop within timeout" >&2
  return 1
}

start() {
  stop || true
  echo "Starting worker..."
  local python_bin
  python_bin="$(resolve_python)" || return 1
  # Load environment variables from .env if it exists
  if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment variables from .env"
  fi
  # allow passing queues: ./worker_service.sh start backtest optimization
  if [ "$#" -gt 1 ]; then
    shift
    QUEUES=("$@")
  else
    QUEUES=("${DEFAULT_QUEUES[@]}")
  fi
  # Start with provided queues
  nohup "$python_bin" -u -m app.worker.service.run_worker "${QUEUES[@]}" >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 1
  pid="$(read_pid)"
  if pid_is_running "$pid"; then
    echo "Worker started (pid $pid) queues: ${QUEUES[*]}"
    return 0
  fi

  echo "Error: Worker failed to start. Check $OUT_FILE" >&2
  tail -n 20 "$OUT_FILE" >&2 || true
  return 1
}

status() {
  local pid
  pid="$(read_pid)"
  if pid_is_running "$pid"; then
    echo "Worker: running (pid $pid)"
    return 0
  fi

  local matched_pids
  matched_pids="$(list_matching_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  if [ -n "$matched_pids" ]; then
    echo "Worker: running (pid $matched_pids)"
    return 0
  fi

  echo "Worker: stopped"
}

case "${1-}" in
  start)
    start "$@"
    ;;
  stop)
    stop
    ;;
  restart)
    stop && start
    ;;
  status)
    status
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status} [queues...]"
    exit 2
    ;;
esac

#!/usr/bin/env bash
set -euo pipefail

# Start/stop/restart data sync daemon using project's .venv
# Logs: quantmate/logs/data_sync.out, PID: quantmate/logs/data_sync.pid

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
VENV_PY="$BASE_DIR/.venv/bin/python3"
PID_FILE="$LOG_DIR/data_sync.pid"
OUT_FILE="$LOG_DIR/data_sync.out"
INIT_PID_FILE="$LOG_DIR/data_sync_init.pid"
INIT_OUT_FILE="$LOG_DIR/data_sync_init.out"
BACKFILL_PID_FILE="$LOG_DIR/data_sync_backfill.pid"
BACKFILL_OUT_FILE="$LOG_DIR/data_sync_backfill.out"

DAEMON_PATTERN="app.datasync.scheduler --daemon"
INIT_PATTERN="app.datasync.cli.init_market_data"
SYNC_INIT_PATTERN="app.datasync.scheduler --init"
BACKFILL_PATTERN="app.datasync.scheduler --backfill"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log_line() {
  printf '%s %s\n' "$(timestamp)" "$*"
}

read_pid_file() {
  local file_path="$1"
  if [ -f "$file_path" ]; then
    cat "$file_path" 2>/dev/null || true
  fi
}

pid_is_running() {
  local pid="$1"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

stop_pid_file() {
  local file_path="$1"
  local label="$2"
  local pid
  pid="$(read_pid_file "$file_path")"
  if pid_is_running "$pid"; then
    echo "Stopping $label pid $pid..."
    kill -TERM "$pid" 2>/dev/null || true
  fi
  rm -f "$file_path"
}

any_sync_process_running() {
  pgrep -f "$DAEMON_PATTERN" >/dev/null || \
    pgrep -f "$INIT_PATTERN" >/dev/null || \
    pgrep -f "$SYNC_INIT_PATTERN" >/dev/null || \
    pgrep -f "$BACKFILL_PATTERN" >/dev/null
}

run_init_sequence() {
  local lookback_days="$1"
  shift

  if market_bootstrap_completed; then
    log_line "[1/3] Market bootstrap already completed; skipping bootstrap import"
  else
    log_line "[1/3] Initialize market bootstrap data (tushare/akshare/vnpy)..."
    PYTHONPATH=. "$VENV_PY" -u -m app.datasync.cli.init_market_data "$@"
  fi

  log_line "[2/3] Reconcile registry-driven sync status..."
  PYTHONPATH=. "$VENV_PY" -u -m app.datasync.scheduler --init

  log_line "[3/3] Run historical backfill pass..."
  LOOKBACK_DAYS="$lookback_days" PYTHONPATH=. "$VENV_PY" -u -m app.datasync.scheduler --backfill

  log_line "Initialization sequence complete"
}

load_env() {
  if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment variables from .env"
  fi
}

ensure_db_host_reachable() {
  local host="${MYSQL_HOST:-}"
  if [ -z "$host" ]; then
    return 0
  fi

  if [ "$host" != "mysql" ]; then
    return 0
  fi

  if "$VENV_PY" - <<'PY' >/dev/null 2>&1
import pymysql
from app.infrastructure.config import get_settings

settings = get_settings()
conn = pymysql.connect(
    host=settings.mysql_host,
    port=settings.mysql_port,
    user=settings.mysql_user,
    password=settings.mysql_password,
    connect_timeout=3,
    read_timeout=3,
    write_timeout=3,
)
with conn.cursor() as cur:
    cur.execute('SELECT 1')
conn.close()
PY
  then
    return 0
  fi

  echo "Warning: MYSQL_HOST=mysql is not reachable from this shell; falling back to 127.0.0.1 for this run"
  export MYSQL_HOST=127.0.0.1

  if ! "$VENV_PY" - <<'PY' >/dev/null 2>&1
import pymysql
from app.infrastructure.config import get_settings

settings = get_settings()
conn = pymysql.connect(
    host=settings.mysql_host,
    port=settings.mysql_port,
    user=settings.mysql_user,
    password=settings.mysql_password,
    connect_timeout=3,
    read_timeout=3,
    write_timeout=3,
)
with conn.cursor() as cur:
    cur.execute('SELECT 1')
conn.close()
PY
  then
    echo "Error: MySQL still not reachable with MYSQL_HOST=127.0.0.1. Check MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD in .env" >&2
    return 1
  fi
}

calc_lookback_start_date() {
  local days="${1:-365}"
  LOOKBACK_DAYS="$days" "$VENV_PY" - <<'PY'
from datetime import date, timedelta
import os
days = int(os.getenv('LOOKBACK_DAYS', '365'))
print((date.today() - timedelta(days=days)).isoformat())
PY
}

market_bootstrap_completed() {
  "$VENV_PY" - <<'PY'
import sys
import pymysql
from app.infrastructure.config import get_settings

settings = get_settings()
try:
  conn = pymysql.connect(
      host=settings.mysql_host,
      port=settings.mysql_port,
      user=settings.mysql_user,
      password=settings.mysql_password,
      database=settings.quantmate_db,
      connect_timeout=3,
      read_timeout=3,
      write_timeout=3,
  )
  with conn.cursor() as cur:
    cur.execute("SELECT phase, status FROM init_progress WHERE id = 1")
    row = cur.fetchone()
  conn.close()
  if row and row[0] == 'finished' and row[1] == 'completed':
    sys.exit(0)
  sys.exit(1)
except Exception:
  sys.exit(1)
PY
}

stop() {
  echo "Stopping DataSync/init/backfill..."
  stop_pid_file "$PID_FILE" "DataSync daemon"
  stop_pid_file "$INIT_PID_FILE" "DataSync init"
  stop_pid_file "$BACKFILL_PID_FILE" "DataSync backfill"
  pkill -f "$DAEMON_PATTERN" || true
  pkill -f "$INIT_PATTERN" || true
  pkill -f "$SYNC_INIT_PATTERN" || true
  pkill -f "$BACKFILL_PATTERN" || true

  for i in {1..15}; do
    if any_sync_process_running; then
      sleep 1
    else
      echo "DataSync/init stopped"
      return 0
    fi
  done
  echo "Warning: DataSync/init did not stop within timeout" >&2
  return 1
}

backfill_running() {
  local backfill_pid
  backfill_pid="$(read_pid_file "$BACKFILL_PID_FILE")"
  pid_is_running "$backfill_pid" || pgrep -f "$BACKFILL_PATTERN" >/dev/null
}

start() {
  stop || true
  echo "Starting DataSync daemon..."
  load_env
  ensure_db_host_reachable
  nohup "$VENV_PY" -u -m app.datasync.scheduler --daemon >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 1
  echo "DataSync started (pid $(cat "$PID_FILE"))"
  echo "Logs: $OUT_FILE"
}

init_data() {
  stop || true
  load_env
  ensure_db_host_reachable

  local lookback_days="${INIT_LOOKBACK_DAYS:-365}"
  local start_date="${INIT_START_DATE:-$(calc_lookback_start_date "$lookback_days")}"
  local daily_start_date="${INIT_DAILY_START_DATE:-$start_date}"
  local daily_lookback_days="${INIT_DAILY_LOOKBACK_DAYS:-}"

  local init_args=("--start-date" "$start_date")
  if [ -n "$daily_lookback_days" ]; then
    init_args+=("--daily-lookback-days" "$daily_lookback_days")
  else
    init_args+=("--daily-start-date" "$daily_start_date")
  fi
  if [ "${INIT_SKIP_AUX:-0}" = "1" ]; then
    init_args+=("--skip-aux")
  fi
  if [ "${INIT_SKIP_VNPY:-0}" = "1" ]; then
    init_args+=("--skip-vnpy")
  fi
  if [ "${INIT_SKIP_SCHEMA:-0}" = "1" ]; then
    init_args+=("--skip-schema")
  fi
  if [ "${INIT_RESET_PROGRESS:-0}" = "1" ]; then
    init_args+=("--reset-progress")
  fi

  echo "Starting DataSync initialization sequence in background..."
  echo "  start_date=${start_date} daily_start_date=${daily_start_date} lookback_days=${lookback_days}"
  echo "  logs: $INIT_OUT_FILE"

  run_init_sequence "$lookback_days" "${init_args[@]}" >>"$INIT_OUT_FILE" 2>&1 &
  echo $! > "$INIT_PID_FILE"

  echo "DataSync init started in background (pid $(cat "$INIT_PID_FILE"))"
}

run_backfill() {
  load_env
  ensure_db_host_reachable

  local lookback_days="${INIT_LOOKBACK_DAYS:-365}"

  if backfill_running; then
    echo "DataSync backfill is already running"
    echo "Logs: $BACKFILL_OUT_FILE"
    return 0
  fi

  echo "Starting DataSync backfill in background..."
  echo "  lookback_days=${lookback_days}"
  echo "  logs: $BACKFILL_OUT_FILE"

  (
    log_line "Starting one-shot backfill (lookback_days=${lookback_days})"
    LOOKBACK_DAYS="$lookback_days" PYTHONPATH=. "$VENV_PY" -u -m app.datasync.scheduler --backfill
    log_line "Backfill command finished"
  ) >>"$BACKFILL_OUT_FILE" 2>&1 &
  echo $! > "$BACKFILL_PID_FILE"

  echo "DataSync backfill started in background (pid $(cat "$BACKFILL_PID_FILE"))"
}

status() {
  local daemon_pid
  local init_pid
  local backfill_pid
  daemon_pid="$(read_pid_file "$PID_FILE")"
  init_pid="$(read_pid_file "$INIT_PID_FILE")"
  backfill_pid="$(read_pid_file "$BACKFILL_PID_FILE")"

  if pid_is_running "$daemon_pid" || pgrep -f "$DAEMON_PATTERN" >/dev/null; then
    echo "DataSync daemon: running (log: $OUT_FILE)"
  else
    echo "DataSync daemon: stopped"
  fi

  if pid_is_running "$init_pid" || pgrep -f "$INIT_PATTERN" >/dev/null || pgrep -f "$SYNC_INIT_PATTERN" >/dev/null; then
    echo "DataSync init: running (log: $INIT_OUT_FILE)"
  else
    echo "DataSync init: stopped"
  fi

  if pid_is_running "$backfill_pid" || pgrep -f "$BACKFILL_PATTERN" >/dev/null; then
    echo "DataSync backfill: running (log: $BACKFILL_OUT_FILE)"
  else
    echo "DataSync backfill: stopped"
  fi
}

case "${1-}" in
  start)
    start
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
  init)
    init_data
    ;;
  backfill)
    run_backfill
    ;;
  unlock)
    echo "Releasing backfill_lock via DAO..."
    "$VENV_PY" - <<'PY'
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from app.domains.extdata.dao import data_sync_status_dao as dao
from sqlalchemy import text
try:
    with dao.engine_tm.connect() as conn:
        row = conn.execute(text('SELECT id,is_locked,locked_at,locked_by FROM backfill_lock WHERE id = 1')).fetchone()
        print('BEFORE:', row)
except Exception as e:
    print('ERR reading before:', e)
try:
    dao.release_backfill_lock()
    print('Called release_backfill_lock()')
except Exception as e:
    print('ERR releasing:', e)
try:
    with dao.engine_tm.connect() as conn:
        row = conn.execute(text('SELECT id,is_locked,locked_at,locked_by FROM backfill_lock WHERE id = 1')).fetchone()
        print('AFTER:', row)
except Exception as e:
    print('ERR reading after:', e)
PY
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|init|backfill|unlock}"
    echo ""
    echo "Init options via env vars:"
    echo "  INIT_START_DATE=YYYY-MM-DD   # historical start date (defaults to last year)"
    echo "  INIT_DAILY_START_DATE=YYYY-MM-DD  # optional: limit stock_daily ingest start date"
    echo "  INIT_DAILY_LOOKBACK_DAYS=365      # optional: limit stock_daily ingest by days"
    echo "  INIT_LOOKBACK_DAYS=365       # for --backfill"
    echo "  INIT_SKIP_AUX=1              # optional: skip adj/dividend/top10"
    echo "  INIT_SKIP_VNPY=1             # optional: skip vnpy sync"
    echo "  INIT_SKIP_SCHEMA=1           # optional: skip schema init"
    echo "  INIT_RESET_PROGRESS=1        # optional: reset init_progress before execution"
    echo "  INIT_LOOKBACK_DAYS=365       # used by init --backfill and backfill command"
    exit 2
    ;;
esac

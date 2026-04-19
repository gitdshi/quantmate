#!/usr/bin/env bash
set -euo pipefail

# Start/stop/restart data sync daemon using project's .venv
# Logs: quantmate/logs/data_sync.out, PID: quantmate/logs/data_sync.pid

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$BASE_DIR"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
VENV_PY="$BASE_DIR/.venv/bin/python3"
PID_FILE="$LOG_DIR/data_sync.pid"
OUT_FILE="$LOG_DIR/data_sync.out"
INIT_PID_FILE="$LOG_DIR/data_sync_init.pid"
INIT_OUT_FILE="$LOG_DIR/data_sync_init.out"
BACKFILL_LOOP_PID_FILE="$LOG_DIR/data_sync_backfill.pid"
BACKFILL_LOOP_OUT_FILE="$LOG_DIR/data_sync_backfill.out"
BACKFILL_ONESHOT_PID_FILE="$LOG_DIR/data_sync_backfill_once.pid"
BACKFILL_ONESHOT_OUT_FILE="$LOG_DIR/data_sync_backfill_once.out"

DAEMON_PATTERN="app\\.datasync\\.scheduler --daemon($| )"
INIT_PATTERN="app.datasync.cli.init_market_data"
SYNC_INIT_PATTERN="app\\.datasync\\.scheduler --init($| )"
SYNC_RECONCILE_PATTERN="app\\.datasync\\.scheduler --reconcile($| )"
BACKFILL_LOOP_PATTERN="app\\.datasync\\.scheduler --backfill-loop($| )"
BACKFILL_PATTERN="app\\.datasync\\.scheduler --backfill($| )"
BOOTSTRAP_PATTERN="datasync_service\\.sh __bootstrap_start($| )"

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
    pgrep -f "$BOOTSTRAP_PATTERN" >/dev/null || \
    pgrep -f "$INIT_PATTERN" >/dev/null || \
    pgrep -f "$SYNC_INIT_PATTERN" >/dev/null || \
    pgrep -f "$SYNC_RECONCILE_PATTERN" >/dev/null || \
    pgrep -f "$BACKFILL_LOOP_PATTERN" >/dev/null || \
    pgrep -f "$BACKFILL_PATTERN" >/dev/null
}

run_init_sequence() {
  local reconcile_end_date
  reconcile_end_date="$(date '+%Y-%m-%d')"

  log_line "[1/3] Run startup daily sync for latest trade date..."
  PYTHONPATH=. "$VENV_PY" -u -m app.datasync.scheduler --daily

  if market_bootstrap_completed; then
    log_line "[2/3] Market bootstrap already completed; skipping bootstrap import"
  else
    log_line "[2/3] Initialize market bootstrap data (tushare/akshare/vnpy)..."
    PYTHONPATH=. "$VENV_PY" -u -m app.datasync.cli.init_market_data "$@"
  fi

  log_line "[3/3] Reconcile registry-driven sync status through ${reconcile_end_date}..."
  PYTHONPATH=. "$VENV_PY" -u -m app.datasync.scheduler --reconcile --date "$reconcile_end_date"

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

mysql_ping() {
  "$VENV_PY" - <<'PY' >/dev/null 2>&1
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
}

ensure_db_host_reachable() {
  local host="${MYSQL_HOST:-}"
  if [ -z "$host" ]; then
    return 0
  fi

  if mysql_ping; then
    return 0
  fi

  if [ "$host" != "mysql" ]; then
    echo "Error: MySQL is not reachable at ${MYSQL_HOST}:${MYSQL_PORT}. Check MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD in .env" >&2
    return 1
  fi

  echo "Warning: MYSQL_HOST=mysql is not reachable from this shell; falling back to 127.0.0.1 for this run"
  export MYSQL_HOST=127.0.0.1

  if ! mysql_ping; then
    echo "Error: MySQL still not reachable with MYSQL_HOST=127.0.0.1. Check MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD in .env" >&2
    return 1
  fi
}

calc_default_init_start_date() {
  PYTHONPATH=. "$VENV_PY" - <<'PY'
from app.datasync.service.init_service import get_coverage_window

print(get_coverage_window()["start_date"].isoformat())
PY
}

needs_initialization() {
  PYTHONPATH=. "$VENV_PY" - <<'PY'
import traceback

from app.datasync.service.init_service import needs_initialization

try:
  raise SystemExit(0 if needs_initialization() else 1)
except SystemExit:
  raise
except Exception:
  traceback.print_exc()
  raise SystemExit(2)
PY
}

prepare_init_args() {
  if [ -n "${INIT_START_DATE:-}" ]; then
    EFFECTIVE_INIT_START_DATE="$INIT_START_DATE"
  else
    EFFECTIVE_INIT_START_DATE="$(calc_default_init_start_date)"
  fi

  EFFECTIVE_INIT_DAILY_START_DATE="${INIT_DAILY_START_DATE:-$EFFECTIVE_INIT_START_DATE}"

  INIT_ARGS=("--start-date" "$EFFECTIVE_INIT_START_DATE")
  INIT_ARGS+=("--daily-start-date" "$EFFECTIVE_INIT_DAILY_START_DATE")
  if [ "${INIT_SKIP_AUX:-0}" = "1" ]; then
    INIT_ARGS+=("--skip-aux")
  fi
  if [ "${INIT_SKIP_VNPY:-0}" = "1" ]; then
    INIT_ARGS+=("--skip-vnpy")
  fi
  if [ "${INIT_SKIP_SCHEMA:-0}" = "1" ]; then
    INIT_ARGS+=("--skip-schema")
  fi
  if [ "${INIT_RESET_PROGRESS:-0}" = "1" ]; then
    INIT_ARGS+=("--reset-progress")
  fi
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
  stop_pid_file "$BACKFILL_LOOP_PID_FILE" "DataSync backfill loop"
  stop_pid_file "$BACKFILL_ONESHOT_PID_FILE" "DataSync backfill one-shot"
  pkill -f "$DAEMON_PATTERN" || true
  pkill -f "$BOOTSTRAP_PATTERN" || true
  pkill -f "$INIT_PATTERN" || true
  pkill -f "$SYNC_INIT_PATTERN" || true
  pkill -f "$SYNC_RECONCILE_PATTERN" || true
  pkill -f "$BACKFILL_LOOP_PATTERN" || true
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
  local backfill_loop_pid
  local backfill_once_pid
  backfill_loop_pid="$(read_pid_file "$BACKFILL_LOOP_PID_FILE")"
  backfill_once_pid="$(read_pid_file "$BACKFILL_ONESHOT_PID_FILE")"
  pid_is_running "$backfill_loop_pid" || \
    pid_is_running "$backfill_once_pid" || \
    pgrep -f "$BACKFILL_LOOP_PATTERN" >/dev/null || \
    pgrep -f "$BACKFILL_PATTERN" >/dev/null
}

start_daemon() {
  local skip_initial_daily="${1:-0}"
  local skip_initial_reconcile="${2:-0}"

  echo "Starting DataSync daemon..."
  nohup env \
    DATASYNC_SKIP_INITIAL_DAILY="$skip_initial_daily" \
    DATASYNC_SKIP_INITIAL_RECONCILE="$skip_initial_reconcile" \
    PYTHONPATH=. \
    "$VENV_PY" -u -m app.datasync.scheduler --daemon >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
}

start_backfill_loop() {
  echo "Starting DataSync backfill loop..."
  nohup env PYTHONPATH=. "$VENV_PY" -u -m app.datasync.scheduler --backfill-loop >>"$BACKFILL_LOOP_OUT_FILE" 2>&1 &
  echo $! > "$BACKFILL_LOOP_PID_FILE"
}

bootstrap_start() {
  local skip_initial_daily=0
  local skip_initial_reconcile=0
  local init_check_status=0

  trap 'rm -f "$INIT_PID_FILE"' EXIT

  load_env
  ensure_db_host_reachable

  if needs_initialization; then
    prepare_init_args
    echo "Initialization markers missing; running first-deploy init sequence in background coordinator..."
    echo "  start_date=${EFFECTIVE_INIT_START_DATE} daily_start_date=${EFFECTIVE_INIT_DAILY_START_DATE}"
    if ! run_init_sequence "${INIT_ARGS[@]}"; then
      echo "DataSync initialization failed; daemon/backfill were not started" >&2
      return 1
    fi
    skip_initial_daily=1
    skip_initial_reconcile=1
  else
    init_check_status=$?
    if [ "$init_check_status" -eq 1 ]; then
      echo "Initialization markers present; skipping first-deploy init"
    else
      echo "Failed to determine initialization state; daemon/backfill were not started" >&2
      return 1
    fi
  fi

  start_daemon "$skip_initial_daily" "$skip_initial_reconcile"
  start_backfill_loop
  sleep 1
  echo "DataSync daemon started (pid $(cat "$PID_FILE"))"
  echo "  log: $OUT_FILE"
  echo "DataSync backfill loop started (pid $(cat "$BACKFILL_LOOP_PID_FILE"))"
  echo "  log: $BACKFILL_LOOP_OUT_FILE"
}

start() {
  stop || true
  load_env
  ensure_db_host_reachable

  : > "$INIT_OUT_FILE"
  echo "Starting DataSync bootstrap coordinator in background..."
  nohup "$SCRIPT_PATH" __bootstrap_start >>"$INIT_OUT_FILE" 2>&1 &
  echo $! > "$INIT_PID_FILE"
  sleep 1
  echo "DataSync bootstrap started in background (pid $(cat "$INIT_PID_FILE"))"
  echo "  init log: $INIT_OUT_FILE"

  if pid_is_running "$(read_pid_file "$PID_FILE")"; then
    echo "DataSync daemon running (pid $(cat "$PID_FILE"))"
    echo "  log: $OUT_FILE"
  else
    echo "DataSync daemon will be started by the bootstrap coordinator"
  fi

  if pid_is_running "$(read_pid_file "$BACKFILL_LOOP_PID_FILE")"; then
    echo "DataSync backfill loop running (pid $(cat "$BACKFILL_LOOP_PID_FILE"))"
    echo "  log: $BACKFILL_LOOP_OUT_FILE"
  else
    echo "DataSync backfill loop will be started by the bootstrap coordinator"
  fi
}

init_data() {
  stop || true
  load_env
  ensure_db_host_reachable
  prepare_init_args

  echo "Starting DataSync initialization sequence in background..."
  echo "  start_date=${EFFECTIVE_INIT_START_DATE} daily_start_date=${EFFECTIVE_INIT_DAILY_START_DATE}"
  echo "  logs: $INIT_OUT_FILE"

  run_init_sequence "${INIT_ARGS[@]}" >>"$INIT_OUT_FILE" 2>&1 &
  echo $! > "$INIT_PID_FILE"

  echo "DataSync init started in background (pid $(cat "$INIT_PID_FILE"))"
}

run_backfill() {
  load_env
  ensure_db_host_reachable

  local backfill_args=("--backfill")
  local backfill_start_date
  backfill_start_date="$(calc_default_init_start_date)"

  if backfill_running; then
    echo "DataSync backfill is already running"
    echo "Logs: $BACKFILL_LOOP_OUT_FILE"
    return 0
  fi

  echo "Starting DataSync one-shot backfill in background..."
  echo "  start_date=${backfill_start_date}"
  echo "  logs: $BACKFILL_ONESHOT_OUT_FILE"

  (
    log_line "Starting one-shot backfill (start_date=${backfill_start_date})"
    env PYTHONPATH=. "$VENV_PY" -u -m app.datasync.scheduler "${backfill_args[@]}"
    log_line "Backfill command finished"
  ) >>"$BACKFILL_ONESHOT_OUT_FILE" 2>&1 &
  echo $! > "$BACKFILL_ONESHOT_PID_FILE"

  echo "DataSync one-shot backfill started in background (pid $(cat "$BACKFILL_ONESHOT_PID_FILE"))"
}

status() {
  local daemon_pid
  local init_pid
  local backfill_loop_pid
  local backfill_once_pid
  daemon_pid="$(read_pid_file "$PID_FILE")"
  init_pid="$(read_pid_file "$INIT_PID_FILE")"
  backfill_loop_pid="$(read_pid_file "$BACKFILL_LOOP_PID_FILE")"
  backfill_once_pid="$(read_pid_file "$BACKFILL_ONESHOT_PID_FILE")"

  if pid_is_running "$daemon_pid" || pgrep -f "$DAEMON_PATTERN" >/dev/null; then
    echo "DataSync daemon: running (log: $OUT_FILE)"
  else
    echo "DataSync daemon: stopped"
  fi

  if pid_is_running "$init_pid" || pgrep -f "$INIT_PATTERN" >/dev/null || pgrep -f "$SYNC_INIT_PATTERN" >/dev/null || pgrep -f "$SYNC_RECONCILE_PATTERN" >/dev/null; then
    echo "DataSync init: running (log: $INIT_OUT_FILE)"
  else
    echo "DataSync init: stopped"
  fi

  if pid_is_running "$backfill_loop_pid" || pgrep -f "$BACKFILL_LOOP_PATTERN" >/dev/null; then
    echo "DataSync backfill loop: running (log: $BACKFILL_LOOP_OUT_FILE)"
  else
    echo "DataSync backfill loop: stopped"
  fi

  if pid_is_running "$backfill_once_pid" || pgrep -f "$BACKFILL_PATTERN" >/dev/null; then
    echo "DataSync one-shot backfill: running (log: $BACKFILL_ONESHOT_OUT_FILE)"
  else
    echo "DataSync one-shot backfill: stopped"
  fi
}

case "${1-}" in
  start)
    start
    ;;
  __bootstrap_start)
    bootstrap_start
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
    echo "  INIT_START_DATE=YYYY-MM-DD   # optional explicit historical start date"
    echo "  INIT_DAILY_START_DATE=YYYY-MM-DD  # optional: limit stock_daily ingest start date"
    echo "  INIT_SKIP_AUX=1              # optional: skip adj/dividend/top10"
    echo "  INIT_SKIP_VNPY=1             # optional: skip vnpy sync"
    echo "  INIT_SKIP_SCHEMA=1           # optional: skip schema init"
    echo "  INIT_RESET_PROGRESS=1        # optional: reset init_progress before execution"
    echo ""
    echo "Runtime/backfill options via env vars:"
    echo "  SYNC_INIT_DEFAULT_START_DATE=2010-01-01    # optional explicit sync start date; if empty, env window applies"
    echo "  BACKFILL_IDLE_INTERVAL_HOURS=4             # dedicated backfill loop sleep interval"
    exit 2
    ;;
esac

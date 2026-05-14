#!/usr/bin/env bash

# If invoked via `sh script.sh`, re-exec with bash to support arrays, process substitution and pipefail.
if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@"
fi

set -euo pipefail

# Safe SQL deployment for test/dev environments.
# - Applies pending files in mysql/migrations/ only
# - Never runs mysql/init/quantmate.sql
# - Blocks migration files that contain DROP DATABASE / DROP SCHEMA

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MIGRATIONS_DIR="$BASE_DIR/mysql/migrations"
ENV_FILE_DEFAULT="$BASE_DIR/.env"
ENV_FILE_TEST="$BASE_DIR/.env.test"

PRESET_DRY_RUN="${DRY_RUN-}"
PRESET_DB_NAME="${DB_NAME-}"
DRY_RUN=0
DRY_RUN_REQUESTED=0
DB_NAME_REQUESTED=0
REQUESTED_DB_NAME=""
TARGET_VERSION=""
ENV_FILE=""
DB_NAME="${DB_NAME:-quantmate}"
OFFLINE_DRY_RUN=0

normalize_bool_flag() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      printf '1'
      ;;
    *)
      printf '0'
      ;;
  esac
}

usage() {
  cat <<'EOF'
Usage:
  ./scripts/deploy_sql_test_safe.sh [options]

Options:
  --db-name <name>       Target database name (default: quantmate)
  --env-file <path>      Explicit env file path (default: .env.test then .env)
  --to-version <ver>     Apply up to specific version (e.g. 018)
  --dry-run              Print what would be executed without applying
  -h, --help             Show help

Environment variables (can come from env file):
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD

Safety guarantees:
  1) Does NOT run mysql/init/quantmate.sql
  2) Does NOT execute DROP DATABASE / DROP SCHEMA
  3) Only applies incremental migration files under mysql/migrations
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --db-name)
      DB_NAME="$2"
      DB_NAME_REQUESTED=1
      REQUESTED_DB_NAME="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --to-version)
      TARGET_VERSION="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN_REQUESTED=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [ -z "$ENV_FILE" ]; then
  if [ -f "$ENV_FILE_TEST" ]; then
    ENV_FILE="$ENV_FILE_TEST"
  else
    ENV_FILE="$ENV_FILE_DEFAULT"
  fi
fi

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  echo "Loaded env from: $ENV_FILE"
else
  echo "Warning: env file not found ($ENV_FILE). Falling back to current shell env."
fi

if [ -n "$PRESET_DB_NAME" ]; then
  DB_NAME="$PRESET_DB_NAME"
fi
if [ "$DB_NAME_REQUESTED" -eq 1 ]; then
  DB_NAME="$REQUESTED_DB_NAME"
fi

if [ -n "$PRESET_DRY_RUN" ]; then
  DRY_RUN="$PRESET_DRY_RUN"
fi

DRY_RUN="$(normalize_bool_flag "${DRY_RUN:-0}")"
if [ "$DRY_RUN_REQUESTED" -eq 1 ]; then
  DRY_RUN=1
fi

: "${MYSQL_HOST:?MYSQL_HOST is required}"
: "${MYSQL_PORT:?MYSQL_PORT is required}"
: "${MYSQL_USER:?MYSQL_USER is required}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD is required}"

# Resolve mysql executable: fall back to docker exec when mysql is not in PATH
MYSQL_CONTAINER=""
if ! command -v mysql >/dev/null 2>&1; then
  MYSQL_CONTAINER="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -iE 'mysql' | head -1)"
  if [ -z "$MYSQL_CONTAINER" ]; then
    echo "Error: 'mysql' client not found in PATH and no running MySQL container detected." >&2
    echo "  Install with: brew install mysql-client" >&2
    echo "  Or start the dev stack: docker compose -f docker-compose.dev.yml up -d" >&2
    exit 1
  fi
  echo "mysql not in PATH — routing through container: $MYSQL_CONTAINER"
fi

_mysql() {
  if [ -n "$MYSQL_CONTAINER" ]; then
    docker exec -i "$MYSQL_CONTAINER" \
      mysql --protocol TCP --default-character-set=utf8mb4 -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$@"
  else
    MYSQL_PWD="$MYSQL_PASSWORD" mysql --protocol TCP --default-character-set=utf8mb4 -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" "$@"
  fi
}

mysql_exec_db() {
  local db="$1"
  local sql="$2"
  _mysql "$db" -Nse "$sql"
}

mysql_run_file() {
  local db="$1"
  local file="$2"
  _mysql "$db" < "$file"
}

echo "Checking MySQL connectivity..."
if ! _mysql -Nse "SELECT 1" >/dev/null 2>&1; then
  if [ "$MYSQL_HOST" = "mysql" ]; then
    echo "MYSQL_HOST=mysql is not reachable from current shell, falling back to 127.0.0.1"
    MYSQL_HOST="127.0.0.1"
  fi
fi

if ! _mysql -Nse "SELECT 1" >/dev/null 2>&1; then
  if [ "$DRY_RUN" -eq 1 ]; then
    OFFLINE_DRY_RUN=1
    echo "Warning: MySQL unreachable. Continuing in offline dry-run mode."
  else
    echo "Error: MySQL unreachable. Please check MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD." >&2
    exit 1
  fi
fi

if [ "$DRY_RUN" -eq 0 ] && [ "$OFFLINE_DRY_RUN" -eq 0 ]; then
  mysql_exec_db "information_schema" "CREATE DATABASE IF NOT EXISTS $DB_NAME CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" >/dev/null || true
fi

HAS_MIGRATION_TABLE="1"
if [ "$OFFLINE_DRY_RUN" -eq 0 ]; then
  HAS_MIGRATION_TABLE="$(mysql_exec_db "$DB_NAME" "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$DB_NAME' AND table_name='schema_migrations';")"
fi

if [ "$HAS_MIGRATION_TABLE" = "0" ]; then
  echo "schema_migrations not found. Bootstrapping with 000_create_migration_table.sql"
  if [ "$DRY_RUN" -eq 0 ]; then
    mysql_run_file "$DB_NAME" "$MIGRATIONS_DIR/000_create_migration_table.sql"
    mysql_exec_db "$DB_NAME" "INSERT IGNORE INTO schema_migrations (version, name) VALUES ('000', '000_create_migration_table.sql');" >/dev/null
  fi
fi

echo "Scanning migration files..."
# bash 3.2 compat (macOS): mapfile is bash 4.x only
FILES=()
BASENAMES=()
while IFS= read -r _f; do
  FILES+=("$_f")
  BASENAMES+=("$(basename "$_f")")
done < <(find "$MIGRATIONS_DIR" -maxdepth 1 -type f -name '*.sql' | sort)

if [ "${#FILES[@]}" -eq 0 ]; then
  echo "No migration files found in: $MIGRATIONS_DIR"
  exit 0
fi

DUPLICATE_VERSIONS="$({ printf '%s\n' "${BASENAMES[@]}" | sed 's/_.*$//'; } | sort | uniq -d)"
if [ -n "$DUPLICATE_VERSIONS" ]; then
  echo "Error: duplicate migration versions detected:" >&2
  while IFS= read -r version; do
    [ -n "$version" ] || continue
    echo "  version $version:" >&2
    printf '%s\n' "${BASENAMES[@]}" | grep "^${version}_" | sed 's/^/    - /' >&2
  done <<< "$DUPLICATE_VERSIONS"
  exit 1
fi

APPLY_COUNT=0
SKIP_COUNT=0

for file in "${FILES[@]}"; do
  name="$(basename "$file")"
  version="${name%%_*}"

  if [ -n "$TARGET_VERSION" ] && [[ "$version" > "$TARGET_VERSION" ]]; then
    continue
  fi

  if grep -Eiq '^[[:space:]]*DROP[[:space:]]+(DATABASE|SCHEMA)([[:space:]]|;)' "$file"; then
    echo "Blocked by safety policy: $name contains DROP DATABASE/SCHEMA" >&2
    exit 1
  fi

  applied="0"
  if [ "$OFFLINE_DRY_RUN" -eq 0 ]; then
    applied="$(mysql_exec_db "$DB_NAME" "SELECT COUNT(*) FROM schema_migrations WHERE version='$version';")"
  fi

  if [ "$applied" != "0" ]; then
    echo "[SKIP] $name (already applied)"
    SKIP_COUNT=$((SKIP_COUNT + 1))
    continue
  fi

  if [ "$DRY_RUN" -eq 1 ]; then
    echo "[PLAN] $name"
    APPLY_COUNT=$((APPLY_COUNT + 1))
    continue
  fi

  echo "[APPLY] $name"
  mysql_run_file "$DB_NAME" "$file"
  mysql_exec_db "$DB_NAME" "INSERT IGNORE INTO schema_migrations (version, name) VALUES ('$version', '$name');" >/dev/null
  APPLY_COUNT=$((APPLY_COUNT + 1))
done

echo ""
echo "Deployment finished."
echo "  applied: $APPLY_COUNT"
echo "  skipped: $SKIP_COUNT"
echo "  target database: $DB_NAME"

if [ "$DRY_RUN" -eq 0 ] && [ "$OFFLINE_DRY_RUN" -eq 0 ]; then
  echo ""
  echo "Latest versions:"
  mysql_exec_db "$DB_NAME" "SELECT version, name, applied_at FROM schema_migrations ORDER BY version DESC LIMIT 5;"
fi

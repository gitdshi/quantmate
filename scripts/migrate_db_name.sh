#!/usr/bin/env bash
set -euo pipefail

SOURCE_DB="${1:-${SOURCE_DB:-}}"
TARGET_DB="${QUANTMATE_DB:-quantmate}"

if [ -z "$SOURCE_DB" ]; then
  echo "Usage: $0 <source_db> (or set SOURCE_DB env var)" >&2
  exit 1
fi

: "${MYSQL_HOST:?MYSQL_HOST is required}"
: "${MYSQL_USER:?MYSQL_USER is required}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD is required}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

MYSQL_CMD=(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD")

# Create target database if missing
"${MYSQL_CMD[@]}" -e "CREATE DATABASE IF NOT EXISTS \`$TARGET_DB\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# Copy all tables from source to target (non-destructive)
TABLES=$("${MYSQL_CMD[@]}" -N -e "SELECT table_name FROM information_schema.tables WHERE table_schema='${SOURCE_DB}'")

if [ -z "$TABLES" ]; then
  echo "No tables found in source DB: $SOURCE_DB" >&2
  exit 1
fi

echo "Copying tables from $SOURCE_DB to $TARGET_DB ..."

"${MYSQL_CMD[@]}" -e "SET FOREIGN_KEY_CHECKS=0;"

for t in $TABLES; do
  echo "  - $t"
  "${MYSQL_CMD[@]}" -e "CREATE TABLE IF NOT EXISTS \`$TARGET_DB\`.\`$t\` LIKE \`$SOURCE_DB\`.\`$t\`;"
  "${MYSQL_CMD[@]}" -e "INSERT INTO \`$TARGET_DB\`.\`$t\` SELECT * FROM \`$SOURCE_DB\`.\`$t\`;"
  "${MYSQL_CMD[@]}" -e "ANALYZE TABLE \`$TARGET_DB\`.\`$t\`;"
  "${MYSQL_CMD[@]}" -e "SET FOREIGN_KEY_CHECKS=1;"

done

echo "Done. Source DB was not modified."

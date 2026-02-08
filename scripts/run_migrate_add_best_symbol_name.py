"""Add `best_symbol_name` column to `bulk_backtest` if missing."""
from app.api.services.db import get_db_connection
from sqlalchemy import text

conn = get_db_connection()

try:
    conn.execute(text("ALTER TABLE bulk_backtest ADD COLUMN best_symbol_name VARCHAR(255) NULL AFTER best_symbol"))
    conn.commit()
    print("OK Added best_symbol_name column to bulk_backtest")
except Exception as e:
    if "Duplicate" in str(e) or "already exists" in str(e):
        print("SKIP best_symbol_name already exists")
    else:
        print("ERR", e)

conn.close()
print("DONE")

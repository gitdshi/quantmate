"""DAO layer for extdata domain: centralized DB operations for sync status and trade calendar."""

import os
import logging
import socket
import subprocess
from functools import lru_cache
from datetime import date, timedelta
from typing import Dict, List, Tuple, Any

from sqlalchemy import text
from app.infrastructure.config import get_runtime_int
from app.infrastructure.db.connections import (
    get_quantmate_engine,
    get_tushare_engine,
    get_vnpy_engine,
    get_akshare_engine,
)

logger = logging.getLogger(__name__)

_LEGACY_STEP_ALIASES = {
    "akshare_index": ("akshare", "index_daily"),
    "vnpy_sync": ("vnpy", "vnpy_sync"),
}
_LEGACY_INTERFACE_ALIASES = {value: key for key, value in _LEGACY_STEP_ALIASES.items()}
_DEFAULT_SOURCE_HINTS = ("tushare", "akshare", "vnpy")

# Engines provided by infrastructure connection helpers
engine_tm = get_quantmate_engine()
engine_ts = get_tushare_engine()
engine_vn = get_vnpy_engine()
engine_ak = get_akshare_engine()


DATA_SYNC_STATUS_SQL = """
CREATE TABLE IF NOT EXISTS data_sync_status (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sync_date DATE NOT NULL,
    source VARCHAR(50) NOT NULL,
    interface_key VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    rows_synced INT DEFAULT 0,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_date_source_interface (sync_date, source, interface_key),
    INDEX idx_status (status),
    INDEX idx_sync_date (sync_date),
    INDEX idx_source_interface (source, interface_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Per-interface per-trading-day sync status tracking';
"""


TRADE_CAL_SQL = """
CREATE TABLE IF NOT EXISTS trade_cal (
    trade_date DATE PRIMARY KEY,
    is_trade_day TINYINT NOT NULL DEFAULT 1,
    source VARCHAR(32) DEFAULT 'akshare',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _step_to_source_interface(step_name: str) -> tuple[str, str]:
    normalized = (step_name or "").strip()
    if not normalized:
        return ("legacy", normalized)

    legacy_alias = _LEGACY_STEP_ALIASES.get(normalized)
    if legacy_alias is not None:
        return legacy_alias

    for delimiter in ("/", ":"):
        if normalized.count(delimiter) == 1:
            source, interface_key = (part.strip() for part in normalized.split(delimiter, 1))
            if source and interface_key:
                return source, interface_key

    for source in sorted(_DEFAULT_SOURCE_HINTS, key=len, reverse=True):
        prefix = f"{source}_"
        if normalized.startswith(prefix) and len(normalized) > len(prefix):
            return source, normalized[len(prefix) :]

    for refresh in (False, True):
        known_sources, _, alias_map = _get_step_resolution_metadata(refresh=refresh)
        resolved = alias_map.get(normalized)
        if resolved is not None:
            return resolved

        for source in sorted(known_sources, key=len, reverse=True):
            prefix = f"{source}_"
            if normalized.startswith(prefix) and len(normalized) > len(prefix):
                return source, normalized[len(prefix) :]

    return ("legacy", normalized)


def _source_interface_to_step(source: str, interface_key: str) -> str:
    normalized_source = (source or "").strip()
    normalized_interface = (interface_key or "").strip()
    if not normalized_source or not normalized_interface:
        return f"{normalized_source}:{normalized_interface}".strip(":")

    legacy_alias = _LEGACY_INTERFACE_ALIASES.get((normalized_source, normalized_interface))
    if legacy_alias is not None:
        return legacy_alias

    if normalized_source in _DEFAULT_SOURCE_HINTS:
        return f"{normalized_source}_{normalized_interface}"

    for refresh in (False, True):
        known_sources, known_pairs, _ = _get_step_resolution_metadata(refresh=refresh)
        if (normalized_source, normalized_interface) in known_pairs or normalized_source in known_sources:
            return f"{normalized_source}_{normalized_interface}"

    return f"{normalized_source}:{normalized_interface}"


@lru_cache(maxsize=8)
def _load_step_resolution_metadata(engine_identity: int) -> tuple[set[str], set[tuple[str, str]], dict[str, tuple[str, str]]]:
    del engine_identity

    known_sources = set(_DEFAULT_SOURCE_HINTS)
    known_pairs: set[tuple[str, str]] = set()
    alias_map = dict(_LEGACY_STEP_ALIASES)

    try:
        with engine_tm.connect() as conn:
            result = conn.execute(text("SELECT source, item_key, target_table FROM data_source_items"))
            rows = result.fetchall()
    except Exception:
        logger.debug("Failed to load dynamic step resolution metadata from data_source_items", exc_info=True)
        return known_sources, known_pairs, alias_map

    if not isinstance(rows, (list, tuple)):
        return known_sources, known_pairs, alias_map

    for row in rows:
        source = (row[0] or "").strip() if isinstance(row[0], str) else str(row[0] or "").strip()
        item_key = (row[1] or "").strip() if isinstance(row[1], str) else str(row[1] or "").strip()
        target_table = (row[2] or "").strip() if isinstance(row[2], str) else str(row[2] or "").strip()
        if not source or not item_key:
            continue

        known_sources.add(source)
        known_pairs.add((source, item_key))

        for candidate in {item_key, target_table} - {""}:
            alias_map.setdefault(f"{source}_{candidate}", (source, candidate))
            alias_map.setdefault(f"{source}/{candidate}", (source, candidate))
            alias_map.setdefault(f"{source}:{candidate}", (source, candidate))

    return known_sources, known_pairs, alias_map


def _get_step_resolution_metadata(refresh: bool = False) -> tuple[set[str], set[tuple[str, str]], dict[str, tuple[str, str]]]:
    if refresh:
        _load_step_resolution_metadata.cache_clear()
    return _load_step_resolution_metadata(id(engine_tm))


def ensure_tables():
    """Ensure `data_sync_status` and `trade_cal` exist."""
    logger.info("Ensuring data_sync_status table in quantmate DB")
    with engine_tm.begin() as conn:
        conn.execute(text(DATA_SYNC_STATUS_SQL))

    logger.info("Ensuring trade_cal table in akshare DB")
    with engine_ak.begin() as conn:
        conn.execute(text(TRADE_CAL_SQL))


def get_stock_daily_counts(start: date, end: date) -> Dict[date, int]:
    """Return mapping trade_date -> count from `stock_daily` between start and end."""
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM stock_daily
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_adj_factor_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM adj_factor
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_bak_daily_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM bak_daily
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_moneyflow_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM stock_moneyflow
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_suspend_d_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM suspend_d
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_suspend_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT suspend_date, COUNT(*) as cnt
            FROM `suspend`
            WHERE suspend_date BETWEEN :s AND :e
            GROUP BY suspend_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_stock_weekly_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM stock_weekly
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_stock_monthly_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM stock_monthly
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_vnpy_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_vn.connect() as conn:
        res = conn.execute(
            text("""
            SELECT DATE(`datetime`) as dt, COUNT(*) as cnt
            FROM dbbardata
            WHERE DATE(`datetime`) BETWEEN :s AND :e
            GROUP BY dt
        """),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_index_daily_count_for_date(d: date) -> int:
    with engine_ak.connect() as conn:
        res = conn.execute(text("SELECT COUNT(1) FROM index_daily WHERE trade_date = :d"), {"d": d})
        row = res.fetchone()
        return int(row[0] if row and row[0] is not None else 0)


def get_stock_basic_count() -> int:
    with engine_ts.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM stock_basic"))
        row = res.fetchone()
        return int(row[0] if row and row[0] is not None else 0)


def get_adj_factor_count_for_date(d: date) -> int:
    with engine_ts.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM adj_factor WHERE trade_date = :d"), {"d": d})
        row = res.fetchone()
        return int(row[0] if row and row[0] is not None else 0)


def get_dividend_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text(
                """
            SELECT ann_date, COUNT(*) as cnt
            FROM stock_dividend
            WHERE ann_date BETWEEN :s AND :e
            GROUP BY ann_date
        """
            ),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_top10_holders_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(
            text(
                """
            SELECT end_date, COUNT(*) as cnt
            FROM top10_holders
            WHERE end_date BETWEEN :s AND :e
            GROUP BY end_date
        """
            ),
            {"s": start, "e": end},
        )
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_stock_daily_ts_codes_for_date(d: date) -> List[str]:
    with engine_ts.connect() as conn:
        res = conn.execute(
            text("SELECT DISTINCT ts_code FROM stock_daily WHERE trade_date = :d ORDER BY ts_code"), {"d": d}
        )
        return [r[0] for r in res.fetchall()]


def bulk_upsert_status(rows: List[Tuple[Any, ...]], chunk_size: int = 1000) -> int:
    """Bulk upsert rows into `data_sync_status`.

    `rows` is a list of tuples matching (sync_date, step_name, status, rows_synced, error_message, started_at, finished_at)
    Returns number of rows processed.
    """
    insert_sql = (
        "INSERT INTO data_sync_status "
        "(sync_date, source, interface_key, status, rows_synced, error_message, started_at, finished_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE status=VALUES(status), rows_synced=VALUES(rows_synced), error_message=VALUES(error_message), finished_at=VALUES(finished_at), updated_at=CURRENT_TIMESTAMP"
    )

    processed = 0
    raw_conn = engine_tm.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            params = []
            for r in chunk:
                sync_date, step_name, status, rows_synced, error_message, started_at, finished_at = r
                source, interface_key = _step_to_source_interface(step_name)
                params.append(
                    (
                        sync_date.strftime("%Y-%m-%d") if hasattr(sync_date, "strftime") else sync_date,
                        source,
                        interface_key,
                        status,
                        rows_synced,
                        error_message,
                        started_at,
                        finished_at,
                    )
                )
            cursor.executemany(insert_sql, params)
            raw_conn.commit()
            processed += len(chunk)
            logger.debug("bulk_upsert_status: inserted %d rows", processed)
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            raw_conn.close()
        except Exception:
            pass

    return processed


def write_step_status(sync_date: date, step_name: str, status: str, rows_synced: int = 0, error_message: Any = None):
    """Insert or update a single step status row."""
    with engine_tm.begin() as conn:
        source, interface_key = _step_to_source_interface(step_name)
        conn.execute(
            text("""
            INSERT INTO data_sync_status 
                (sync_date, source, interface_key, status, rows_synced, error_message, started_at, finished_at)
            VALUES (:sd, :src, :ik, :status, :rows, :err, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                rows_synced = VALUES(rows_synced),
                error_message = VALUES(error_message),
                finished_at = VALUES(finished_at),
                updated_at = CURRENT_TIMESTAMP
        """),
            {
                "sd": sync_date,
                "src": source,
                "ik": interface_key,
                "status": status,
                "rows": rows_synced,
                "err": error_message,
            },
        )


def get_step_status(sync_date: date, step_name: str) -> Any:
    with engine_tm.connect() as conn:
        source, interface_key = _step_to_source_interface(step_name)
        res = conn.execute(
            text("SELECT status FROM data_sync_status WHERE sync_date = :sd AND source = :src AND interface_key = :ik"),
            {"sd": sync_date, "src": source, "ik": interface_key},
        )
        row = res.fetchone()
        return row[0] if row else None


def get_failed_steps(lookback_days: int = 60) -> List[Tuple[date, str]]:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=lookback_days)
    with engine_tm.connect() as conn:
        res = conn.execute(
            text("""
            SELECT sync_date, source, interface_key FROM data_sync_status
            WHERE sync_date >= :start AND sync_date <= :end
              AND status IN ('error', 'partial', 'pending')
            ORDER BY sync_date ASC, source, interface_key
        """),
            {"start": start, "end": end},
        )
        return [(row[0], _source_interface_to_step(row[1], row[2])) for row in res.fetchall()]


def get_cached_trade_dates(start_date: date, end_date: date) -> List[date]:
    """Return cached trade dates from akshare.trade_cal between start and end."""
    with engine_ak.connect() as conn:
        res = conn.execute(
            text("""
            SELECT trade_date FROM trade_cal
            WHERE trade_date BETWEEN :s AND :e AND is_trade_day = 1
            ORDER BY trade_date ASC
        """),
            {"s": start_date, "e": end_date},
        )
        return [row[0] for row in res.fetchall()]


def upsert_trade_dates(dates: List[date]):
    """Bulk insert trade dates into akshare.trade_cal (INSERT IGNORE semantics)."""
    if not dates:
        return 0
    raw = engine_ak.raw_connection()
    try:
        cur = raw.cursor()
        params = [(d.strftime("%Y-%m-%d"), 1, "akshare") for d in dates]
        cur.executemany("INSERT IGNORE INTO trade_cal (trade_date, is_trade_day, source) VALUES (%s, %s, %s)", params)
        raw.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            raw.close()
        except Exception:
            pass


def truncate_trade_cal():
    """Truncate the akshare.trade_cal table."""
    with engine_ak.begin() as conn:
        conn.execute(text("TRUNCATE TABLE trade_cal"))


# =============================================================================
# Backfill Lock Management (DB-based concurrency control)
# =============================================================================

BACKFILL_LOCK_SQL = """
CREATE TABLE IF NOT EXISTS backfill_lock (
    id INT PRIMARY KEY DEFAULT 1,
    is_locked TINYINT NOT NULL DEFAULT 0,
    locked_at TIMESTAMP NULL,
    locked_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_backfill_lock_table():
    """Ensure backfill_lock table exists."""
    logger.info("Ensuring backfill_lock table in quantmate DB")
    with engine_tm.begin() as conn:
        conn.execute(text(BACKFILL_LOCK_SQL))
        # Initialize with unlocked state
        conn.execute(
            text("""
            INSERT IGNORE INTO backfill_lock (id, is_locked) VALUES (1, 0)
        """)
        )


def _is_local_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _local_backfill_process_running() -> bool:
    patterns = (
        "app.datasync.scheduler --backfill",
        "app.datasync.scheduler --daemon",
        "app.datasync.service.data_sync_daemon",
    )
    for pattern in patterns:
        try:
            result = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, check=False)
        except FileNotFoundError:
            return False
        if result.returncode == 0 and result.stdout.strip():
            return True
    return False


def release_orphaned_backfill_lock() -> bool:
    """Release a local backfill lock when no matching owner process still exists."""
    ensure_backfill_lock_table()
    hostname = socket.gethostname()
    with engine_tm.begin() as conn:
        row = conn.execute(text("SELECT is_locked, locked_by FROM backfill_lock WHERE id = 1")).fetchone()
        if not row:
            return False

        is_locked, locked_by = row[0], row[1]
        if not is_locked or not locked_by:
            return False

        owner_alive = False
        if locked_by == hostname:
            owner_alive = _local_backfill_process_running()
        elif locked_by.startswith(f"{hostname}:"):
            parts = locked_by.split(":", 2)
            if len(parts) >= 2:
                try:
                    owner_alive = _is_local_pid_running(int(parts[1]))
                except ValueError:
                    owner_alive = _local_backfill_process_running()
        else:
            return False

        if owner_alive:
            return False

        result = conn.execute(
            text(
                """
                UPDATE backfill_lock
                SET is_locked = 0, locked_at = NULL, locked_by = NULL
                WHERE id = 1 AND is_locked = 1
                """
            )
        )
        if result.rowcount > 0:
            logger.warning("Released orphaned backfill lock held by %s", locked_by)
            return True
    return False


def acquire_backfill_lock() -> bool:
    """Acquire backfill lock. Returns True if acquired, False if already locked."""
    ensure_backfill_lock_table()
    hostname = socket.gethostname()
    owner = f"{hostname}:{os.getpid()}"
    try:
        release_orphaned_backfill_lock()
    except Exception:
        logger.exception("Failed to release orphaned backfill lock")
    # Clear stale locks before attempting to acquire
    stale_hours = get_runtime_int(
        env_keys="BACKFILL_LOCK_STALE_HOURS",
        db_key="datasync.backfill_lock_stale_hours",
        default=6,
    )
    try:
        release_stale_backfill_lock(stale_hours)
    except Exception:
        logger.exception("Failed to release stale backfill lock")

    with engine_tm.begin() as conn:
        result = conn.execute(
            text("""
            UPDATE backfill_lock 
            SET is_locked = 1, 
                locked_at = CURRENT_TIMESTAMP, 
                locked_by = :owner
            WHERE id = 1 AND is_locked = 0
        """),
            {"owner": owner},
        )

        if result.rowcount > 0:
            logger.info("Acquired backfill lock (owner: %s)", owner)
            return True
        else:
            logger.warning("Failed to acquire backfill lock (already locked)")
            return False


def release_stale_backfill_lock(max_age_hours: int = 6) -> bool:
    """Release the backfill lock if it has been held longer than `max_age_hours`.

    Returns True if a stale lock was released, False otherwise.
    """
    ensure_backfill_lock_table()
    max_age_seconds = max(int(max_age_hours), 0) * 3600
    with engine_tm.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE backfill_lock
                SET is_locked = 0, locked_at = NULL, locked_by = NULL
                WHERE id = 1
                  AND is_locked = 1
                  AND locked_at IS NOT NULL
                  AND TIMESTAMPDIFF(SECOND, locked_at, CURRENT_TIMESTAMP) >= :max_age_seconds
                """
            ),
            {"max_age_seconds": max_age_seconds},
        )
        if result.rowcount > 0:
            logger.info("Released stale backfill lock (older than %d hours)", max_age_hours)
            return True
    return False


def release_backfill_lock():
    """Release backfill lock."""
    ensure_backfill_lock_table()
    with engine_tm.begin() as conn:
        conn.execute(
            text("""
            UPDATE backfill_lock 
            SET is_locked = 0, 
                locked_at = NULL, 
                locked_by = NULL
            WHERE id = 1
        """)
        )
    logger.info("Released backfill lock")


def acquire_backfill_lock_with_token(token: str) -> bool:
    """Acquire backfill lock using an owner token (host:pid:uuid).

    Returns True if acquired, False if already locked.
    """
    ensure_backfill_lock_table()
    try:
        release_orphaned_backfill_lock()
    except Exception:
        logger.exception("Failed to release orphaned backfill lock")
    stale_hours = get_runtime_int(
        env_keys="BACKFILL_LOCK_STALE_HOURS",
        db_key="datasync.backfill_lock_stale_hours",
        default=6,
    )
    try:
        release_stale_backfill_lock(stale_hours)
    except Exception:
        logger.exception("Failed to release stale backfill lock")

    with engine_tm.begin() as conn:
        result = conn.execute(
            text("""
            UPDATE backfill_lock
            SET is_locked = 1,
                locked_at = CURRENT_TIMESTAMP,
                locked_by = :token
            WHERE id = 1 AND is_locked = 0
        """),
            {"token": token},
        )
        if result.rowcount > 0:
            logger.info("Acquired backfill lock (token: %s)", token)
            return True
        else:
            logger.warning("Failed to acquire backfill lock (already locked)")
            return False


def refresh_backfill_lock(token: str) -> bool:
    """Refresh the lock timestamp while owning the lock.

    Returns True if refreshed (owner matched), False otherwise.
    """
    ensure_backfill_lock_table()
    with engine_tm.begin() as conn:
        result = conn.execute(
            text(
                """
            UPDATE backfill_lock
            SET locked_at = CURRENT_TIMESTAMP
            WHERE id = 1 AND is_locked = 1 AND locked_by = :token
            """
            ),
            {"token": token},
        )
        if result.rowcount > 0:
            logger.debug("Refreshed backfill lock for token %s", token)
            return True
        else:
            logger.warning("Failed to refresh backfill lock for token %s (owner mismatch)", token)
            return False


def release_backfill_lock_token(token: str) -> bool:
    """Release backfill lock only if owned by `token`.

    Returns True if released, False otherwise.
    """
    ensure_backfill_lock_table()
    with engine_tm.begin() as conn:
        result = conn.execute(
            text(
                """
            UPDATE backfill_lock
            SET is_locked = 0,
                locked_at = NULL,
                locked_by = NULL
            WHERE id = 1 AND locked_by = :token
            """
            ),
            {"token": token},
        )
        if result.rowcount > 0:
            logger.info("Released backfill lock for token %s", token)
            return True
        else:
            logger.warning("Did not release backfill lock for token %s (owner mismatch or not locked)", token)
            return False


def is_backfill_locked() -> bool:
    """Check if backfill is currently locked."""
    ensure_backfill_lock_table()
    try:
        release_orphaned_backfill_lock()
    except Exception:
        logger.exception("Failed to release orphaned backfill lock during lock check")
    stale_hours = get_runtime_int(
        env_keys="BACKFILL_LOCK_STALE_HOURS",
        db_key="datasync.backfill_lock_stale_hours",
        default=6,
    )
    try:
        release_stale_backfill_lock(stale_hours)
    except Exception:
        logger.exception("Failed to release stale backfill lock during lock check")
    with engine_tm.connect() as conn:
        result = conn.execute(
            text("""
            SELECT is_locked FROM backfill_lock WHERE id = 1
        """)
        )
        row = result.fetchone()
        return bool(row[0]) if row else False

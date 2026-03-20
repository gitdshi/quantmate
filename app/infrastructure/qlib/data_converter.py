"""Convert tushare/akshare MySQL data to Qlib binary format.

Qlib expects data in a specific directory structure with binary files.
This module reads OHLCV data from tushare and akshare MySQL databases
and writes it to Qlib's expected format.

Data source hierarchy:
  - tushare.stock_daily + tushare.adj_factor → primary source
  - akshare.stock_daily → fallback / supplement
  - vnpy DB is NOT used (reserved for vnpy only)
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.infrastructure.db.connections import get_tushare_engine, get_akshare_engine, get_qlib_engine
from app.infrastructure.qlib.qlib_config import QLIB_DATA_DIR

logger = logging.getLogger(__name__)


def _ts_code_to_qlib_instrument(ts_code: str) -> str:
    """Convert tushare ts_code (e.g. '000001.SZ') to Qlib instrument (e.g. 'SZ000001')."""
    if "." not in ts_code:
        return ts_code
    code, exch = ts_code.split(".", 1)
    exch_map = {"SZ": "SZ", "SH": "SH", "BJ": "BJ"}
    qlib_exch = exch_map.get(exch.upper(), exch.upper())
    return f"{qlib_exch}{code}"


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def fetch_tushare_daily(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Fetch daily OHLCV + adj_factor from tushare MySQL.

    Returns DataFrame with columns:
        instrument, date, open, high, low, close, volume, factor
    """
    engine = get_tushare_engine()
    params = {}
    where_clauses = []

    if start_date:
        where_clauses.append("d.trade_date >= :start")
        params["start"] = start_date
    if end_date:
        where_clauses.append("d.trade_date <= :end")
        params["end"] = end_date

    where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

    query = f"""
        SELECT
            d.ts_code,
            d.trade_date,
            d.open,
            d.high,
            d.low,
            d.close,
            d.vol AS volume,
            d.amount,
            COALESCE(a.adj_factor, 1.0) AS factor
        FROM stock_daily d
        LEFT JOIN adj_factor a ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date
        WHERE 1=1 {where_sql}
        ORDER BY d.ts_code, d.trade_date
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    if df.empty:
        return df

    df["instrument"] = df["ts_code"].apply(_ts_code_to_qlib_instrument)
    df = df.rename(columns={"trade_date": "date"})
    df["date"] = pd.to_datetime(df["date"])

    return df[["instrument", "date", "open", "high", "low", "close", "volume", "amount", "factor"]]


def fetch_akshare_daily(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Fetch daily OHLCV from akshare MySQL as supplement."""
    engine = get_akshare_engine()
    params = {}
    where_clauses = []

    if start_date:
        where_clauses.append("d.trade_date >= :start")
        params["start"] = start_date
    if end_date:
        where_clauses.append("d.trade_date <= :end")
        params["end"] = end_date

    where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

    query = f"""
        SELECT
            d.ts_code,
            d.trade_date,
            d.open,
            d.high,
            d.low,
            d.close,
            d.volume,
            d.amount
        FROM stock_daily d
        WHERE 1=1 {where_sql}
        ORDER BY d.ts_code, d.trade_date
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    if df.empty:
        return df

    df["instrument"] = df["ts_code"].apply(_ts_code_to_qlib_instrument)
    df = df.rename(columns={"trade_date": "date"})
    df["date"] = pd.to_datetime(df["date"])
    df["factor"] = 1.0  # akshare has no adj_factor column by default

    return df[["instrument", "date", "open", "high", "low", "close", "volume", "amount", "factor"]]


def convert_to_qlib_format(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    data_dir: Optional[str] = None,
    use_akshare_supplement: bool = False,
) -> dict:
    """Convert tushare/akshare data to Qlib binary format on disk.

    Args:
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)
        data_dir: Target directory for Qlib binary data (default: QLIB_DATA_DIR)
        use_akshare_supplement: Also include akshare data for instruments not in tushare

    Returns:
        Summary dict with instrument_count, date_range, etc.
    """
    target_dir = Path(data_dir or QLIB_DATA_DIR)
    _ensure_dir(target_dir)

    logger.info("[qlib-converter] Fetching tushare daily data...")
    df = fetch_tushare_daily(start_date, end_date)

    if use_akshare_supplement and not df.empty:
        logger.info("[qlib-converter] Fetching akshare supplement data...")
        ak_df = fetch_akshare_daily(start_date, end_date)
        if not ak_df.empty:
            # Only add instruments not already in tushare
            existing = set(df["instrument"].unique())
            ak_df = ak_df[~ak_df["instrument"].isin(existing)]
            if not ak_df.empty:
                df = pd.concat([df, ak_df], ignore_index=True)
                logger.info("[qlib-converter] Added %d akshare instruments", ak_df["instrument"].nunique())

    if df.empty:
        logger.warning("[qlib-converter] No data found to convert")
        return {"instrument_count": 0, "status": "empty"}

    instrument_count = df["instrument"].nunique()
    date_min = df["date"].min()
    date_max = df["date"].max()
    logger.info(
        "[qlib-converter] Converting %d instruments, date range %s to %s",
        instrument_count,
        date_min.date(),
        date_max.date(),
    )

    # Write Qlib-format binary data
    # Qlib expects: <data_dir>/features/<instrument>/<feature>.day.bin
    # and <data_dir>/calendars/day.txt, <data_dir>/instruments/all.txt
    features_dir = target_dir / "features"
    calendars_dir = target_dir / "calendars"
    instruments_dir = target_dir / "instruments"
    _ensure_dir(features_dir)
    _ensure_dir(calendars_dir)
    _ensure_dir(instruments_dir)

    # Write calendar
    all_dates = sorted(df["date"].unique())
    with open(calendars_dir / "day.txt", "w") as f:
        for dt in all_dates:
            f.write(pd.Timestamp(dt).strftime("%Y-%m-%d") + "\n")

    # Write instruments file
    instruments_data = []
    feature_cols = ["open", "high", "low", "close", "volume", "amount", "factor"]

    for instrument, idf in df.groupby("instrument"):
        idf = idf.sort_values("date")
        inst_start = idf["date"].iloc[0].strftime("%Y-%m-%d")
        inst_end = idf["date"].iloc[-1].strftime("%Y-%m-%d")
        instruments_data.append(f"{instrument}\t{inst_start}\t{inst_end}")

        # Write feature binary files
        inst_dir = features_dir / str(instrument)
        _ensure_dir(inst_dir)

        for col in feature_cols:
            values = idf[col].values.astype(np.float32)
            values.tofile(str(inst_dir / f"{col}.day.bin"))

    with open(instruments_dir / "all.txt", "w") as f:
        f.write("\n".join(instruments_data) + "\n")

    # Log conversion to qlib DB
    _log_conversion(
        source_db="tushare",
        source_table="stock_daily",
        instrument_count=instrument_count,
        date_start=date_min.date() if hasattr(date_min, "date") else date_min,
        date_end=date_max.date() if hasattr(date_max, "date") else date_max,
    )

    logger.info("[qlib-converter] Conversion complete: %d instruments written to %s", instrument_count, target_dir)

    return {
        "instrument_count": instrument_count,
        "date_range_start": str(date_min.date() if hasattr(date_min, "date") else date_min),
        "date_range_end": str(date_max.date() if hasattr(date_max, "date") else date_max),
        "data_dir": str(target_dir),
        "status": "completed",
    }


def _log_conversion(
    source_db: str,
    source_table: str,
    instrument_count: int,
    date_start: date,
    date_end: date,
) -> None:
    """Record conversion run in qlib.data_conversion_log."""
    try:
        engine = get_qlib_engine()
        with engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO data_conversion_log "
                    "(source_db, source_table, instrument_count, date_range_start, date_range_end, status, completed_at) "
                    "VALUES (:src_db, :src_table, :cnt, :ds, :de, 'completed', NOW())"
                ),
                {
                    "src_db": source_db,
                    "src_table": source_table,
                    "cnt": instrument_count,
                    "ds": date_start,
                    "de": date_end,
                },
            )
            conn.commit()
    except Exception as exc:
        logger.warning("[qlib-converter] Failed to log conversion: %s", exc)

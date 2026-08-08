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
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.infrastructure.db.connections import connection as db_connection, get_qlib_engine
from app.infrastructure.qlib.qlib_config import QLIB_DATA_DIR

logger = logging.getLogger(__name__)


def _qlib_calendar_uses_timestamp() -> bool:
    """Return True if the installed pyqlib expects Unix-timestamp calendars.

    pyqlib built from the microsoft/qlib GitHub source (v0.9.7 tag) reads
    ``day.txt`` as ``YYYY-MM-DD`` strings via ``pd.Timestamp(x)``. Pre-built
    wheels from PyPI (>= 0.9.5) use integer Unix timestamps. If pyqlib is not
    installed we default to the string format which is safer.
    """
    try:
        import qlib  # noqa: F401
        from packaging import version

        # Check if this is a source build (has no PyPI metadata) vs a wheel
        # The source build from v0.9.7 tag uses string calendars
        v = version.parse(qlib.__version__)
        if v >= version.parse("0.9.5"):
            # Try to detect source build vs wheel by checking if the package
            # has dist-info (wheel) or not (source install)
            import importlib.util
            spec = importlib.util.find_spec("qlib")
            if spec and spec.submodule_search_locations:
                import os
                pkg_dir = os.path.dirname(spec.origin)
                parent = os.path.dirname(pkg_dir)
                # Source installs don't have qlib-*.dist-info
                has_dist_info = any(
                    d.startswith("qlib-") and d.endswith(".dist-info")
                    for d in os.listdir(parent)
                )
                return has_dist_info
        return False
    except Exception:
        # Conservative default: write date strings.
        return False


def _write_calendar(calendars_dir: Path, calendar_dates: List[pd.Timestamp]) -> str:
    """Write ``calendars/day.txt`` in the format expected by the installed pyqlib.

    Returns the format used: ``"timestamp"`` or ``"string"``.
    """
    calendars_dir.mkdir(parents=True, exist_ok=True)

    if _qlib_calendar_uses_timestamp():
        # Modern pyqlib: integer Unix timestamps (seconds).
        ts_values = pd.DatetimeIndex(calendar_dates).astype("int64") // 10**9
        with open(calendars_dir / "day.txt", "w") as f:
            for ts in ts_values:
                f.write(f"{int(ts)}\n")
        return "timestamp"

    # Legacy pyqlib: ``YYYY-MM-DD`` strings.
    with open(calendars_dir / "day.txt", "w") as f:
        for dt in calendar_dates:
            f.write(pd.Timestamp(dt).strftime("%Y-%m-%d") + "\n")
    return "string"


def _fetch_universe_data() -> Dict[str, List[Tuple[str, str, str]]]:
    """Query tushare for instrument universes (all + index constituents).

    Returns ``{market_name: [(instrument, start, end), ...]}`` where
    instruments are converted to the Qlib naming convention (e.g. ``SZ000001``).
    """
    universes: Dict[str, List[Tuple[str, str, str]]] = {}

    try:
        with connection("tushare") as conn:
            # All listed A-shares
            try:
                rows = conn.execute(
                    text(
                        "SELECT ts_code, list_date, delist_date "
                        "FROM stock_basic WHERE list_status = 'L'"
                    )
                ).fetchall()
                universes["all"] = [
                    (
                        _ts_code_to_qlib_instrument(r[0]),
                        str(r[1]) if r[1] else "2010-01-01",
                        str(r[2]) if r[2] else "2099-12-31",
                    )
                    for r in rows
                ]
            except Exception as exc:
                logger.warning("[qlib-converter] Failed to fetch stock_basic: %s", exc)

            # Index constituents: csi300 / csi500 / csi1000
            index_map = {
                "000300.SH": "csi300",
                "000905.SH": "csi500",
                "000852.SH": "csi1000",
            }
            for index_code, market_name in index_map.items():
                try:
                    rows = conn.execute(
                        text(
                            "SELECT con_code, MIN(trade_date) AS in_date, MAX(trade_date) AS out_date "
                            "FROM index_weight WHERE index_code = :code "
                            "GROUP BY con_code"
                        ),
                        {"code": index_code},
                    ).fetchall()
                    universes[market_name] = [
                        (
                            _ts_code_to_qlib_instrument(r[0]),
                            str(r[1]) if r[1] else "2010-01-01",
                            str(r[2]) if r[2] else "2099-12-31",
                        )
                        for r in rows
                    ]
                except Exception as exc:
                    logger.warning(
                        "[qlib-converter] Failed to fetch %s constituents: %s", market_name, exc
                    )
    except Exception as exc:
        logger.warning("[qlib-converter] Failed to fetch universe data: %s", exc)

    return universes


def _write_instruments(
    instruments_dir: Path,
    universe_data: Dict[str, List[Tuple[str, str, str]]],
) -> None:
    """Write one ``<market>.txt`` file per entry in ``universe_data``."""
    instruments_dir.mkdir(parents=True, exist_ok=True)
    for market, instruments in universe_data.items():
        if not instruments:
            continue
        file_path = instruments_dir / f"{market}.txt"
        with open(file_path, "w") as f:
            for code, start, end in instruments:
                f.write(f"{code}\t{start}\t{end}\n")
        logger.info("[qlib-converter] Wrote %d instruments to %s", len(instruments), file_path)


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


@contextmanager
def connection(source: str):
    """Yield a DB connection for the requested source.

    This wrapper keeps the module easy to patch in unit tests.
    """
    with db_connection(source) as conn:
        yield conn


def _normalize_daily_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw SQL results into the standard Qlib input shape."""
    if df.empty:
        return df

    normalized = df.copy()
    if "instrument" not in normalized.columns:
        code_col = (
            "ts_code" if "ts_code" in normalized.columns else "symbol" if "symbol" in normalized.columns else None
        )
        if code_col is None:
            raise KeyError("instrument")
        normalized["instrument"] = normalized[code_col].apply(_ts_code_to_qlib_instrument)

    if "date" not in normalized.columns:
        date_col = "trade_date" if "trade_date" in normalized.columns else None
        if date_col is None:
            raise KeyError("date")
        normalized = normalized.rename(columns={date_col: "date"})

    if "volume" not in normalized.columns and "vol" in normalized.columns:
        normalized = normalized.rename(columns={"vol": "volume"})
    if "factor" not in normalized.columns:
        if "adj_factor" in normalized.columns:
            normalized = normalized.rename(columns={"adj_factor": "factor"})
        else:
            normalized["factor"] = 1.0
    if "amount" not in normalized.columns:
        normalized["amount"] = 0.0

    normalized["date"] = pd.to_datetime(normalized["date"])
    return normalized[["instrument", "date", "open", "high", "low", "close", "volume", "amount", "factor"]]


def fetch_tushare_daily(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Fetch daily OHLCV + adj_factor from tushare MySQL.

    Returns DataFrame with columns:
        instrument, date, open, high, low, close, volume, factor
    """
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

    with connection("tushare") as conn:
        df = pd.read_sql(text(query), conn, params=params)

    return _normalize_daily_dataframe(df)


def fetch_akshare_daily(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Fetch daily OHLCV from akshare MySQL as supplement."""
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

    with connection("akshare") as conn:
        df = pd.read_sql(text(query), conn, params=params)

    return _normalize_daily_dataframe(df)


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
    df = _normalize_daily_dataframe(fetch_tushare_daily(start_date, end_date))

    if use_akshare_supplement and not df.empty:
        logger.info("[qlib-converter] Fetching akshare supplement data...")
        ak_df = fetch_akshare_daily(start_date, end_date)
        if not ak_df.empty:
            ak_df = _normalize_daily_dataframe(ak_df)
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

    # Write calendar in the format expected by the installed pyqlib version
    all_dates = sorted(df["date"].unique())
    calendar_format = _write_calendar(calendars_dir, all_dates)

    # Write instruments file(s): all + csi300/csi500/csi1000 (when available)
    universe_data = _fetch_universe_data()

    # Always include the instruments we actually have data for in "all",
    # preferring tushare's full list when present but falling back to the
    # data-derived list.
    data_instruments: Dict[str, Tuple[str, str]] = {}
    for instrument, idf in df.groupby("instrument"):
        idf = idf.sort_values("date")
        inst_start = idf["date"].iloc[0].strftime("%Y-%m-%d")
        inst_end = idf["date"].iloc[-1].strftime("%Y-%m-%d")
        data_instruments[instrument] = (inst_start, inst_end)

    if not universe_data.get("all"):
        universe_data["all"] = [
            (inst, start, end) for inst, (start, end) in data_instruments.items()
        ]
    else:
        # Restrict "all" to instruments we actually have features for
        universe_data["all"] = [
            (inst, start, end)
            for inst, start, end in universe_data["all"]
            if inst in data_instruments
        ]
        if not universe_data["all"]:
            universe_data["all"] = [
                (inst, start, end) for inst, (start, end) in data_instruments.items()
            ]

    # For index universes, also restrict to instruments we have data for
    for market in ("csi300", "csi500", "csi1000"):
        if market in universe_data:
            universe_data[market] = [
                (inst, start, end)
                for inst, start, end in universe_data[market]
                if inst in data_instruments
            ]

    _write_instruments(instruments_dir, universe_data)

    # Write feature binary files
    # Qlib's FileFeatureStorage expects each bin file to contain:
    #   [start_index (float32), value_0, value_1, ..., value_N]
    # where start_index is the calendar position of the first value.
    # Instrument directory names MUST be lowercase (qlib lowercases
    # instrument names when constructing file paths).
    feature_cols = ["open", "high", "low", "close", "volume", "amount", "factor"]
    calendar_dates = sorted(df["date"].unique())
    calendar_index = pd.DatetimeIndex(calendar_dates)
    cal_pos = {d: i for i, d in enumerate(calendar_index)}

    for instrument, idf in df.groupby("instrument"):
        idf = idf.sort_values("date")
        # Use lowercase instrument name for the directory
        inst_dir = features_dir / str(instrument).lower()
        _ensure_dir(inst_dir)

        # Find the calendar positions where this instrument has data
        inst_dates = idf["date"].values
        positions = sorted(cal_pos[d] for d in inst_dates if d in cal_pos)
        if not positions:
            continue
        start_idx = positions[0]
        end_idx = positions[-1]
        n_values = end_idx - start_idx + 1

        # Reindex data to the calendar range [start_idx, end_idx]
        idf_indexed = idf.set_index("date")
        for col in feature_cols:
            values = np.full(n_values, np.nan, dtype=np.float32)
            for pos in positions:
                dt = calendar_index[pos]
                if dt in idf_indexed.index:
                    values[pos - start_idx] = idf_indexed.loc[dt, col]

            # Write: [start_index (as float32), values...]
            header = np.array([start_idx], dtype=np.float32)
            out = np.hstack([header, values])
            out.tofile(str(inst_dir / f"{col}.day.bin"))

    # Log conversion to qlib DB
    _log_conversion(
        source_db="tushare",
        source_table="stock_daily",
        instrument_count=instrument_count,
        date_start=date_min.date() if hasattr(date_min, "date") else date_min,
        date_end=date_max.date() if hasattr(date_max, "date") else date_max,
    )

    logger.info(
        "[qlib-converter] Conversion complete: %d instruments written to %s (calendar=%s, markets=%s)",
        instrument_count,
        target_dir,
        calendar_format,
        list(universe_data.keys()),
    )

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

"""Background Tasks for RQ Workers."""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import traceback
import numpy as np
import logging

logger = logging.getLogger(__name__)

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rq import get_current_job
from vnpy.trader.constant import Interval
from vnpy.trader.optimize import OptimizationSetting
from vnpy.trader.setting import SETTINGS as VNPY_SETTINGS

# Configure vn.py DB backend before any code path can initialize the global database singleton.
# This must run at import time in the worker process, otherwise vn.py may cache the SQLite backend.


def _configure_vnpy_mysql_from_env() -> None:
    """Force vn.py to use MySQL (vnpy DB) based on the application's MYSQL_* env vars."""
    host = os.getenv("MYSQL_HOST")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    port = int(os.getenv("MYSQL_PORT", "3306"))

    if not host or not user or not password:
        logger.warning("[worker] VNPy MySQL config skipped: missing MYSQL_* env")
        return

    VNPY_SETTINGS["database.name"] = "mysql"
    VNPY_SETTINGS["database.host"] = host
    VNPY_SETTINGS["database.port"] = port
    VNPY_SETTINGS["database.user"] = user
    VNPY_SETTINGS["database.password"] = password
    VNPY_SETTINGS["database.database"] = "vnpy"

    logger.info("[worker] VNPy DB backend set to mysql %s:%s db=vnpy user=%s", host, port, user)


_configure_vnpy_mysql_from_env()

from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode, evaluate

# empyrical (used by vnpy_ctastrategy) still references np.NINF, removed in NumPy 2.0.
# Patch for runtime compatibility while keeping vn.py's numpy>=2 requirement.
import numpy as _np

if not hasattr(_np, "NINF"):
    _np.NINF = -_np.inf

from app.api.services.strategy_service import compile_strategy
from app.api.services.job_storage_service import get_job_storage

from app.datasync.service.vnpy_ingest import (
    get_symbol as get_ts_symbol,
    map_exchange as map_ts_exchange,
    sync_symbol_to_vnpy,
    update_bar_overview,
)
from app.domains.market.service import MarketService
from app.domains.backtests.dao.akshare_benchmark_dao import AkshareBenchmarkDao
from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
from app.domains.backtests.dao.bulk_backtest_dao import BulkBacktestDao
from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao
from app.domains.system.dao.optimization_dao import OptimizationTaskDao


def convert_to_vnpy_symbol(symbol: str) -> str:
    """Convert a symbol into the VNPy vt_symbol format.

    Supports both:
    - Tushare-style: 000001.SZ / 600000.SH / 430047.BJ
    - VNPy-style:    000001.SZSE / 600000.SSE / 430047.BSE

    Important: If the exchange is already a VNPy exchange (SZSE/SSE/BSE), do
    NOT append it again.
    """
    if not symbol or "." not in symbol:
        return symbol

    code, exch = symbol.rsplit(".", 1)
    exch_upper = exch.upper()

    # Already VNPy-style
    if exch_upper in {"SZSE", "SSE", "BSE"}:
        return f"{code}.{exch_upper}"

    exchange_map = {
        "SZ": "SZSE",  # Shenzhen Stock Exchange
        "SH": "SSE",  # Shanghai Stock Exchange
        "BJ": "BSE",  # Beijing Stock Exchange
    }
    vnpy_exchange = exchange_map.get(exch_upper, exch_upper)
    return f"{code}.{vnpy_exchange}"


def convert_to_tushare_symbol(symbol: str) -> str:
    """Convert a symbol into Tushare ts_code format when possible."""
    if not symbol or "." not in symbol:
        return symbol

    code, exch = symbol.rsplit(".", 1)
    exch_upper = exch.upper()
    suffix_map = {
        "SZSE": "SZ",
        "SSE": "SH",
        "BSE": "BJ",
        "SZ": "SZ",
        "SH": "SH",
        "BJ": "BJ",
    }
    return f"{code}.{suffix_map.get(exch_upper, exch_upper)}"


def ensure_vnpy_history_data(symbol: str, start_date: str) -> int:
    """Backfill the requested symbol into vn.py DB from Tushare on demand."""
    ts_code = convert_to_tushare_symbol(symbol)
    if not ts_code or "." not in ts_code:
        return 0

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if isinstance(start_date, str) else start_date
    synced = sync_symbol_to_vnpy(ts_code, start_date=start_dt)
    if synced > 0:
        update_bar_overview(get_ts_symbol(ts_code), map_ts_exchange(ts_code))
        logger.info("[worker] Backfilled %s bars for %s into vnpy DB", synced, ts_code)
    else:
        logger.warning("[worker] No vnpy bars backfilled for %s from %s", symbol, ts_code)
    return synced


def resolve_symbol_name(input_symbol: str) -> str:
    """Resolve a human-readable symbol name.

    Delegates to the Market domain (DAO-backed). Kept as a thin wrapper for
    backward compatibility inside worker/tasks.
    """
    try:
        return MarketService().resolve_symbol_name(input_symbol)
    except Exception:
        return ""


def get_benchmark_data_for_worker(
    start_date: str, end_date: str, benchmark_symbol: str = "399300.SZ"
) -> Optional[Dict]:
    """
    Fetch HS300 benchmark data for the given period (worker version).
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if isinstance(start_date, str) else start_date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if isinstance(end_date, str) else end_date
        return AkshareBenchmarkDao().get_benchmark_data(start=start_dt, end=end_dt, benchmark_symbol=benchmark_symbol)
    except Exception as e:
        logger.exception("[worker] Error fetching benchmark data: %s", e)
        return None


def calculate_alpha_beta_for_worker(strategy_returns: np.ndarray, benchmark_returns: np.ndarray) -> tuple:
    """Calculate alpha and beta using linear regression."""
    if len(strategy_returns) < 2 or len(benchmark_returns) < 2:
        return None, None

    min_len = min(len(strategy_returns), len(benchmark_returns))
    strategy_returns = strategy_returns[:min_len]
    benchmark_returns = benchmark_returns[:min_len]

    mask = ~(np.isnan(strategy_returns) | np.isnan(benchmark_returns))
    strategy_returns = strategy_returns[mask]
    benchmark_returns = benchmark_returns[mask]

    if len(strategy_returns) < 2:
        return None, None

    try:
        beta, alpha = np.polyfit(benchmark_returns, strategy_returns, 1)
        alpha_annualized = alpha * 252
        return float(alpha_annualized), float(beta)
    except Exception:
        return None, None


def save_backtest_to_db(
    job_id: str,
    user_id: int,
    strategy_id: Optional[int],
    strategy_class: str,
    symbol: str,
    start_date: str,
    end_date: str,
    parameters: Dict,
    status: str,
    result: Dict,
    error: str = None,
    strategy_version: int = None,
):
    """Save backtest result to database for permanent storage."""
    try:
        now = datetime.utcnow()
        BacktestHistoryDao().upsert_history(
            user_id=user_id,
            job_id=job_id,
            strategy_id=strategy_id,
            strategy_class=strategy_class,
            strategy_version=strategy_version,
            vt_symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            parameters=parameters or {},
            status=status,
            result=result,
            error=error,
            created_at=now,
            completed_at=now if status in ["completed", "failed"] else None,
        )
        logger.info("[worker] Saved backtest %s to database", job_id)
    except Exception as e:
        logger.exception("[worker] Error saving backtest to database: %s", e)


def run_backtest_task(
    strategy_code: Optional[str],
    strategy_class_name: str,
    symbol: str,
    start_date: str,
    end_date: str,
    initial_capital: float,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    parameters: Optional[Dict[str, Any]] = None,
    benchmark: str = "399300.SZ",
    user_id: int = None,
    strategy_id: int = None,
) -> Dict[str, Any]:
    """
    Run backtest task in background worker.

    Args:
        strategy_code: Custom strategy code (if not builtin)
        strategy_class_name: Name of strategy class
        symbol: Trading symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        initial_capital: Initial capital
        rate: Commission rate
        slippage: Slippage
        size: Contract size
        pricetick: Price tick
        parameters: Strategy parameters
        benchmark: Benchmark symbol (default: 399300.SZ for HS300)
        user_id: User ID for DB storage
        strategy_id: Strategy ID for DB storage

    Returns:
        Dict with backtest results
    """
    # Get job_id from RQ context (RQ uses job_id kwarg as the actual job ID)
    current_job = get_current_job()
    job_id = current_job.id if current_job else None

    try:
        strategy_settings: Dict[str, Any] = {}

        # Ensure vn.py reads bars from MySQL(vnpy) instead of default sqlite.
        _configure_vnpy_mysql_from_env()

        # Convert symbol to VNPy format
        vnpy_symbol = convert_to_vnpy_symbol(symbol)

        logger.info("[worker] Starting backtest job %s", job_id)
        logger.info("[worker] Strategy=%s Symbol=%s -> %s", strategy_class_name, symbol, vnpy_symbol)

        # Load strategy class
        # Jobs MUST include the strategy. If `strategy_code` is provided, compile it.
        # Otherwise attempt to load the strategy source from the quantmate `strategies`
        # table (by `strategy_id` or `strategy_class_name`). Do NOT fall back to
        # embedded/builtin VNpy strategy classes here.
        strategy_class = None
        if strategy_code:
            # Compile custom strategy provided with the job
            strategy_class = compile_strategy(strategy_code, strategy_class_name)
        else:
            # Attempt to load strategy source from quantmate DB (DAO-backed)
            source_dao = StrategySourceDao()
            if strategy_id is not None and user_id is not None:
                strategy_code_db, strategy_class_name_db, _sv = source_dao.get_strategy_source_for_user(
                    strategy_id, user_id
                )
                # Prefer class name from DB if provided
                if strategy_class_name_db:
                    strategy_class_name = strategy_class_name_db
                strategy_class = compile_strategy(strategy_code_db, strategy_class_name)
            elif strategy_class_name:
                strategy_code_db = source_dao.get_strategy_code_by_class_name(strategy_class_name)
                strategy_class = compile_strategy(strategy_code_db, strategy_class_name)
            else:
                raise ValueError(
                    "No strategy code provided and no matching strategy found in database; jobs must include `strategy_code` or a valid `strategy_id`"
                )

        # Initialize backtest engine
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        engine = BacktestingEngine()
        engine.set_parameters(
            vt_symbol=vnpy_symbol,
            interval=Interval.DAILY,
            start=start_dt,
            end=end_dt,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=initial_capital,
        )

        # Ensure a valid strategy class was compiled/loaded
        if not strategy_class:
            raise RuntimeError(f"Strategy class '{strategy_class_name}' not loaded or compiled successfully")

        if hasattr(strategy_class, "get_class_parameters"):
            try:
                strategy_settings = strategy_class.get_class_parameters() or {}
            except Exception:
                logger.exception("[worker] Failed to read default parameters for %s", strategy_class_name)
                strategy_settings = {}
        if parameters:
            strategy_settings.update(parameters)

        # Add strategy
        engine.add_strategy(strategy_class, strategy_settings)

        # Load data
        logger.info("[worker] Loading data for %s...", symbol)
        engine.load_data()
        if not engine.history_data:
            ensure_vnpy_history_data(vnpy_symbol, start_date)
            engine.history_data = []
            engine.load_data()
        if not engine.history_data:
            raise RuntimeError(f"No historical bar data found for {symbol} between {start_date} and {end_date}")

        # Run backtest
        logger.info("[worker] Running backtest...")
        engine.run_backtesting()

        # Calculate results
        logger.info("[worker] Calculating results...")
        df = engine.calculate_result()
        statistics = engine.calculate_statistics()

        # Build equity curve data for charts
        equity_curve = None
        strategy_daily_returns = None
        if df is not None and not df.empty and "balance" in df.columns:
            # Convert DataFrame index (datetime) to ISO strings for JSON serialization
            equity_data = []
            for idx, row in df.iterrows():
                dt_str = idx.isoformat() if hasattr(idx, "isoformat") else str(idx)
                equity_data.append(
                    {"datetime": dt_str, "balance": float(row["balance"]), "net_pnl": float(row.get("net_pnl", 0))}
                )
            equity_curve = equity_data

            # Calculate daily returns for alpha/beta
            balance_values = df["balance"].values
            if len(balance_values) > 1:
                strategy_daily_returns = np.diff(balance_values) / balance_values[:-1]

        # Calculate alpha and beta against benchmark
        alpha = None
        beta = None
        benchmark_return = None
        benchmark_data = get_benchmark_data_for_worker(start_date, end_date, benchmark)
        if benchmark_data and strategy_daily_returns is not None:
            alpha, beta = calculate_alpha_beta_for_worker(strategy_daily_returns, benchmark_data["returns"])
            benchmark_return = benchmark_data["total_return"]

        # Build trade list
        trades = []
        if engine.trades:
            for t in list(engine.trades.values())[:100]:
                trades.append(
                    {
                        "datetime": t.datetime.isoformat() if t.datetime else None,
                        "symbol": t.symbol,
                        "direction": str(t.direction.value) if hasattr(t.direction, "value") else str(t.direction),
                        "offset": str(t.offset.value) if hasattr(t.offset, "value") else str(t.offset),
                        "price": float(t.price),
                        "volume": float(t.volume),
                    }
                )

        # Build stock price curve from historical data
        stock_price_curve = []
        if engine.history_data:
            for bar in engine.history_data:
                stock_price_curve.append(
                    {
                        "datetime": bar.datetime.isoformat() if bar.datetime else None,
                        "open": float(bar.open_price),
                        "high": float(bar.high_price),
                        "low": float(bar.low_price),
                        "close": float(bar.close_price),
                    }
                )

        # Add benchmark curve if available
        benchmark_curve = None
        if benchmark_data and "prices" in benchmark_data:
            benchmark_curve = benchmark_data["prices"]

        # Build result with all metrics
        # Resolve human-readable symbol name from tushare.stock_basic
        symbol_name = resolve_symbol_name(symbol) or resolve_symbol_name(vnpy_symbol)

        result = {
            "job_id": job_id,
            "status": "completed",
            "symbol": symbol,
            "symbol_name": symbol_name,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "benchmark": benchmark,
            "parameters": strategy_settings,
            "statistics": {
                "total_return": float(statistics.get("total_return", 0)),
                "annual_return": float(statistics.get("annual_return", 0)),
                "max_drawdown": float(statistics.get("max_drawdown", 0)),
                "max_drawdown_percent": float(statistics.get("max_ddpercent", 0)),
                "sharpe_ratio": float(statistics.get("sharpe_ratio", 0)),
                "total_trades": int(statistics.get("total_trade_count", 0)),
                "winning_rate": float(statistics.get("winning_rate", 0)),
                "profit_factor": float(statistics.get("profit_factor", 0)),
                "total_days": int(statistics.get("total_days", 0)),
                "profit_days": int(statistics.get("profit_days", 0)),
                "loss_days": int(statistics.get("loss_days", 0)),
                "end_balance": float(statistics.get("end_balance", 0)),
                # Benchmark comparison
                "alpha": alpha,
                "beta": beta,
                "benchmark_return": benchmark_return,
                "benchmark_symbol": benchmark,
            },
            "equity_curve": equity_curve,
            "trades": trades,
            "stock_price_curve": stock_price_curve,
            "benchmark_curve": benchmark_curve,
            "completed_at": datetime.now().isoformat(),
        }

        # Save to database for permanent storage
        if user_id:
            # Read strategy_version from job metadata (set by submit_backtest)
            _strategy_version = None
            try:
                _meta = get_job_storage().get_job_metadata(job_id)
                if _meta:
                    _sv = _meta.get("strategy_version")
                    _strategy_version = int(_sv) if _sv is not None else None
            except Exception:
                pass
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_class=strategy_class_name,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                parameters=strategy_settings,
                status="completed",
                result=result,
                strategy_version=_strategy_version,
            )

        # Update job storage for API status tracking
        job_storage = get_job_storage()
        job_storage.update_job_status(job_id, "finished")
        job_storage.save_result(job_id, result)

        logger.info("[worker] Backtest job %s completed successfully", job_id)
        return result

    except Exception as e:
        error_msg = f"Backtest failed: {str(e)}"
        logger.exception("[worker] %s", error_msg)

        # Update job storage with error
        try:
            job_storage = get_job_storage()
            job_storage.update_job_status(job_id, "failed", error=error_msg)
        except Exception:
            pass

        # Save failed job to database
        if user_id:
            # Read strategy_version from job metadata (set by submit_backtest)
            _strategy_version = None
            try:
                _meta = get_job_storage().get_job_metadata(job_id)
                if _meta:
                    _sv = _meta.get("strategy_version")
                    _strategy_version = int(_sv) if _sv is not None else None
            except Exception:
                pass
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_class=strategy_class_name,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                parameters=parameters or {},
                status="failed",
                result=None,
                error=error_msg,
                strategy_version=_strategy_version,
            )

        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }


def run_bulk_backtest_task(
    strategy_code: Optional[str],
    strategy_class_name: str,
    symbols: list[str],
    start_date: str,
    end_date: str,
    initial_capital: float,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    parameters: Optional[Dict[str, Any]] = None,
    benchmark: str = "399300.SZ",
    bulk_job_id: str = None,
    user_id: int = None,
    strategy_id: int = None,
) -> Dict[str, Any]:
    """
    Run bulk backtest task – one strategy against many symbols sequentially.
    Each child result is saved to backtest_history with bulk_job_id.
    Progress is reported after every symbol completes.

    Note: We use `bulk_job_id` instead of `job_id` because RQ intercepts
    `job_id` as a special enqueue parameter and does not forward it to
    the function. The actual RQ job ID is retrieved via get_current_job().
    """
    # Resolve the actual RQ job ID (should match bulk_job_id)
    current_job = get_current_job()
    job_id = current_job.id if current_job else bulk_job_id

    job_storage = get_job_storage()
    total = len(symbols)
    successful = 0
    failed_count = 0
    best_return = None
    best_symbol = None
    best_symbol_name = None

    try:
        logger.info("[worker] Starting bulk backtest job %s", job_id)
        logger.info("[worker] Strategy=%s Symbols=%s", strategy_class_name, total)
        job_storage.update_job_status(job_id, "started")

        # Read strategy_version from parent metadata
        _strategy_version = None
        try:
            _meta = job_storage.get_job_metadata(job_id)
            if _meta:
                sv = _meta.get("strategy_version")
                _strategy_version = int(sv) if sv is not None else None
        except Exception:
            pass

        for idx, symbol in enumerate(symbols):
            child_job_id = f"{job_id}__{symbol}"
            try:
                logger.info("[worker] [%s/%s] Processing %s...", idx + 1, total, symbol)
                # Pass user_id=None so run_backtest_task does NOT save its own DB row;
                # we save the child row ourselves via _save_bulk_child with the bulk_job_id link.
                result = run_backtest_task(
                    strategy_code=strategy_code,
                    strategy_class_name=strategy_class_name,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    initial_capital=initial_capital,
                    rate=rate,
                    slippage=slippage,
                    size=size,
                    pricetick=pricetick,
                    parameters=parameters,
                    benchmark=benchmark,
                    user_id=None,
                    strategy_id=strategy_id,
                )

                child_status = result.get("status", "failed")
                if child_status == "completed":
                    successful += 1
                    ret = result.get("statistics", {}).get("total_return")
                    if ret is not None and (best_return is None or ret > best_return):
                        best_return = ret
                        best_symbol = symbol
                        # capture human-readable symbol name from child result
                        try:
                            best_symbol_name = result.get("symbol_name") or None
                        except Exception:
                            best_symbol_name = None
                else:
                    failed_count += 1

                # Save child to DB with bulk_job_id link
                _save_bulk_child(
                    child_job_id,
                    job_id,
                    user_id,
                    strategy_id,
                    strategy_class_name,
                    _strategy_version,
                    symbol,
                    start_date,
                    end_date,
                    parameters,
                    child_status,
                    result,
                    result.get("error"),
                )

            except Exception as e:
                logger.exception("[worker] Error processing %s: %s", symbol, e)
                failed_count += 1
                _save_bulk_child(
                    child_job_id,
                    job_id,
                    user_id,
                    strategy_id,
                    strategy_class_name,
                    _strategy_version,
                    symbol,
                    start_date,
                    end_date,
                    parameters,
                    "failed",
                    None,
                    str(e),
                )

            # Update progress
            completed = idx + 1
            pct = int(completed / total * 100)
            job_storage.update_progress(job_id, pct, f"{completed}/{total} symbols done")

            # Update bulk_backtest row (include best symbol name when available)
            _update_bulk_row(job_id, completed, best_return, best_symbol, best_symbol_name)

        # Final result summary stored in Redis for the parent job
        summary = {
            "job_id": job_id,
            "status": "completed",
            "total_symbols": total,
            "successful": successful,
            "failed": failed_count,
            "best_return": best_return,
            "best_symbol": best_symbol,
            "best_symbol_name": best_symbol_name,
            "parameters": parameters or {},
            "completed_at": datetime.now().isoformat(),
        }
        job_storage.update_job_status(job_id, "finished")
        job_storage.save_result(job_id, summary)

        # Mark DB row completed
        _finish_bulk_row(job_id, "completed", best_return, best_symbol, best_symbol_name, total)

        logger.info("[worker] Bulk backtest %s done: %s ok, %s failed", job_id, successful, failed_count)
        return summary

    except Exception as e:
        error_msg = f"Bulk backtest failed: {str(e)}"
        logger.exception("[worker] %s", error_msg)
        try:
            job_storage.update_job_status(job_id, "failed", error=error_msg)
        except Exception:
            pass
        if job_id:
            _finish_bulk_row(job_id, "failed", best_return, best_symbol, best_symbol_name, successful + failed_count)
        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }


# ---------- helpers for bulk backtest DB persistence ----------


def _save_bulk_child(
    child_job_id,
    bulk_job_id,
    user_id,
    strategy_id,
    strategy_class,
    strategy_version,
    symbol,
    start_date,
    end_date,
    parameters,
    status,
    result,
    error=None,
):
    """Insert a child backtest row linked to a bulk job."""
    try:
        now = datetime.utcnow()
        BacktestHistoryDao().upsert_history(
            user_id=user_id,
            job_id=child_job_id,
            bulk_job_id=bulk_job_id,
            strategy_id=strategy_id,
            strategy_class=strategy_class,
            strategy_version=strategy_version,
            vt_symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            parameters=parameters or {},
            status=status,
            result=result,
            error=error,
            created_at=now,
            completed_at=now if status in ("completed", "failed") else None,
        )
    except Exception as e:
        logger.exception("[worker] Error saving bulk child %s: %s", child_job_id, e)


def _update_bulk_row(job_id, completed_count, best_return, best_symbol, best_symbol_name=None):
    """Incremental update of the bulk_backtest row."""
    try:
        BulkBacktestDao().update_progress(job_id, completed_count, best_return, best_symbol, best_symbol_name)
    except Exception as e:
        logger.exception("[worker] Error updating bulk row: %s", e)


def _finish_bulk_row(job_id, status, best_return, best_symbol, best_symbol_name, completed_count):
    """Mark bulk_backtest row as completed/failed."""
    try:
        BulkBacktestDao().finish(
            job_id,
            status,
            datetime.utcnow(),
            completed_count,
            best_return,
            best_symbol,
            best_symbol_name,
        )
    except Exception as e:
        logger.exception("[worker] Error finishing bulk row: %s", e)


def _build_optimization_setting(
    param_space: Dict[str, Any], objective_metric: str = "sharpe_ratio"
) -> OptimizationSetting:
    """Convert param range payload to vn.py OptimizationSetting."""
    setting = OptimizationSetting()
    setting.set_target(objective_metric or "sharpe_ratio")

    for name, config in (param_space or {}).items():
        if not isinstance(name, str) or not name:
            continue

        if isinstance(config, (int, float)):
            setting.add_parameter(name, float(config))
            continue

        if not isinstance(config, dict):
            continue

        min_value = config.get("min")
        max_value = config.get("max")
        step_value = config.get("step")

        try:
            start = float(min_value)
            end = float(max_value)
            step = float(step_value)
        except (TypeError, ValueError):
            continue

        if not np.isfinite(start) or not np.isfinite(end) or not np.isfinite(step):
            continue
        if step <= 0:
            continue

        if end <= start:
            setting.add_parameter(name, start)
        else:
            setting.add_parameter(name, start, end, step)

    return setting


def _resolve_optimization_context(user_id: int, strategy_id: int) -> tuple[str, str, str]:
    """Resolve symbol/date context from latest backtest, with sensible fallback."""
    latest = BacktestHistoryDao().get_latest_strategy_run(user_id=user_id, strategy_id=strategy_id)
    if latest and latest.get("vt_symbol") and latest.get("start_date") and latest.get("end_date"):
        start_value = latest.get("start_date")
        end_value = latest.get("end_date")
        start_text = start_value.isoformat() if hasattr(start_value, "isoformat") else str(start_value)
        end_text = end_value.isoformat() if hasattr(end_value, "isoformat") else str(end_value)
        return str(latest["vt_symbol"]), start_text, end_text

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=365)
    return "000001.SZ", start_date.isoformat(), end_date.isoformat()


def _normalize_optimization_results(raw_results: list[tuple], objective_metric: str) -> list[dict[str, Any]]:
    """Normalize vn.py optimization tuples into API-friendly rows."""
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(raw_results, start=1):
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        params, target_value, statistics = row[0], row[1], row[2]
        if not isinstance(params, dict):
            params = {}
        if not isinstance(statistics, dict):
            statistics = {}

        metrics: dict[str, float] = {
            "target_value": float(statistics.get(objective_metric, target_value) or 0.0),
            "total_return": float(statistics.get("total_return", 0.0) or 0.0),
            "annual_return": float(statistics.get("annual_return", 0.0) or 0.0),
            "max_drawdown": float(statistics.get("max_drawdown", statistics.get("max_ddpercent", 0.0)) or 0.0),
            "max_drawdown_percent": float(statistics.get("max_ddpercent", statistics.get("max_drawdown", 0.0)) or 0.0),
            "sharpe_ratio": float(statistics.get("sharpe_ratio", 0.0) or 0.0),
            "calmar_ratio": float(statistics.get("calmar_ratio", 0.0) or 0.0),
        }

        if objective_metric not in metrics:
            metrics[objective_metric] = float(statistics.get(objective_metric, target_value) or 0.0)

        rows.append(
            {
                "rank_order": index,
                "parameters": params,
                "statistics": metrics,
            }
        )
    return rows


def _run_sequential_optimization(
    *,
    strategy_class: type,
    symbol: str,
    start: datetime,
    end: datetime,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    capital: float,
    optimization_setting: OptimizationSetting,
    search_method: str,
) -> list[tuple]:
    """Run optimization in-process (fallback for dynamically compiled strategies)."""
    all_settings = optimization_setting.generate_settings()
    if not all_settings:
        return []

    sampled_settings = all_settings
    max_budget = 400
    if search_method == "random":
        sample_size = min(len(all_settings), 100)
        indices = np.random.default_rng(42).choice(len(all_settings), size=sample_size, replace=False)
        sampled_settings = [all_settings[int(i)] for i in indices]
    elif search_method == "bayesian":
        # Bayesian approximation fallback: evaluate a smaller adaptive-like sample budget.
        sample_size = min(len(all_settings), 50)
        indices = np.random.default_rng(7).choice(len(all_settings), size=sample_size, replace=False)
        sampled_settings = [all_settings[int(i)] for i in indices]
    elif len(all_settings) > max_budget:
        # Grid fallback budget: evaluate representative evenly-distributed points.
        indices = np.linspace(0, len(all_settings) - 1, num=max_budget, dtype=int)
        sampled_settings = [all_settings[int(i)] for i in indices]
        logger.info(
            "[worker] Grid space too large (%s), evaluating sampled subset %s",
            len(all_settings),
            max_budget,
        )

    results: list[tuple] = []
    for index, setting in enumerate(sampled_settings, start=1):
        if index == 1 or index % 20 == 0:
            logger.info("[worker] Sequential optimization progress %s/%s", index, len(sampled_settings))
        try:
            result = evaluate(
                optimization_setting.target_name,
                strategy_class,
                symbol,
                Interval.DAILY,
                start,
                rate,
                slippage,
                size,
                pricetick,
                capital,
                end,
                BacktestingMode.BAR,
                setting,
            )
            results.append(result)
        except Exception:
            logger.exception("[worker] Optimization evaluation failed for setting %s", setting)

    results.sort(reverse=True, key=lambda row: float(row[1] or 0.0))
    return results


def run_optimization_task(
    strategy_code: Optional[str],
    strategy_class_name: str,
    symbol: str,
    start_date: str,
    end_date: str,
    initial_capital: float,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    optimization_settings: Dict[str, Any] | OptimizationSetting,
    job_id: str = None,
    search_method: str = "grid",
    objective_metric: str = "sharpe_ratio",
) -> Dict[str, Any]:
    """
    Run parameter optimization task in background worker.

    Args:
        strategy_code: Custom strategy code (if not builtin)
        strategy_class_name: Name of strategy class
        symbol: Trading symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        initial_capital: Initial capital
        rate: Commission rate
        slippage: Slippage
        size: Contract size
        pricetick: Price tick
        optimization_settings: Parameter ranges for optimization
        job_id: Job ID for tracking

    Returns:
        Dict with optimization results
    """
    try:
        _configure_vnpy_mysql_from_env()
        vnpy_symbol = convert_to_vnpy_symbol(symbol)

        logger.info("[worker] Starting optimization job %s", job_id)
        logger.info("[worker] Strategy=%s Symbol=%s -> %s", strategy_class_name, symbol, vnpy_symbol)

        # Load strategy class
        if strategy_code:
            strategy_class = compile_strategy(strategy_code, strategy_class_name)
        else:
            from app.strategies.triple_ma_strategy import TripleMAStrategy
            from app.strategies.turtle_trading import TurtleTradingStrategy

            builtin_strategies = {
                "TripleMAStrategy": TripleMAStrategy,
                "TurtleTradingStrategy": TurtleTradingStrategy,
            }
            strategy_class = builtin_strategies.get(strategy_class_name)

            if not strategy_class:
                raise ValueError(f"Unknown builtin strategy: {strategy_class_name}")

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Initialize backtest engine
        engine = BacktestingEngine()
        engine.set_parameters(
            vt_symbol=vnpy_symbol,
            interval=Interval.DAILY,
            start=start_dt,
            end=end_dt,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=initial_capital,
        )

        # Add strategy
        engine.add_strategy(strategy_class, {})

        # Load data
        logger.info("[worker] Loading data for %s...", symbol)
        engine.load_data()
        if not engine.history_data:
            ensure_vnpy_history_data(vnpy_symbol, start_date)
            engine.history_data = []
            engine.load_data()
        if not engine.history_data:
            raise RuntimeError(f"No historical bar data found for {symbol} between {start_date} and {end_date}")

        setting = (
            optimization_settings
            if isinstance(optimization_settings, OptimizationSetting)
            else _build_optimization_setting(optimization_settings, objective_metric)
        )

        # Run optimization
        logger.info("[worker] Running optimization with method=%s target=%s...", search_method, setting.target_name)
        needs_sequential = getattr(strategy_class, "__module__", "") == "builtins"
        if needs_sequential:
            logger.info("[worker] Using sequential optimization path for dynamic strategy class")
            optimization_result = _run_sequential_optimization(
                strategy_class=strategy_class,
                symbol=vnpy_symbol,
                start=start_dt,
                end=end_dt,
                rate=rate,
                slippage=slippage,
                size=size,
                pricetick=pricetick,
                capital=initial_capital,
                optimization_setting=setting,
                search_method=search_method,
            )
        elif search_method == "grid":
            optimization_result = engine.run_bf_optimization(
                optimization_setting=setting,
                output=False,
                max_workers=4,
            )
        else:
            optimization_result = engine.run_ga_optimization(
                optimization_setting=setting,
                output=False,
                max_workers=4,
                ngen=18 if search_method == "bayesian" else 12,
            )

        # Format results
        results = _normalize_optimization_results(optimization_result, setting.target_name or objective_metric)

        return {
            "job_id": job_id,
            "status": "completed",
            "symbol": symbol,
            "total_combinations": len(results),
            "best_parameters": results[0]["parameters"] if results else {},
            "best_statistics": results[0]["statistics"] if results else {},
            "top_10_results": results[:10],
            "all_results": results,
            "completed_at": datetime.now().isoformat(),
        }

    except Exception as e:
        error_msg = f"Optimization failed: {str(e)}"
        logger.exception("[worker] %s", error_msg)

        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }


def run_optimization_record_task(task_id: int) -> Dict[str, Any]:
    """
    Execute one optimization_tasks row and persist results back to DB.
    """
    dao = OptimizationTaskDao()
    task = dao.get_task_for_worker(task_id)
    if not task:
        logger.error("[worker] Optimization task %s not found", task_id)
        return {"status": "failed", "error": "Task not found", "task_id": task_id}

    user_id = int(task.get("user_id") or 0)
    strategy_id = int(task.get("strategy_id") or 0)
    search_method = str(task.get("search_method") or "grid")
    objective_metric = str(task.get("objective_metric") or "sharpe_ratio")
    param_space = task.get("param_space") or {}

    dao.update_status(task_id, "running")

    try:
        strategy_code, strategy_class_name, _ = StrategySourceDao().get_strategy_source_for_user(strategy_id, user_id)
        symbol, start_date, end_date = _resolve_optimization_context(user_id, strategy_id)

        result = run_optimization_task(
            strategy_code=strategy_code,
            strategy_class_name=strategy_class_name,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            initial_capital=100000.0,
            rate=0.0003,
            slippage=0.0001,
            size=1,
            pricetick=0.01,
            optimization_settings=param_space,
            job_id=f"opt_task_{task_id}",
            search_method=search_method,
            objective_metric=objective_metric,
        )

        if result.get("status") != "completed":
            dao.update_status(task_id, "failed")
            return {"task_id": task_id, **result}

        all_results = list(result.get("all_results") or [])
        rows_for_db = [
            {
                "rank_order": index,
                "params": row.get("parameters") or {},
                "metrics": row.get("statistics") or {},
            }
            for index, row in enumerate(all_results[:200], start=1)
        ]
        dao.replace_results(task_id, rows_for_db)
        dao.update_status(
            task_id=task_id,
            status="completed",
            best_params=result.get("best_parameters") or {},
            best_metrics=result.get("best_statistics") or {},
            total_iterations=int(result.get("total_combinations") or len(all_results)),
        )
        return {"task_id": task_id, **result}

    except Exception as exc:
        logger.exception("[worker] Optimization task %s failed", task_id)
        dao.update_status(task_id, "failed")
        return {"status": "failed", "error": str(exc), "task_id": task_id}

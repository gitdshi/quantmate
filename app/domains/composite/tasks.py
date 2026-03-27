"""RQ worker task for composite strategy backtests."""

import json
import logging
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_market_data(
    symbols: List[str],
    start_date: str,
    end_date: str,
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Load daily OHLCV data for all symbols in the date range.

    Returns: date_str → symbol → {open, high, low, close, volume, prev_close}
    """
    from datetime import date as date_type
    from app.domains.market.service import MarketService

    market_svc = MarketService()
    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    e = datetime.strptime(end_date, "%Y-%m-%d").date()

    for sym in symbols:
        try:
            # Convert tushare-style to vnpy-style for MarketService
            code, exch = sym.rsplit(".", 1) if "." in sym else (sym, "")
            exch_upper = exch.upper()
            vnpy_map = {"SZ": "SZSE", "SH": "SSE", "BJ": "BSE"}
            vt_exch = vnpy_map.get(exch_upper, exch_upper)
            vt_symbol = f"{code}.{vt_exch}" if vt_exch else sym

            bars = market_svc.get_history(vt_symbol, s, e)
            if not bars:
                continue
            prev_close = None
            for bar in bars:
                dt = bar.get("datetime")
                if isinstance(dt, datetime):
                    day = dt.strftime("%Y-%m-%d")
                elif hasattr(dt, "isoformat"):
                    day = dt.isoformat()
                else:
                    day = str(dt) if dt else ""
                if not day:
                    continue
                if day not in result:
                    result[day] = {}
                close = float(bar.get("close", 0))
                result[day][sym] = {
                    "open": float(bar.get("open", 0)),
                    "high": float(bar.get("high", 0)),
                    "low": float(bar.get("low", 0)),
                    "close": close,
                    "volume": float(bar.get("volume", 0)),
                    "prev_close": prev_close if prev_close else close,
                }
                prev_close = close
        except Exception as exc:
            logger.warning("[composite_bt] Failed to load data for %s: %s", sym, exc)

    return result


def _load_benchmark_data(
    benchmark: str, start_date: str, end_date: str
) -> Optional[Dict[str, float]]:
    """Load benchmark close prices for the date range."""
    try:
        from app.domains.backtests.dao.akshare_benchmark_dao import AkshareBenchmarkDao

        dao = AkshareBenchmarkDao()
        from datetime import date as date_type

        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
        data = dao.get_benchmark_data(start=s, end=e, benchmark_symbol=benchmark)
        if data and "dates" in data and "close" in data:
            return dict(zip(data["dates"], data["close"]))
    except Exception as exc:
        logger.warning("[composite_bt] Failed to load benchmark: %s", exc)
    return None


def run_composite_backtest_task(
    composite_strategy_id: int,
    user_id: int,
    start_date: str,
    end_date: str,
    initial_capital: float = 1_000_000.0,
    benchmark: str = "000300.SH",
    job_id: str = "",
) -> Dict[str, Any]:
    """RQ worker task: run a composite strategy backtest.

    Loads the composite strategy definition, resolves components via bindings,
    builds the orchestrator and backtest engine, then executes.
    """
    from app.domains.composite.dao.composite_strategy_dao import CompositeStrategyDao
    from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
    from app.domains.composite.dao.composite_backtest_dao import CompositeBacktestDao
    from app.domains.composite.orchestrator import CompositeStrategyOrchestrator
    from app.domains.composite.backtest_engine import CompositeBacktestEngine
    from app.domains.composite.market_constraints import MarketConstraints
    from rq import get_current_job

    current_job = get_current_job()
    if current_job and not job_id:
        job_id = current_job.id

    bt_dao = CompositeBacktestDao()

    try:
        # Mark as running
        bt_dao.update_status(job_id, "running")
        logger.info(
            "[composite_bt] Starting job %s for composite %s (%s ~ %s)",
            job_id, composite_strategy_id, start_date, end_date,
        )

        # 1. Load composite strategy + bindings
        cs_dao = CompositeStrategyDao()
        strategy = cs_dao.get_for_user(user_id, composite_strategy_id)
        if not strategy:
            raise ValueError(f"Composite strategy {composite_strategy_id} not found")

        bindings = cs_dao.get_bindings(composite_strategy_id)
        if not bindings:
            raise ValueError("No component bindings defined")

        # 2. Load all bound components
        comp_dao = StrategyComponentDao()
        component_ids = [b["component_id"] for b in bindings]
        components_map: Dict[int, Dict] = {}
        for cid in set(component_ids):
            comp = comp_dao.get_for_user(user_id, cid)
            if comp:
                components_map[cid] = comp

        # 3. Group by layer, attach config_override
        universe_comps: List[Dict] = []
        trading_comps: List[Dict] = []
        risk_comps: List[Dict] = []

        for binding in sorted(bindings, key=lambda b: b.get("ordinal", 0)):
            comp = components_map.get(binding["component_id"])
            if not comp:
                continue
            # Merge config_override from binding
            enriched = dict(comp)
            override = binding.get("config_override")
            if isinstance(override, str):
                try:
                    override = json.loads(override)
                except (json.JSONDecodeError, TypeError):
                    override = None
            enriched["config_override"] = override
            enriched["weight"] = float(binding.get("weight", 1.0))

            layer = binding.get("layer", comp.get("layer", ""))
            if layer == "universe":
                universe_comps.append(enriched)
            elif layer == "trading":
                trading_comps.append(enriched)
            elif layer == "risk":
                risk_comps.append(enriched)

        if not trading_comps:
            raise ValueError("At least one trading component is required")

        # 4. Build market constraints
        mc_raw = strategy.get("market_constraints")
        if isinstance(mc_raw, str):
            try:
                mc_raw = json.loads(mc_raw)
            except (json.JSONDecodeError, TypeError):
                mc_raw = None
        constraints = MarketConstraints.from_dict(mc_raw)

        # 5. Determine symbols to load
        # Collect symbols from universe component configs
        symbols_to_load: set = set()
        for comp in universe_comps:
            cfg = comp.get("config") or {}
            if isinstance(cfg, str):
                try:
                    cfg = json.loads(cfg)
                except (json.JSONDecodeError, TypeError):
                    cfg = {}
            syms = cfg.get("symbols", [])
            if isinstance(syms, list):
                symbols_to_load.update(syms)

        # If no explicit symbols, use a default CSI 300 sample (first few)
        if not symbols_to_load:
            symbols_to_load = {
                "600519.SH", "000858.SZ", "601318.SH",
                "000333.SZ", "600036.SH",
            }

        all_symbols = sorted(symbols_to_load)

        # 6. Load market data
        market_data = _load_market_data(all_symbols, start_date, end_date)
        if not market_data:
            raise ValueError("No market data available for the specified period")

        # 7. Load benchmark
        benchmark_data = _load_benchmark_data(benchmark, start_date, end_date)

        # 8. Build engine and run
        orchestrator = CompositeStrategyOrchestrator(
            universe_comps, trading_comps, risk_comps
        )
        engine = CompositeBacktestEngine(
            orchestrator=orchestrator,
            constraints=constraints,
            initial_capital=initial_capital,
            benchmark=benchmark,
        )
        result = engine.run(
            start_date=start_date,
            end_date=end_date,
            market_data_by_day=market_data,
            all_symbols=all_symbols,
            benchmark_data=benchmark_data,
        )

        # 9. Save results
        bt_dao.update_status(
            job_id,
            "completed",
            result={
                "equity_curve": result["equity_curve"],
                "trade_log": result["trade_log"],
                "position_history": result["position_history"],
                "metrics": result["metrics"],
            },
            attribution=result.get("attribution"),
        )
        logger.info("[composite_bt] Job %s completed", job_id)
        return {"status": "completed", "metrics": result["metrics"]}

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("[composite_bt] Job %s failed: %s", job_id, error_msg)
        try:
            bt_dao.update_status(
                job_id, "failed", error_message=error_msg
            )
        except Exception:
            logger.exception("[composite_bt] Failed to update error status")
        return {"status": "failed", "error": error_msg}

"""Background task helpers for unified factor backtests."""

from __future__ import annotations

import logging
from datetime import date as date_type, datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text

from app.api.models.backtest import BacktestRunRequest
from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
from app.domains.composite.backtest_engine import CompositeBacktestEngine
from app.domains.composite.market_constraints import MarketConstraints
from app.domains.composite.orchestrator import CompositeStrategyOrchestrator
from app.domains.factors.expression_engine import (
    augment_factor_eval_ohlcv,
    compute_custom_factor,
    compute_factor_metrics,
    compute_forward_returns,
    fetch_ohlcv,
    normalize_factor_expression,
)
from app.domains.factors.service import FactorService
from app.domains.market.dao.watchlist_dao import WatchlistDao
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

_UNIVERSE_INDEX_CODES = {
    "csi300": "000300.SH",
    "csi500": "000905.SH",
    "csi1000": "000852.SH",
}


def _normalize_symbol(symbol: str) -> str:
    value = str(symbol or "").strip().upper()
    if not value:
        return ""

    if "." in value:
        code, suffix = value.split(".", 1)
        suffix_map = {"SZSE": "SZ", "SSE": "SH", "BJSE": "BJ", "SZ": "SZ", "SH": "SH", "BJ": "BJ"}
        return f"{code}.{suffix_map.get(suffix, suffix)}"

    if value.startswith(("6", "9", "5")):
        return f"{value}.SH"
    if value.startswith(("4", "8")):
        return f"{value}.BJ"
    return f"{value}.SZ"


def _dedupe_symbols(symbols: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = _normalize_symbol(symbol)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _resolve_index_universe(preset: str, end_date: date_type) -> list[str]:
    index_code = _UNIVERSE_INDEX_CODES.get(str(preset or "").strip().lower())
    if not index_code:
        raise ValueError(f"Unsupported factor universe preset: {preset}")

    with connection("tushare") as conn:
        snapshot_date = conn.execute(
            text(
                "SELECT MAX(trade_date) FROM index_weight "
                "WHERE index_code = :index_code AND trade_date <= :trade_date"
            ),
            {"index_code": index_code, "trade_date": end_date},
        ).scalar()

        if snapshot_date is None:
            snapshot_date = conn.execute(
                text("SELECT MAX(trade_date) FROM index_weight WHERE index_code = :index_code"),
                {"index_code": index_code},
            ).scalar()

        if snapshot_date is None:
            raise ValueError(f"No constituent snapshot found for preset universe: {preset}")

        rows = conn.execute(
            text(
                "SELECT DISTINCT con_code FROM index_weight "
                "WHERE index_code = :index_code AND trade_date = :trade_date "
                "ORDER BY con_code"
            ),
            {"index_code": index_code, "trade_date": snapshot_date},
        ).fetchall()

    instruments = [str(row[0]).strip() for row in rows if str(row[0]).strip()]
    return _dedupe_symbols(instruments)


def _resolve_watchlist_symbols(user_id: int, watchlist_id: int) -> list[str]:
    dao = WatchlistDao()
    watchlist = dao.get(watchlist_id)
    if not watchlist or watchlist.get("user_id") != user_id:
        raise ValueError("Watchlist not found")
    items = dao.list_items(watchlist_id)
    return _dedupe_symbols([str(item.get("symbol") or "") for item in items])


def _resolve_instruments(user_id: int, profile: dict[str, Any], end_date: date_type) -> tuple[list[str], dict[str, Any]]:
    universe = profile.get("universe") if isinstance(profile.get("universe"), dict) else {}

    if isinstance(profile.get("instruments"), list):
        instruments = _dedupe_symbols(profile["instruments"])
        return instruments, {"type": "symbols", "symbols": instruments}

    if isinstance(universe.get("symbols"), list):
        instruments = _dedupe_symbols(universe["symbols"])
        return instruments, {"type": "symbols", "symbols": instruments}

    watchlist_id = universe.get("watchlist_id") or profile.get("watchlist_id")
    if watchlist_id is not None:
        instruments = _resolve_watchlist_symbols(user_id, int(watchlist_id))
        return instruments, {"type": "watchlist", "watchlist_id": int(watchlist_id), "symbols": instruments}

    preset = universe.get("preset") or profile.get("preset")
    if preset:
        instruments = _resolve_index_universe(str(preset), end_date)
        return instruments, {"type": "preset", "preset": str(preset), "symbols": instruments}

    raise ValueError("Factor backtest requires universe.symbols, universe.watchlist_id, profile.instruments, or universe.preset")


def _build_factor_components(expression: str, instruments: list[str], profile: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], int, float]:
    requested_top_n = int(profile.get("top_n") or 10)
    top_n = max(1, min(requested_top_n, len(instruments)))
    direction_raw = profile.get("direction", 1.0)
    if isinstance(direction_raw, str):
        normalized = direction_raw.strip().lower()
        if normalized in {"short", "bottom", "asc", "ascending", "-1"}:
            direction = -1.0
        else:
            direction = 1.0
    else:
        direction = -1.0 if float(direction_raw or 1.0) < 0 else 1.0

    universe_top_n = int(profile.get("universe_top_n") or len(instruments))
    max_total_positions = max(1, int(profile.get("max_total_positions") or top_n))
    max_position_pct = float(profile.get("max_position_pct") or min(1.0 / max_total_positions, 1.0))

    universe_components = [
        {
            "name": "Factor Universe",
            "layer": "universe",
            "config": {
                "symbols": instruments,
                "factor_expression": expression,
                "direction": direction,
                "top_n": max(1, min(universe_top_n, len(instruments))),
                "min_volume": profile.get("min_volume"),
            },
        }
    ]
    trading_components = [
        {
            "name": "Factor Ranking",
            "layer": "trading",
            "config": {
                "factor_expression": expression,
                "direction": direction,
                "top_n": top_n,
                "close_on_universe_exit": True,
            },
        }
    ]
    risk_components = [
        {
            "name": "Factor Risk",
            "layer": "risk",
            "config": {
                "max_total_positions": max_total_positions,
                "max_position_pct": max_position_pct,
                "stop_loss_pct": profile.get("stop_loss_pct"),
            },
        }
    ]
    return universe_components, trading_components, risk_components, top_n, direction


def _latest_factor_snapshot(factor_values: pd.Series, top_n: int, direction: float) -> list[dict[str, Any]]:
    if factor_values.empty or not isinstance(factor_values.index, pd.MultiIndex):
        return []

    frame = factor_values.dropna().rename("score").reset_index()
    if frame.empty or "date" not in frame.columns:
        return []

    latest_date = frame["date"].max()
    latest = frame[frame["date"] == latest_date].copy()
    latest = latest.sort_values("score", ascending=direction < 0).head(top_n)
    return [
        {
            "instrument": str(row["instrument"]),
            "date": str(pd.Timestamp(row["date"]).date()),
            "score": round(float(row["score"]), 6),
        }
        for _, row in latest.iterrows()
    ]


def _upsert_factor_history(
    *,
    user_id: int,
    job_id: str,
    request: BacktestRunRequest,
    subject_id: Optional[int],
    subject_name: Optional[str],
    request_payload: dict[str, Any],
    status: str,
    result: Optional[dict[str, Any]] = None,
    summary: Optional[dict[str, Any]] = None,
    artifacts: Optional[dict[str, Any]] = None,
    extensions: Optional[dict[str, Any]] = None,
    error: Optional[str] = None,
    completed: bool = False,
) -> None:
    BacktestHistoryDao().upsert_history(
        user_id=user_id,
        job_id=job_id,
        strategy_id=None,
        strategy_class=None,
        strategy_version=None,
        source="runs_api",
        vt_symbol="",
        start_date=str(request.start_date),
        end_date=str(request.end_date),
        parameters=request.profile,
        status=status,
        result=result,
        error=error,
        created_at=datetime.utcnow(),
        completed_at=datetime.utcnow() if completed else None,
        subject_type="factor",
        subject_id=subject_id,
        subject_name=subject_name,
        engine_type="portfolio_daily",
        scope_type="cross_sectional_portfolio",
        request_payload=request_payload,
        summary_json=summary,
        artifacts_json=artifacts,
        extensions_json=extensions,
        result_schema_version=2,
    )


def run_factor_backtest_task(job_id: str, user_id: int, request_payload: dict[str, Any]) -> dict[str, Any]:
    """Execute a unified factor backtest and persist it into backtest history."""
    from app.domains.composite.tasks import _load_benchmark_data, _load_market_data

    request = BacktestRunRequest.model_validate(request_payload)
    profile = request.profile or {}
    factor_id = request.subject_id or profile.get("factor_id")
    factor = None
    if factor_id is not None:
        factor = FactorService().get_factor(user_id, int(factor_id))

    expression = str(profile.get("expression") or (factor or {}).get("expression") or "").strip()
    if not expression:
        raise ValueError("Factor backtest requires a factor definition or profile.expression")

    subject_id = int(factor_id) if factor_id is not None else None
    subject_name = request.subject_name or (factor or {}).get("name") or "Custom Factor"
    request_payload = {**request_payload, "subject_id": subject_id, "subject_name": subject_name}

    try:
        _upsert_factor_history(
            user_id=user_id,
            job_id=job_id,
            request=request,
            subject_id=subject_id,
            subject_name=subject_name,
            request_payload=request_payload,
            status="running",
        )

        instruments, universe_meta = _resolve_instruments(user_id, profile, request.end_date)
        if not instruments:
            raise ValueError("Factor backtest universe resolved to zero symbols")

        ohlcv = fetch_ohlcv(start_date=request.start_date, end_date=request.end_date, instruments=instruments)
        if ohlcv.empty:
            raise ValueError("No OHLCV data available for the selected universe and date range")

        eval_ohlcv = augment_factor_eval_ohlcv(ohlcv)
        normalized_expression = expression
        try:
            factor_values = compute_custom_factor(expression, eval_ohlcv)
        except Exception:
            normalized_expression = normalize_factor_expression(expression)
            factor_values = compute_custom_factor(normalized_expression, eval_ohlcv)

        forward_periods = max(1, int(profile.get("forward_periods") or 1))
        forward_returns = compute_forward_returns(ohlcv, periods=forward_periods)
        factor_metrics = compute_factor_metrics(factor_values, forward_returns)

        market_data = _load_market_data(instruments, str(request.start_date), str(request.end_date))
        if not market_data:
            raise ValueError("No market data available for the selected factor universe")

        benchmark = request.benchmark or "000300.SH"
        benchmark_data = _load_benchmark_data(benchmark, str(request.start_date), str(request.end_date))
        universe_components, trading_components, risk_components, top_n, direction = _build_factor_components(
            normalized_expression,
            instruments,
            profile,
        )

        orchestrator = CompositeStrategyOrchestrator(
            universe_components,
            trading_components,
            risk_components,
        )
        engine = CompositeBacktestEngine(
            orchestrator=orchestrator,
            constraints=MarketConstraints.from_dict(profile.get("market_constraints")),
            initial_capital=request.initial_capital,
            benchmark=benchmark,
        )
        portfolio_result = engine.run(
            start_date=str(request.start_date),
            end_date=str(request.end_date),
            market_data_by_day=market_data,
            all_symbols=instruments,
            benchmark_data=benchmark_data,
        )

        latest_scores = _latest_factor_snapshot(factor_values, top_n, direction)
        summary = {
            **portfolio_result.get("metrics", {}),
            **factor_metrics,
            "universe_size": len(instruments),
            "top_n": top_n,
        }
        result = {
            "statistics": portfolio_result.get("metrics", {}),
            "factor_metrics": factor_metrics,
            "equity_curve": portfolio_result.get("equity_curve", []),
            "trade_log": portfolio_result.get("trade_log", []),
            "position_history": portfolio_result.get("position_history", []),
            "latest_factor_snapshot": latest_scores,
        }
        artifacts = {
            "equity_curve": portfolio_result.get("equity_curve", []),
            "trade_log": portfolio_result.get("trade_log", []),
            "position_history": portfolio_result.get("position_history", []),
            "latest_factor_snapshot": latest_scores,
        }
        extensions = {
            "factor": {
                "expression": expression,
                "normalized_expression": normalized_expression,
                "forward_periods": forward_periods,
                "universe": universe_meta,
            }
        }

        _upsert_factor_history(
            user_id=user_id,
            job_id=job_id,
            request=request,
            subject_id=subject_id,
            subject_name=subject_name,
            request_payload=request_payload,
            status="completed",
            result=result,
            summary=summary,
            artifacts=artifacts,
            extensions=extensions,
            completed=True,
        )
        return {"status": "completed", "metrics": summary}
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        logger.exception("[factor-backtest] Job %s failed: %s", job_id, error_message)
        _upsert_factor_history(
            user_id=user_id,
            job_id=job_id,
            request=request,
            subject_id=subject_id if 'subject_id' in locals() else None,
            subject_name=subject_name if 'subject_name' in locals() else request.subject_name,
            request_payload=request_payload,
            status="failed",
            error=error_message,
            completed=True,
        )
        return {"status": "failed", "error": error_message}
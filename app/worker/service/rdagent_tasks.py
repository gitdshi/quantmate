"""RD-Agent background worker tasks — autonomous factor mining via RQ.

Follows the same pattern as qlib_tasks.py: lazy imports, try/except wrapping,
dict return with status.
"""

from __future__ import annotations

import logging
import math
import re
import traceback
from datetime import date as date_type, datetime, timedelta
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_SENSITIVE_ERROR_PATTERNS = (
    re.compile(r"(?i)(authorization['\"]?\s*[:=]\s*['\"]?bearer\s+)([^'\"\s,]+)"),
    re.compile(r"(?i)((?:openai|opencode|litellm)[a-z0-9_]*api[_-]?key['\"]?\s*[:=]\s*['\"]?)([^'\"\s,]+)"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{8,}\b"),
)

# Lazy-loaded references
_RDAgentService = None
_feature_descriptor = None
_UNIVERSE_INDEX_CODES = {
    "csi300": "000300.SH",
    "csi500": "000905.SH",
    "csi1000": "000852.SH",
}


def _get_rdagent_service():
    global _RDAgentService
    if _RDAgentService is None:
        from app.domains.factors.rdagent_service import RDAgentService

        _RDAgentService = RDAgentService
    return _RDAgentService


def _get_feature_descriptor():
    global _feature_descriptor
    if _feature_descriptor is None:
        from app.domains.factors import feature_descriptor

        _feature_descriptor = feature_descriptor
    return _feature_descriptor


def _is_run_cancelled(run_id: str) -> bool:
    try:
        from app.domains.factors.rdagent_service import _get_run_status

        return _get_run_status(run_id) == "cancelled"
    except Exception:
        logger.debug("[rdagent-worker] Could not load run status for %s", run_id, exc_info=True)
        return False


def _sanitize_error_text(error_text: Any) -> Optional[str]:
    if error_text is None:
        return None

    sanitized = str(error_text)
    for pattern in _SENSITIVE_ERROR_PATTERNS[:2]:
        sanitized = pattern.sub(r"\1[REDACTED]", sanitized)
    sanitized = _SENSITIVE_ERROR_PATTERNS[2].sub("sk-[REDACTED]", sanitized)
    return sanitized


def run_rdagent_mining_task(
    user_id: int,
    run_id: str,
    config_dict: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute an RD-Agent factor mining run in a background worker.

    This task:
    1. Updates run status to 'running'
    2. Calls the RD-Agent sidecar API to start the mining loop
    3. Polls / streams iteration results back to the database
    4. Marks the run as completed or failed
    """
    try:
        logger.info("[rdagent-worker] Starting mining run %s for user %d", run_id, user_id)

        from app.domains.factors.rdagent_service import (
            _update_run_status,
            save_iteration,
            save_discovered_factor,
        )

        _update_run_status(run_id, "running")

        # Build feature context for the sidecar
        fd = _get_feature_descriptor()
        prompt_context = fd.build_prompt_context()

        # Call sidecar
        result = _call_sidecar_mining(
            run_id=run_id,
            config=config_dict,
            prompt_context=prompt_context,
        )

        sanitized_error = _sanitize_error_text(result.get("error"))
        if sanitized_error is not None:
            result["error"] = sanitized_error

        if result.get("status") == "cancelled" or _is_run_cancelled(run_id):
            _update_run_status(run_id, "cancelled", result.get("error"))
            return {
                "run_id": run_id,
                "status": "cancelled",
                "error": result.get("error"),
            }

        if result.get("status") == "failed":
            _update_run_status(run_id, "failed", result.get("error"))
            return {
                "run_id": run_id,
                "status": "failed",
                "error": result.get("error"),
            }

        # Save iterations
        iterations = result.get("iterations", [])
        for it in iterations:
            save_iteration(
                run_id=run_id,
                iteration_number=it.get("iteration", 0),
                hypothesis=it.get("hypothesis"),
                experiment_code=it.get("code"),
                metrics=_serialize(it.get("metrics")),
                feedback=it.get("feedback"),
                status=it.get("status", "completed"),
            )

        # Save discovered factors
        factors = result.get("discovered_factors", [])
        eval_context = _build_discovered_factor_eval_context(config_dict)
        for f in factors:
            evaluated_metrics = None
            if _needs_metric_evaluation(f):
                evaluated_metrics = _evaluate_discovered_factor_metrics(
                    f.get("expression", ""),
                    eval_context,
                )
            save_discovered_factor(
                run_id=run_id,
                factor_name=f.get("name", "unnamed"),
                expression=f.get("expression", ""),
                description=f.get("description"),
                ic_mean=_coalesce_metric(f.get("ic_mean"), evaluated_metrics, "ic_mean"),
                icir=_coalesce_metric(f.get("icir"), evaluated_metrics, "icir"),
                sharpe=_coalesce_metric(f.get("sharpe"), evaluated_metrics, "sharpe"),
            )

        completed_iterations = _count_completed_iterations(iterations)
        _update_run_status(
            run_id,
            "completed",
            current_iteration=completed_iterations,
            total_iterations=_resolve_total_iterations(config_dict, completed_iterations),
        )

        return {
            "run_id": run_id,
            "status": "completed",
            "iterations": len(iterations),
            "discovered_factors": len(factors),
            "completed_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.exception("[rdagent-worker] Mining run %s failed: %s", run_id, e)
        sanitized_error = _sanitize_error_text(str(e))
        try:
            from app.domains.factors.rdagent_service import _update_run_status

            if not _is_run_cancelled(run_id):
                _update_run_status(run_id, "failed", sanitized_error)
        except Exception:
            logger.exception("[rdagent-worker] Failed to update run status")
        return {
            "run_id": run_id,
            "status": "failed",
            "error": sanitized_error,
            "traceback": traceback.format_exc(),
        }


def _call_sidecar_mining(
    run_id: str,
    config: Dict[str, Any],
    prompt_context: str,
) -> Dict[str, Any]:
    """Call the RD-Agent sidecar container to run the mining loop.

    In production this makes HTTP requests to the sidecar.
    Returns a result dict with iterations and discovered_factors.
    """
    import httpx

    from app.infrastructure.config import get_runtime_float, get_runtime_str, get_settings

    settings = get_settings()
    sidecar_url = get_runtime_str(
        env_keys="RDAGENT_SIDECAR_URL",
        db_key="rdagent.sidecar_url",
        default=getattr(settings, "rdagent_sidecar_url", "http://rdagent-service:8001"),
    )

    payload = {
        "run_id": run_id,
        "config": config,
        "prompt_context": prompt_context,
    }

    try:
        with httpx.Client(
            timeout=httpx.Timeout(
                timeout=get_runtime_float(
                    env_keys="RDAGENT_REQUEST_TIMEOUT_SECONDS",
                    db_key="rdagent.request_timeout_seconds",
                    default=14400.0,
                )
            )
        ) as client:
            resp = client.post(f"{sidecar_url}/mine", json=payload)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as exc:
        error_text = exc.response.text[:500]
        try:
            payload = exc.response.json()
            if isinstance(payload, dict):
                error_text = payload.get("error") or payload.get("detail") or error_text
        except ValueError:
            pass
        return {
            "status": "failed",
            "error": _sanitize_error_text(
                f"Sidecar returned {exc.response.status_code}: {error_text}"
            ),
        }
    except httpx.ConnectError:
        return {
            "status": "failed",
            "error": "Cannot connect to RD-Agent sidecar. Is the service running?",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": _sanitize_error_text(f"Sidecar call failed: {exc}"),
        }


def _serialize(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    import json

    return json.dumps(obj, ensure_ascii=False, default=str)


def _needs_metric_evaluation(factor: Dict[str, Any]) -> bool:
    return all(_is_effectively_missing_metric(factor.get(key)) for key in ("ic_mean", "icir", "sharpe"))


def _is_effectively_missing_metric(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return math.isclose(float(value), 0.0, abs_tol=1e-12)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return True
        try:
            return math.isclose(float(stripped), 0.0, abs_tol=1e-12)
        except ValueError:
            return True
    return True


def _coalesce_metric(raw_value: Any, evaluated: Optional[Dict[str, float]], key: str) -> Optional[float]:
    if not _is_effectively_missing_metric(raw_value):
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            pass
    if evaluated is None:
        return 0.0 if _is_effectively_missing_metric(raw_value) else None
    return float(evaluated.get(key, 0.0))


def _build_discovered_factor_eval_context(config_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        from app.domains.factors.expression_engine import compute_forward_returns, fetch_ohlcv

        start_date = date_type.fromisoformat(config_dict.get("start_date", "2023-01-01"))
        end_date = date_type.fromisoformat(config_dict.get("end_date", "2024-12-31"))
        instruments = _resolve_eval_instruments(config_dict.get("universe"), end_date)
        ohlcv = fetch_ohlcv(start_date=start_date, end_date=end_date, instruments=instruments)
        if ohlcv.empty:
            fallback_dates = _resolve_eval_fallback_dates(instruments, start_date, end_date)
            if fallback_dates is None:
                return None

            fallback_start, fallback_end = fallback_dates
            ohlcv = fetch_ohlcv(start_date=fallback_start, end_date=fallback_end, instruments=instruments)
            if ohlcv.empty:
                return None

        eval_ohlcv = _augment_eval_ohlcv(ohlcv)
        return {
            "ohlcv": eval_ohlcv,
            "forward_returns": compute_forward_returns(eval_ohlcv, periods=1),
        }
    except Exception:
        logger.debug("[rdagent-worker] Failed to build evaluation context", exc_info=True)
        return None


def _resolve_eval_instruments(universe: Any, end_date: date_type) -> Optional[list[str]]:
    if not isinstance(universe, str):
        return None

    normalized = universe.strip().lower()
    if not normalized or normalized in {"all", "all_a", "all-a", "market"}:
        return None

    if "," in normalized:
        instruments = [item.strip().upper() for item in universe.split(",") if item.strip()]
        return instruments or None

    index_code = _UNIVERSE_INDEX_CODES.get(normalized)
    if not index_code:
        return None

    try:
        from sqlalchemy import text

        from app.infrastructure.db.connections import connection

        with connection("tushare") as conn:
            snapshot_query = text(
                "SELECT MAX(trade_date) FROM index_weight "
                "WHERE index_code = :index_code AND trade_date <= :trade_date"
            )
            snapshot_date = conn.execute(
                snapshot_query,
                {"index_code": index_code, "trade_date": end_date},
            ).scalar()

            if snapshot_date is None:
                snapshot_date = conn.execute(
                    text("SELECT MAX(trade_date) FROM index_weight WHERE index_code = :index_code"),
                    {"index_code": index_code},
                ).scalar()

            if snapshot_date is None:
                return None

            result = conn.execute(
                text(
                    "SELECT DISTINCT con_code "
                    "FROM index_weight "
                    "WHERE index_code = :index_code AND trade_date = :trade_date "
                    "ORDER BY con_code"
                ),
                {"index_code": index_code, "trade_date": snapshot_date},
            )
            instruments = [str(row[0]).strip() for row in result.fetchall() if str(row[0]).strip()]
            return instruments or None
    except Exception:
        logger.debug("[rdagent-worker] Failed to resolve universe %s", universe, exc_info=True)
        return None


def _resolve_eval_fallback_dates(
    instruments: Optional[list[str]],
    start_date: date_type,
    end_date: date_type,
) -> Optional[tuple[date_type, date_type]]:
    try:
        from sqlalchemy import bindparam, text

        from app.infrastructure.db.connections import connection

        query = "SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily WHERE 1=1"
        params: dict[str, Any] = {}
        statement = text(query)

        if instruments:
            query += " AND ts_code IN :instruments"
            params["instruments"] = tuple(instruments)
            statement = text(query).bindparams(bindparam("instruments", expanding=True))

        with connection("tushare") as conn:
            row = conn.execute(statement, params).first()

        if not row or row[0] is None or row[1] is None:
            return None

        available_start = _coerce_trade_date(row[0])
        available_end = _coerce_trade_date(row[1])
        if available_start is None or available_end is None:
            return None

        if available_end < start_date or available_start > end_date:
            requested_span = max((end_date - start_date).days, 0)
            fallback_end = available_end
            fallback_start = max(available_start, fallback_end - timedelta(days=requested_span))
            return fallback_start, fallback_end

        overlap_start = max(start_date, available_start)
        overlap_end = min(end_date, available_end)
        if overlap_start > overlap_end:
            return None

        return overlap_start, overlap_end
    except Exception:
        logger.debug("[rdagent-worker] Failed to resolve fallback eval dates", exc_info=True)
        return None


def _coerce_trade_date(value: Any) -> Optional[date_type]:
    if isinstance(value, date_type):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date_type.fromisoformat(value)
        except ValueError:
            return None
    return None


def _augment_eval_ohlcv(ohlcv):
    eval_ohlcv = ohlcv.copy()
    for periods in (1, 5, 10, 20):
        series = eval_ohlcv.groupby(level=0)["close"].pct_change(periods=periods)
        eval_ohlcv[f"ret_{periods}d"] = series
    return eval_ohlcv


def _evaluate_discovered_factor_metrics(
    expression: str,
    eval_context: Optional[Dict[str, Any]],
) -> Optional[Dict[str, float]]:
    if not expression or eval_context is None:
        return None

    try:
        from app.domains.factors.expression_engine import compute_custom_factor, compute_factor_metrics

        normalized_expression = _normalize_discovered_factor_expression(expression)
        factor_values = compute_custom_factor(normalized_expression, eval_context["ohlcv"])
        metrics = compute_factor_metrics(factor_values, eval_context["forward_returns"])
        return {
            "ic_mean": float(metrics.get("ic_mean", 0.0)),
            "icir": float(metrics.get("ic_ir", 0.0)),
            "sharpe": _compute_long_short_sharpe(factor_values, eval_context["forward_returns"]),
        }
    except Exception:
        logger.debug("[rdagent-worker] Failed to evaluate discovered factor expression: %s", expression, exc_info=True)
        return None


def _normalize_discovered_factor_expression(expression: str) -> str:
    normalized = expression.strip().replace("$", "")
    normalized = normalized.replace(r"\_", "_")
    compact = re.sub(r"\s+", " ", normalized)
    compact = re.sub(r"\s*where\s+.*$", "", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\\text\{([^{}]+)\}", r"\1", compact)
    compact = compact.replace("}]", "}")

    for source, target in {
        "Close": "close",
        "High": "high",
        "Low": "low",
        "Open": "open",
        "Volume": "volume",
        "VWAP": "vwap",
        "Mean": "mean",
        "Max": "max",
        "Min": "min",
        "Momentum": "momentum",
        "Volatility": "volatility",
        "VolumeRatio": "volume_ratio",
        "HighLowRange": "high_low_range",
        "VWAPCloseRatio": "vwap_close_ratio",
        "MA": "ma",
    }.items():
        compact = re.sub(rf"\b{source}\b", target, compact)

    compact = re.sub(r"^[A-Za-z_]+_\{[^}]+\}\s*=\s*", "", compact)
    compact = re.sub(r"^[A-Za-z_][A-Za-z0-9_]*\s*=\s*", "", compact)
    if " = " in compact and not compact.lstrip().startswith(r"\sigma"):
        compact = compact.rsplit(" = ", 1)[-1].strip()
    compact = re.sub(
        r"\\sigma\s*\(\s*R_\{t-(\d+):t\}\s*\)",
        lambda match: f"ts_std(ret_1d, {int(match.group(1)) + 1})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{volume(?:_t)?\}\{\\frac\{1\}\{(\d+)\}\s*\\sum_\{i=0\}\^\{\d+\}\s*volume_\{t-i\}\}",
        lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        compact,
    )
    compact = re.sub(
        r"\\frac\{volume(?:_t)?\}\{mean\((\w+)_\{t-(\d+):t\}\}\s*",
        lambda match: f"(volume) / (ts_mean({match.group(1).lower()}, {int(match.group(2)) + 1}))",
        compact,
    )
    compact = re.sub(
        r"\\frac\{close_t\}\{\\frac\{1\}\{(\d+)\}\s*\\sum_\{i=0\}\^\{\d+\}\s*close_\{t-i\}\}\s*-\s*1",
        lambda match: f"(close) / (ts_mean(close, {match.group(1)})) - 1",
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)_\{(\d+),t\}",
        lambda match: f"{match.group(1).lower()}_{match.group(2)}d",
        compact,
    )
    compact = re.sub(
        r"\\frac\{close_t\}\{ma_(\d+)d\}\s*-\s*1",
        lambda match: f"(close) / (ts_mean(close, {match.group(1)})) - 1",
        compact,
    )

    window_functions = {
        "mean": "ts_mean",
        "max": "ts_max",
        "min": "ts_min",
    }
    for function_name, replacement in window_functions.items():
        compact = re.sub(
            rf"{function_name}\((\w+)_\{{t-(\d+):t\}}\)",
            lambda match: f"{replacement}({match.group(1).lower()}, {int(match.group(2)) + 1})",
            compact,
        )

    compact = re.sub(
        r"([A-Za-z]+)_\{t-(\d+)\}",
        lambda match: f"delay({match.group(1).lower()}, {match.group(2)})",
        compact,
    )
    compact = re.sub(
        r"([A-Za-z]+)_t",
        lambda match: match.group(1).lower(),
        compact,
    )

    while True:
        updated = re.sub(r"\\frac\{([^{}]+)\}\{([^{}]+)\}", r"(\1) / (\2)", compact)
        if updated == compact:
            break
        compact = updated

    latex_patterns = (
        (
            re.compile(r"^M_\{(\d+)d\}\s*=\s*\\frac\{close_t\}\{close_\{t-(\d+)\}\}\s*-\s*1$"),
            lambda match: f"ret_{match.group(1)}d" if match.group(1) == match.group(2) else normalized,
        ),
        (
            re.compile(
                r"^VR_\{(\d+)d\}\s*=\s*\\frac\{volume_t\}\{\\frac\{1\}\{\d+\}\s*\\sum_\{i=0\}\^\{\d+\}\s*volume_\{t-i\}\}$"
            ),
            lambda match: f"volume / ts_mean(volume, {match.group(1)})",
        ),
        (
            re.compile(r"^CR_\{HL\}\s*=\s*\\frac\{close_t\s*-\s*low_t\}\{high_t\s*-\s*low_t\}$"),
            lambda match: "(close - low) / (high - low)",
        ),
        (
            re.compile(r"^\\sigma_\{(\d+)d\}\s*=.*$"),
            lambda match: f"ts_std(ret_1d, {match.group(1)})",
        ),
    )
    for pattern, replacement in latex_patterns:
        match = pattern.match(compact)
        if match:
            replaced = replacement(match)
            if isinstance(replaced, str) and replaced != normalized:
                normalized = replaced
                break
    else:
        normalized = compact

    normalized = re.sub(r"\$(\w+)", r"\1", normalized)
    replacements = {
        r"(?<![\w.])rolling_mean\s*\(": "ts_mean(",
        r"(?<![\w.])rolling_std\s*\(": "ts_std(",
        r"(?<![\w.])rolling_sum\s*\(": "ts_sum(",
        r"(?<![\w.])rolling_max\s*\(": "ts_max(",
        r"(?<![\w.])rolling_min\s*\(": "ts_min(",
        r"(?<![\w.])mean\s*\(": "ts_mean(",
        r"(?<![\w.])std\s*\(": "ts_std(",
        r"(?<![\w.])sum\s*\(": "ts_sum(",
        r"(?<![\w.])max\s*\(": "ts_max(",
        r"(?<![\w.])min\s*\(": "ts_min(",
        r"(?<![\w.])corr\s*\(": "ts_corr(",
    }
    for pattern, replacement in replacements.items():
        normalized = re.sub(pattern, replacement, normalized)

    aliases = {
        r"\bdaily_return\b": "ret_1d",
        r"\breturn_1d\b": "ret_1d",
        r"\breturn_5d\b": "ret_5d",
        r"\breturn_10d\b": "ret_10d",
        r"\breturn_20d\b": "ret_20d",
        r"\bmomentum_5d\b": "ret_5d",
        r"\bmomentum_10d\b": "ret_10d",
        r"\bmomentum_20d\b": "ret_20d",
        r"\breturn\b": "ret_1d",
    }
    for pattern, replacement in aliases.items():
        normalized = re.sub(pattern, replacement, normalized)
    return normalized


def _compute_long_short_sharpe(factor_values, forward_returns) -> float:
    import pandas as pd
    import numpy as np

    aligned = pd.DataFrame({"factor": factor_values, "return": forward_returns}).dropna()
    if aligned.empty or len(aligned) < 10:
        return 0.0

    if "date" in aligned.index.names:
        dates = aligned.index.get_level_values("date")
    elif "datetime" in aligned.index.names:
        dates = aligned.index.get_level_values("datetime")
    else:
        dates = aligned.index.get_level_values(1)
    aligned["_date"] = dates

    spreads = []
    for _, group in aligned.groupby("_date"):
        if len(group) < 10:
            continue
        ranked = group.sort_values("factor")
        bucket_size = max(len(ranked) // 5, 1)
        spreads.append(ranked["return"].iloc[-bucket_size:].mean() - ranked["return"].iloc[:bucket_size].mean())

    if len(spreads) < 2:
        return 0.0

    spread_series = pd.Series(spreads, dtype=float).dropna()
    if len(spread_series) < 2:
        return 0.0
    spread_std = float(spread_series.std())
    if spread_std <= 1e-9:
        return 0.0
    return round(float(spread_series.mean()) / spread_std * np.sqrt(252.0), 4)


def _count_completed_iterations(iterations: list[Dict[str, Any]]) -> int:
    observed_numbers = [
        int(it["iteration"])
        for it in iterations
        if isinstance(it, dict) and isinstance(it.get("iteration"), int)
    ]
    if observed_numbers:
        return max(observed_numbers)
    return len(iterations)


def _resolve_total_iterations(config_dict: Dict[str, Any], completed_iterations: int) -> int:
    configured_total = config_dict.get("max_iterations") if isinstance(config_dict, dict) else None
    try:
        if configured_total is not None:
            return max(int(configured_total), completed_iterations)
    except (TypeError, ValueError):
        pass
    return completed_iterations

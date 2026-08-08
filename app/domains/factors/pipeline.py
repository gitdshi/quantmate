"""Factor mining → screen → persist pipeline (TASK-004).

Stitches together the previously-disconnected pieces:
    - mining (Alpha158 / custom / RD-Agent discovered)
    - evaluation (IC / IR / coverage)
    - persistence (factor_screening_results + factor_screening_details)
    - optional promotion into the multi-factor engine config

The pipeline is invoked from a background worker task and from the
``POST /api/v1/factors/mine-and-screen`` route.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


def run_factor_mining_pipeline(
    *,
    user_id: int,
    source: str = "alpha158",  # alpha158 | custom | rdagent
    universe: str = "csi300",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    ic_threshold: float = 0.03,
    ir_threshold: float = 0.5,
    top_n: int = 20,
    auto_promote: bool = False,
) -> Dict[str, Any]:
    """Run the full factor mining pipeline.

    Returns a summary dict with screening_id, total_factors, passed_factors,
    and top_factors list.
    """
    screening_id = str(uuid.uuid4())
    run_label = f"{source}-{screening_id[:8]}"

    # Stage 1: Mine candidate factor expressions
    # For alpha158, mining already returns evaluated factors with metrics.
    mined_factors = _mine_factors(source, universe, start_date, end_date)
    logger.info(
        "[pipeline %s] Mined %d factors from %s", screening_id, len(mined_factors), source
    )

    # Create screening_results parent row — returns the auto-increment run_id.
    run_id = _create_screening_record(
        screening_id=screening_id,
        user_id=user_id,
        run_label=run_label,
        source=source,
        universe=universe,
        start_date=start_date,
        end_date=end_date,
        total=len(mined_factors),
        config={
            "ic_threshold": ic_threshold,
            "ir_threshold": ir_threshold,
            "top_n": top_n,
            "auto_promote": auto_promote,
        },
    )

    # Stage 2+3: Evaluate each factor and collect metrics
    # For alpha158, mining already returns dicts with metrics (ic_mean, ic_ir, etc.).
    # For custom/rdagent, mining returns expression strings that need evaluation.
    results: List[Dict[str, Any]] = []
    for item in mined_factors:
        if isinstance(item, dict) and "ic_mean" in item:
            # Already evaluated by mining (alpha158)
            metrics = dict(item)
            factor_name = metrics.get("factor_name") or metrics.get("name") or "unknown"
            passed = (
                abs(metrics.get("ic_ir", 0) or metrics.get("rank_ir", 0) or 0) >= ir_threshold
                and abs(metrics.get("ic_mean", 0) or 0) >= ic_threshold
            )
            metrics["factor_name"] = factor_name
            metrics["factor_source"] = source
            metrics["passed"] = passed
            results.append(metrics)
        else:
            # Need to evaluate (custom / rdagent expressions)
            factor_expr = item if isinstance(item, str) else (
                item.get("expression") or item.get("factor_name") or item.get("name")
                if isinstance(item, dict) else str(item)
            )
            if not factor_expr:
                continue
            try:
                metrics = _evaluate_factor(
                    factor_expr=str(factor_expr),
                    source=source,
                    universe=universe,
                    start_date=start_date,
                    end_date=end_date,
                )
                passed = (
                    abs(metrics.get("rank_ir", 0) or 0) >= ir_threshold
                    and abs(metrics.get("ic_mean", 0) or 0) >= ic_threshold
                )
                metrics["factor_name"] = str(factor_expr)
                metrics["factor_source"] = source
                metrics["passed"] = passed
                results.append(metrics)
            except Exception:
                logger.exception(
                    "[pipeline %s] Failed to evaluate factor: %s", screening_id, factor_expr
                )
                results.append(
                    {
                        "factor_name": str(factor_expr),
                        "factor_source": source,
                        "passed": False,
                        "error": True,
                    }
                )

    # Persist detail rows
    if run_id is not None:
        try:
            _save_screening_details(run_id, results)
        except Exception:
            logger.exception("[pipeline %s] Failed to persist details", screening_id)

    # Stage 4: Rank
    passed_results = [r for r in results if r.get("passed")]
    passed_results.sort(
        key=lambda r: abs(r.get("rank_ir", 0) or r.get("ic_ir", 0) or 0),
        reverse=True,
    )
    top_factors = passed_results[:top_n]

    if run_id is not None:
        _update_screening_record(
            run_id,
            total=len(results),
            passed=len(passed_results),
        )

    # Stage 5: Promote (optional)
    promoted: List[str] = []
    if auto_promote and top_factors:
        try:
            promoted = _promote_factors(top_factors, universe)
        except Exception:
            logger.exception("[pipeline %s] Promotion failed", screening_id)

    logger.info(
        "[pipeline %s] Done: total=%d passed=%d top=%d promoted=%d",
        screening_id,
        len(results),
        len(passed_results),
        len(top_factors),
        len(promoted),
    )

    return {
        "screening_id": screening_id,
        "run_id": run_id,
        "source": source,
        "universe": universe,
        "total_factors": len(results),
        "passed_factors": len(passed_results),
        "top_factors": [f["factor_name"] for f in top_factors],
        "promoted": promoted,
    }


# ---------------------------------------------------------------------------
# Stage implementations
# ---------------------------------------------------------------------------


def _mine_factors(
    source: str,
    universe: str,
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[str]:
    """Return a list of factor expressions for the given source."""
    sd = start_date.isoformat() if start_date else "2023-01-01"
    ed = end_date.isoformat() if end_date else "2024-12-31"

    if source == "alpha158":
        from app.domains.factors.factor_screening import mine_alpha158_factors

        mined = mine_alpha158_factors(
            start_date=sd,
            end_date=ed,
            instruments=universe,
        )
        # mine_alpha158_factors returns list[dict] with metrics already computed.
        # Return the dicts directly so the pipeline can skip re-evaluation.
        return list(mined or [])

    if source == "custom":
        # Pull registered custom factors from the FactorLab table.
        try:
            with connection("quantmate") as conn:
                rows = conn.execute(
                    text(
                        "SELECT expression FROM factor_lab WHERE status = 'active'"
                    )
                ).fetchall()
            return [r[0] for r in rows if r[0]]
        except Exception:
            logger.warning("[pipeline] Failed to load custom factors", exc_info=True)
            return []

    if source == "rdagent":
        try:
            with connection("qlib") as conn:
                rows = conn.execute(
                    text(
                        "SELECT expression FROM rdagent_discovered_factors "
                        "WHERE status = 'discovered' OR status = 'validated'"
                    )
                ).fetchall()
            return [r[0] for r in rows if r[0]]
        except Exception:
            logger.warning("[pipeline] Failed to load RD-Agent factors", exc_info=True)
            return []

    raise ValueError(f"Unknown factor source: {source!r}")


def _evaluate_factor(
    *,
    factor_expr: str,
    source: str,
    universe: str,
    start_date: Optional[date],
    end_date: Optional[date],
) -> Dict[str, Any]:
    """Evaluate a single factor expression and return metrics dict."""
    from app.domains.factors.expression_engine import (
        compute_factor_metrics,
        compute_forward_returns,
        compute_custom_factor,
        augment_factor_eval_ohlcv,
        fetch_ohlcv,
    )
    from app.domains.factors.factor_screening import normalize_factor_expression

    sd = start_date or date(2023, 1, 1)
    ed = end_date or date(2024, 12, 31)

    ohlcv = fetch_ohlcv(start_date=sd, end_date=ed, instruments=None)
    if ohlcv.empty:
        return {"ic_mean": 0.0, "rank_ir": 0.0, "coverage": 0.0}

    eval_ohlcv = augment_factor_eval_ohlcv(ohlcv)
    fwd_returns = compute_forward_returns(ohlcv, periods=1)

    try:
        fv = compute_custom_factor(factor_expr, eval_ohlcv)
    except Exception:
        normalized = normalize_factor_expression(factor_expr)
        if normalized == factor_expr:
            raise
        fv = compute_custom_factor(normalized, eval_ohlcv)

    metrics = compute_factor_metrics(fv, fwd_returns)
    metrics["coverage"] = float(fv.notna().mean())
    return metrics


def _create_screening_record(
    *,
    screening_id: str,
    user_id: int,
    run_label: str,
    source: str,
    universe: str,
    start_date: Optional[date],
    end_date: Optional[date],
    total: int,
    config: Dict[str, Any],
) -> Optional[int]:
    """Insert a factor_screening_results row and return its auto-increment id."""
    import json

    with connection("quantmate") as conn:
        result = conn.execute(
            text(
                "INSERT INTO factor_screening_results "
                "(user_id, run_label, config, result_count, status) "
                "VALUES (:uid, :label, :cfg, :cnt, 'running')"
            ),
            {
                "uid": user_id,
                "label": run_label,
                "cfg": json.dumps(
                    {
                        **config,
                        "screening_id": screening_id,
                        "source": source,
                        "universe": universe,
                        "start_date": start_date.isoformat() if start_date else None,
                        "end_date": end_date.isoformat() if end_date else None,
                    }
                ),
                "cnt": total,
            },
        )
        conn.commit()
        return result.lastrowid


def _update_screening_record(run_id: int, total: int, passed: int) -> None:
    try:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    "UPDATE factor_screening_results SET "
                    "result_count = :total, passed_count = :passed, "
                    "status = 'completed', completed_at = NOW() "
                    "WHERE id = :rid"
                ),
                {"total": total, "passed": passed, "rid": run_id},
            )
            conn.commit()
    except Exception:
        # passed_count column may not exist on some deployments; fall back silently.
        logger.debug("[pipeline] Failed to update screening record", exc_info=True)


def _save_screening_details(run_id: int, results: List[Dict[str, Any]]) -> None:
    """Persist per-factor evaluation metrics to factor_screening_details."""
    if not results:
        return

    import json

    rows = []
    for idx, r in enumerate(results):
        rows.append(
            {
                "run_id": run_id,
                "rank_order": idx + 1,
                "factor_name": r.get("factor_name", "")[:200],
                "factor_source": r.get("factor_source", "")[:64],
                "ic_mean": float(r.get("ic_mean") or 0),
                "rank_ir": float(r.get("rank_ir") or 0),
                "passed": 1 if r.get("passed") else 0,
                "metrics": json.dumps(r),
            }
        )

    with connection("quantmate") as conn:
        conn.execute(
            text(
                "INSERT INTO factor_screening_details "
                "(run_id, rank_order, factor_name, factor_source, ic_mean, rank_ir, passed, metrics) "
                "VALUES (:run_id, :rank_order, :factor_name, :factor_source, :ic_mean, :rank_ir, :passed, :metrics)"
            ),
            rows,
        )
        conn.commit()


def _promote_factors(top_factors: List[Dict[str, Any]], universe: str) -> List[str]:
    """Mark top factors as recommended in the multi_factor_engine config.

    Best-effort: if the table/column doesn't exist, returns [] silently.
    """
    promoted: List[str] = []
    for f in top_factors:
        name = f.get("factor_name")
        if not name:
            continue
        try:
            with connection("quantmate") as conn:
                conn.execute(
                    text(
                        "INSERT INTO factor_recommendations "
                        "(factor_name, factor_source, universe, rank_ir, recommended_at) "
                        "VALUES (:name, :src, :uni, :rir, NOW()) "
                        "ON DUPLICATE KEY UPDATE rank_ir = VALUES(rank_ir), recommended_at = NOW()"
                    ),
                    {
                        "name": name[:256],
                        "src": f.get("factor_source", "")[:64],
                        "uni": universe[:64],
                        "rir": float(f.get("rank_ir") or f.get("ic_ir") or 0),
                    },
                )
                conn.commit()
            promoted.append(name)
        except Exception:
            logger.debug("[pipeline] Promotion table missing, skipping", exc_info=True)
            break
    return promoted

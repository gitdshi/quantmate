"""Autopilot decision engine.

Two pure rule sets:
  - research side: select factors + direction + weight from candidate metrics
  - trading side: map live paper performance to an action (continue/reduce/stop/weight)

The engine is pure (no DB writes); persistence is the caller's responsibility.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from app.domains.autopilot.policies import Policies


# ── Research side: factor selection ───────────────────────────────────────


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        f = float(value)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _factor_metric(candidate: Dict[str, Any]) -> Dict[str, float]:
    name = (
        candidate.get("factor_name")
        or candidate.get("name")
        or candidate.get("expression")
        or "unknown"
    )
    ic_mean = _num(candidate.get("ic_mean"))
    ic_ir = _num(candidate.get("ic_ir")) or _num(candidate.get("rank_ir"))
    rank_ir = _num(candidate.get("rank_ir")) or ic_ir
    turnover = _num(candidate.get("turnover"))
    corr = _num(candidate.get("corr"), default=0.0) if candidate.get("corr") is not None else None
    return {
        "name": str(name),
        "ic_mean": ic_mean,
        "ic_ir": ic_ir,
        "rank_ir": rank_ir,
        "turnover": turnover,
        "corr": corr if corr is not None else 0.0,
        "has_corr": corr is not None,
    }


def select_factors(
    candidates: List[Dict[str, Any]],
    policies: Policies,
    previous_factors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Select factors from candidate metrics (research-side decision).

    Returns ``{"factors": [...], "rejected": [...]}`` where each factor entry has
    ``name/direction/weight/ic_mean/ic_ir``.
    """
    previous = set(previous_factors or [])

    # 1. Filter by IC/IR thresholds and turnover cap.
    passed: List[Dict[str, Any]] = []
    rejected: List[Dict[str, str]] = []
    for c in candidates:
        m = _factor_metric(c)
        if abs(m["rank_ir"]) < policies.ir_threshold:
            rejected.append({"name": m["name"], "reason": "rank_ir below threshold"})
            continue
        if abs(m["ic_mean"]) < policies.ic_threshold:
            rejected.append({"name": m["name"], "reason": "ic_mean below threshold"})
            continue
        if m["turnover"] >= policies.turnover_cap:
            rejected.append({"name": m["name"], "reason": "turnover above cap"})
            continue
        passed.append({**c, "_m": m})

    # 2. Correlation dedup: keep higher |IC| within correlated pairs.
    if policies.corr_threshold > 0:
        selected: List[Dict[str, Any]] = []
        for c in sorted(passed, key=lambda x: abs(x["_m"]["ic_mean"]), reverse=True):
            dup = False
            for s in selected:
                if s["_m"].get("has_corr") and abs(_corr_between(c, s)) > policies.corr_threshold:
                    dup = True
                    break
            if dup:
                rejected.append({"name": c["_m"]["name"], "reason": "correlated with higher-IC factor"})
            else:
                selected.append(c)
    else:
        selected = passed

    # 3. Direction + stability weighting.
    factors: List[Dict[str, Any]] = []
    for c in selected:
        m = c["_m"]
        direction = 1.0 if m["ic_mean"] >= 0 else -1.0
        weight = 1.5 if m["name"] in previous else 1.0
        factors.append(
            {
                "name": m["name"],
                "expression": c.get("expression") or m["name"],
                "direction": direction,
                "weight": weight,
                "ic_mean": m["ic_mean"],
                "ic_ir": m["ic_ir"],
                "rank_ir": m["rank_ir"],
            }
        )

    # 4. Rank and normalize weights, take top N.
    factors.sort(key=lambda f: (abs(f["ic_ir"]), f["weight"]), reverse=True)
    factors = factors[: policies.top_n_factor]
    total_weight = sum(f["weight"] for f in factors) or 1.0
    for f in factors:
        f["weight"] = round(f["weight"] / total_weight, 4)

    return {"factors": factors, "rejected": rejected}


def _corr_between(a: Dict[str, Any], b: Dict[str, Any]) -> float:
    # Correlation is precomputed on candidates; without a shared metric, treat
    # non-provided correlation as 0 (no dedup).
    if not a["_m"].get("has_corr") or not b["_m"].get("has_corr"):
        return 0.0
    # Candidates from the same mining run may carry pairwise corr as 'corr' only
    # for the representative factor; fall back to 0 unless explicit.
    pair_corr = a.get("_pair_corr", {}).get(b["_m"]["name"], 0.0)
    return float(pair_corr or 0.0)


# ── Trading side: performance evaluation ──────────────────────────────────


def evaluate_performance(
    analytics: Dict[str, Any],
    policies: Policies,
    history: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Map live paper performance to a strategy action.

    Rule order: drawdown stop > sharpe floor (reduce) > healthy (weight).
    """
    history = history or []
    max_drawdown_pct = _num(analytics.get("max_drawdown_pct"), default=0.0)
    sharpe = analytics.get("sharpe_ratio")
    sharpe_val = _num(sharpe, default=float("inf")) if analytics.get("sharpe_ratio") is not None else None

    # 1. Drawdown stop.
    if max_drawdown_pct >= policies.max_drawdown_stop_pct:
        return {
            "action": "stop",
            "reason": (
                f"max_drawdown_pct {max_drawdown_pct:.2f}% >= stop threshold "
                f"{policies.max_drawdown_stop_pct}%"
            ),
            "feedback": [],
        }

    # 2. Sharpe floor: below floor for N consecutive days → reduce.
    below_floor = (
        sharpe_val is not None
        and sharpe_val < policies.sharpe_floor
    )
    if below_floor:
        streak = _count_below_floor(history, policies)
        if streak >= policies.sharpe_floor_days:
            worst = _worst_factor(history)
            return {
                "action": "reduce",
                "reason": (
                    f"sharpe {sharpe_val:.4f} < floor {policies.sharpe_floor} for "
                    f"{streak} days (threshold {policies.sharpe_floor_days})"
                ),
                "feedback": [{"factor": worst, "action": "eliminate"}] if worst else [],
            }

    # 3. Healthy → keep (and optionally weight stable factors).
    return {
        "action": "weight" if sharpe_val is not None and sharpe_val >= policies.sharpe_floor else "continue",
        "reason": "performance within guardrails",
        "feedback": [],
    }


def _count_below_floor(history: List[Dict[str, Any]], policies: Policies) -> int:
    streak = 0
    for entry in reversed(history):
        s = entry.get("sharpe_ratio")
        val = _num(s, default=float("inf")) if s is not None else None
        if val is not None and val < policies.sharpe_floor:
            streak += 1
        else:
            break
    return streak


def _worst_factor(history: List[Dict[str, Any]]) -> Optional[str]:
    worst_name: Optional[str] = None
    worst_ic = float("inf")
    for entry in history:
        for f in entry.get("factors", []) or []:
            ic = abs(_num(f.get("ic_ir")))
            if ic < worst_ic:
                worst_ic = ic
                worst_name = f.get("name") or f.get("factor_name")
    return worst_name
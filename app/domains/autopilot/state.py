"""Autopilot stage and status enums plus the stage dependency state machine."""

from __future__ import annotations

from enum import Enum


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    ABORTED = "aborted"


class StageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class Stage(str, Enum):
    """Autopilot stages in dependency order (D1 data sync is owned by datasync daemon)."""

    QLIB_INGEST = "d2_qlib_ingest"
    FACTOR_MINING = "r1_factor_mining"
    FACTOR_BACKTEST = "r2_factor_backtest"
    FACTOR_SELECTION = "r3_factor_selection"
    STRATEGY_DEPLOY = "r4_strategy_deploy"
    PREMARKET_CHECK = "r5_premarket_check"
    SETTLEMENT = "e1_settlement"
    ANALYSIS = "e2_analysis"
    STRATEGY_ADJUST = "e3_strategy_adjust"


# Dependency order: research loop first, then trading loop.
STAGE_ORDER: tuple[Stage, ...] = (
    Stage.QLIB_INGEST,
    Stage.FACTOR_MINING,
    Stage.FACTOR_BACKTEST,
    Stage.FACTOR_SELECTION,
    Stage.STRATEGY_DEPLOY,
    Stage.PREMARKET_CHECK,
    Stage.SETTLEMENT,
    Stage.ANALYSIS,
    Stage.STRATEGY_ADJUST,
)

# Stages that belong to the research loop (require data-quality gate).
RESEARCH_STAGES: frozenset[Stage] = frozenset(
    {
        Stage.QLIB_INGEST,
        Stage.FACTOR_MINING,
        Stage.FACTOR_BACKTEST,
        Stage.FACTOR_SELECTION,
        Stage.STRATEGY_DEPLOY,
    }
)

# Stages that belong to the trading loop (trade-day only).
TRADING_STAGES: frozenset[Stage] = frozenset(
    {
        Stage.PREMARKET_CHECK,
        Stage.SETTLEMENT,
        Stage.ANALYSIS,
        Stage.STRATEGY_ADJUST,
    }
)


def is_terminal(status: StageStatus) -> bool:
    return status in {StageStatus.SUCCESS, StageStatus.FAILED, StageStatus.SKIPPED}


def previous_stage(stage: Stage) -> Stage | None:
    idx = STAGE_ORDER.index(stage)
    return STAGE_ORDER[idx - 1] if idx > 0 else None
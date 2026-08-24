"""Autopilot configurable policies (thresholds and switches).

All values are read from ``system_config`` via the ``autopilot.*`` keys and
fall back to environment variables, so the frontend SystemConfigTab and env
both work. Values are cached by the runtime-config layer (30s TTL).
"""

from __future__ import annotations

from dataclasses import dataclass

from app.infrastructure.config import get_runtime_bool, get_runtime_float, get_runtime_int, get_runtime_str

_DB_KEY_PREFIX = "autopilot"


@dataclass(frozen=True)
class Policies:
    enabled: bool
    kill_switch: bool
    approval_required: bool

    ic_threshold: float
    ir_threshold: float
    corr_threshold: float
    turnover_cap: float
    top_n_factor: int
    max_position_pct: float

    max_drawdown_stop_pct: float
    sharpe_floor: float
    sharpe_floor_days: int

    min_overlap_days: int
    universe: str
    user_id: int
    paper_account_id: int

    init_backtest_years: int

    @classmethod
    def load(cls) -> "Policies":
        return cls(
            enabled=get_runtime_bool(env_keys="AUTOPILOT_ENABLED", db_key=f"{_DB_KEY_PREFIX}.enabled", default=True),
            kill_switch=get_runtime_bool(env_keys="AUTOPILOT_KILL_SWITCH", db_key=f"{_DB_KEY_PREFIX}.kill_switch", default=False),
            approval_required=get_runtime_bool(env_keys="AUTOPILOT_APPROVAL_REQUIRED", db_key=f"{_DB_KEY_PREFIX}.approval_required", default=True),
            ic_threshold=get_runtime_float(env_keys="AUTOPILOT_IC_THRESHOLD", db_key=f"{_DB_KEY_PREFIX}.ic_threshold", default=0.03),
            ir_threshold=get_runtime_float(env_keys="AUTOPILOT_IR_THRESHOLD", db_key=f"{_DB_KEY_PREFIX}.ir_threshold", default=0.5),
            corr_threshold=get_runtime_float(env_keys="AUTOPILOT_CORR_THRESHOLD", db_key=f"{_DB_KEY_PREFIX}.corr_threshold", default=0.7),
            turnover_cap=get_runtime_float(env_keys="AUTOPILOT_TURNOVER_CAP", db_key=f"{_DB_KEY_PREFIX}.turnover_cap", default=0.5),
            top_n_factor=get_runtime_int(env_keys="AUTOPILOT_TOP_N_FACTOR", db_key=f"{_DB_KEY_PREFIX}.top_n_factor", default=10),
            max_position_pct=get_runtime_float(env_keys="AUTOPILOT_MAX_POSITION_PCT", db_key=f"{_DB_KEY_PREFIX}.max_position_pct", default=0.1),
            max_drawdown_stop_pct=get_runtime_float(env_keys="AUTOPILOT_MAX_DRAWDOWN_STOP_PCT", db_key=f"{_DB_KEY_PREFIX}.max_drawdown_stop_pct", default=20.0),
            sharpe_floor=get_runtime_float(env_keys="AUTOPILOT_SHARPE_FLOOR", db_key=f"{_DB_KEY_PREFIX}.sharpe_floor", default=0.0),
            sharpe_floor_days=get_runtime_int(env_keys="AUTOPILOT_SHARPE_FLOOR_DAYS", db_key=f"{_DB_KEY_PREFIX}.sharpe_floor_days", default=3),
            min_overlap_days=get_runtime_int(env_keys="AUTOPILOT_MIN_OVERLAP_DAYS", db_key=f"{_DB_KEY_PREFIX}.min_overlap_days", default=60),
            universe=get_runtime_str(env_keys="AUTOPILOT_UNIVERSE", db_key=f"{_DB_KEY_PREFIX}.universe", default="csi300"),
            user_id=get_runtime_int(env_keys="AUTOPILOT_USER_ID", db_key=f"{_DB_KEY_PREFIX}.user_id", default=0),
            paper_account_id=get_runtime_int(env_keys="AUTOPILOT_PAPER_ACCOUNT_ID", db_key=f"{_DB_KEY_PREFIX}.paper_account_id", default=0),
            init_backtest_years=get_runtime_int(env_keys="AUTOPILOT_INIT_BACKTEST_YEARS", db_key=f"{_DB_KEY_PREFIX}.init_backtest_years", default=3),
        )
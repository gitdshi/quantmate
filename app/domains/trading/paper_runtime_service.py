"""Paper runtime service.

This service is the control surface for paper trading runtime orchestration.
It owns runtime sessions and paper gateways, and dispatches either to the
CTA bridge or the portfolio-strategy bridge.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import logging
import threading
from typing import Any, Dict, Optional

from app.domains.trading.paper_gateway import PaperGateway

logger = logging.getLogger(__name__)


class PaperStrategyKind(str, Enum):
    CTA = "cta"
    PORTFOLIO = "portfolio"


class PaperRuntimeMode(str, Enum):
    NATIVE_CTA_RUNTIME = "native_cta_runtime"
    LEGACY_EXECUTOR_BRIDGE = "legacy_executor_bridge"
    PORTFOLIO_STRATEGY_BRIDGE = "portfolio_strategy_bridge"
    VNPY_PAPER_GATEWAY = "vnpy_paper_gateway"


@dataclass(slots=True)
class PaperRuntimeSession:
    deployment_id: int
    paper_account_id: int
    user_id: int
    strategy_id: Optional[int]
    strategy_name: str
    vt_symbol: str
    parameters: Dict[str, Any]
    strategy_kind: PaperStrategyKind
    runtime_mode: PaperRuntimeMode
    gateway_name: str
    status: str = "starting"
    warnings: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None


class PaperRuntimeService:
    """Registry-backed entrypoint for paper runtime lifecycle."""

    _instance: Optional["PaperRuntimeService"] = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "PaperRuntimeService":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._lock = threading.Lock()
        self._sessions: dict[int, PaperRuntimeSession] = {}
        self._gateways: dict[int, PaperGateway] = {}

    def start_deployment(
        self,
        *,
        deployment_id: int,
        paper_account_id: int,
        user_id: int,
        strategy_id: Optional[int],
        strategy_name: str,
        vt_symbol: str,
        parameters: Optional[Dict[str, Any]] = None,
        execution_mode: str = "auto",
    ) -> Dict[str, Any]:
        with self._lock:
            existing = self._sessions.get(deployment_id)
            if existing and existing.status not in {"stopped", "failed"}:
                return {
                    "success": False,
                    "error": "Deployment runtime already registered",
                    "runtime": self._session_payload(existing),
                }

            strategy_kind = self._infer_strategy_kind(
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                vt_symbol=vt_symbol,
            )
            runtime_mode = self._runtime_mode_for(strategy_kind)
            gateway = PaperGateway(gateway_name=f"PAPER.{deployment_id}")
            session = PaperRuntimeSession(
                deployment_id=deployment_id,
                paper_account_id=paper_account_id,
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                vt_symbol=vt_symbol,
                parameters=dict(parameters or {}),
                strategy_kind=strategy_kind,
                runtime_mode=runtime_mode,
                gateway_name=gateway.gateway_name,
            )
            self._sessions[deployment_id] = session
            self._gateways[deployment_id] = gateway

        if session.strategy_kind == PaperStrategyKind.PORTFOLIO:
            start_result = self._start_portfolio_executor(
                session=session,
                execution_mode=execution_mode,
                gateway=gateway,
            )
        else:
            start_result = self._start_legacy_executor(
                session=session,
                execution_mode=execution_mode,
                gateway=gateway,
            )

        if not start_result.get("success"):
            warning = start_result.get("error", "Paper executor failed to start")
            session.status = "failed"
            session.warnings.append(warning)
            session.stopped_at = datetime.utcnow()
            logger.warning("[paper-runtime] deployment %s failed to start: %s", deployment_id, warning)
            return {
                "success": False,
                "error": warning,
                "runtime": self._session_payload(session),
            }

        session.status = "running"
        session.started_at = datetime.utcnow()
        return {
            "success": True,
            "runtime": self._session_payload(session),
        }

    def preview_runtime(
        self,
        *,
        deployment_id: int,
        paper_account_id: int,
        user_id: int,
        strategy_id: Optional[int],
        strategy_name: str,
        vt_symbol: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        strategy_kind = self._infer_strategy_kind(
            user_id=user_id,
            strategy_id=strategy_id,
            strategy_name=strategy_name,
            vt_symbol=vt_symbol,
        )
        runtime_mode = self._runtime_mode_for(strategy_kind)
        session = PaperRuntimeSession(
            deployment_id=deployment_id,
            paper_account_id=paper_account_id,
            user_id=user_id,
            strategy_id=strategy_id,
            strategy_name=strategy_name,
            vt_symbol=vt_symbol,
            parameters=dict(parameters or {}),
            strategy_kind=strategy_kind,
            runtime_mode=runtime_mode,
            gateway_name=f"PAPER.{deployment_id}",
            status="pending",
        )
        return self._session_payload(session)

    def stop_deployment(self, deployment_id: int) -> Dict[str, Any]:
        session = self._sessions.get(deployment_id)
        if session and session.strategy_kind == PaperStrategyKind.PORTFOLIO:
            stopped = self._stop_portfolio_executor(deployment_id)
        else:
            stopped = self._stop_legacy_executor(deployment_id)
        if session is None:
            return {"success": stopped, "runtime": None}

        session.status = "stopped"
        session.stopped_at = datetime.utcnow()
        return {
            "success": True,
            "runtime": self._session_payload(session),
        }

    def get_runtime(self, deployment_id: int) -> Optional[Dict[str, Any]]:
        session = self._sessions.get(deployment_id)
        if session is None:
            return None
        return self._session_payload(session)

    def _session_payload(self, session: PaperRuntimeSession) -> Dict[str, Any]:
        payload = asdict(session)
        payload["strategy_kind"] = session.strategy_kind.value
        payload["runtime_mode"] = session.runtime_mode.value
        payload["capabilities"] = self._capabilities_for(session)
        return payload

    @staticmethod
    def _infer_strategy_kind(
        *,
        user_id: int,
        strategy_id: Optional[int],
        strategy_name: str,
        vt_symbol: str,
    ) -> PaperStrategyKind:
        strategy_cls = PaperRuntimeService._load_strategy_class(
            user_id=user_id,
            strategy_id=strategy_id,
            strategy_name=strategy_name,
        )
        if strategy_cls is not None:
            try:
                from vnpy_portfoliostrategy import StrategyTemplate as PortfolioStrategyTemplate

                if issubclass(strategy_cls, PortfolioStrategyTemplate):
                    return PaperStrategyKind.PORTFOLIO
            except Exception:
                logger.debug("[paper-runtime] failed to inspect portfolio strategy type", exc_info=True)

            try:
                from vnpy_ctastrategy import CtaTemplate

                if issubclass(strategy_cls, CtaTemplate):
                    return PaperStrategyKind.CTA
            except Exception:
                logger.debug("[paper-runtime] failed to inspect CTA strategy type", exc_info=True)

        if "," in vt_symbol:
            return PaperStrategyKind.PORTFOLIO
        return PaperStrategyKind.CTA

    @staticmethod
    def _runtime_mode_for(strategy_kind: PaperStrategyKind) -> PaperRuntimeMode:
        if strategy_kind == PaperStrategyKind.PORTFOLIO:
            return PaperRuntimeMode.PORTFOLIO_STRATEGY_BRIDGE
        return PaperRuntimeMode.NATIVE_CTA_RUNTIME

    @staticmethod
    def _load_strategy_class(
        *,
        user_id: int,
        strategy_id: Optional[int],
        strategy_name: str,
    ) -> Optional[type[Any]]:
        try:
            from app.api.services.strategy_service import compile_strategy
            from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao

            source_dao = StrategySourceDao()
            if strategy_id is not None:
                code, class_name, _version = source_dao.get_strategy_source_for_user(strategy_id, user_id)
                return compile_strategy(code, class_name or strategy_name)

            code = source_dao.get_strategy_code_by_class_name(strategy_name)
            return compile_strategy(code, strategy_name)
        except Exception:
            logger.debug("[paper-runtime] failed to load strategy class for runtime inference", exc_info=True)
            return None

    @staticmethod
    def _capabilities_for(session: PaperRuntimeSession) -> Dict[str, bool]:
        return {
            "legacy_executor_bridge": session.runtime_mode == PaperRuntimeMode.LEGACY_EXECUTOR_BRIDGE,
            "native_gateway_execution": session.runtime_mode == PaperRuntimeMode.NATIVE_CTA_RUNTIME,
            "portfolio_runtime": session.strategy_kind == PaperStrategyKind.PORTFOLIO,
            "native_tick_feed": session.strategy_kind == PaperStrategyKind.CTA,
            "checkpoint_recovery": True,
            "recovery": False,
        }

    @staticmethod
    def _start_legacy_executor(*, session: PaperRuntimeSession, execution_mode: str, gateway: Any = None) -> Dict[str, Any]:
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor

        executor = PaperStrategyExecutor()
        return executor.start_deployment(
            deployment_id=session.deployment_id,
            paper_account_id=session.paper_account_id,
            user_id=session.user_id,
            strategy_class_name=session.strategy_name,
            vt_symbol=session.vt_symbol,
            parameters=session.parameters,
            execution_mode=execution_mode,
            strategy_id=session.strategy_id,
            gateway=gateway,
        )

    @staticmethod
    def _start_portfolio_executor(*, session: PaperRuntimeSession, execution_mode: str, gateway: Any = None) -> Dict[str, Any]:
        from app.domains.trading.paper_portfolio_executor import PaperPortfolioExecutor

        executor = PaperPortfolioExecutor()
        return executor.start_deployment(
            deployment_id=session.deployment_id,
            paper_account_id=session.paper_account_id,
            user_id=session.user_id,
            strategy_class_name=session.strategy_name,
            vt_symbol=session.vt_symbol,
            parameters=session.parameters,
            execution_mode=execution_mode,
            strategy_id=session.strategy_id,
            gateway=gateway,
        )

    @staticmethod
    def _stop_legacy_executor(deployment_id: int) -> bool:
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor

        executor = PaperStrategyExecutor()
        return executor.stop_deployment(deployment_id)

    @staticmethod
    def _stop_portfolio_executor(deployment_id: int) -> bool:
        from app.domains.trading.paper_portfolio_executor import PaperPortfolioExecutor

        executor = PaperPortfolioExecutor()
        return executor.stop_deployment(deployment_id)
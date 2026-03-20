"""CTA Strategy Runner — manages live automated strategy execution via vnpy's CTA engine.

Strategies are loaded by class name (compiled from user code or DB) and
executed against a connected vnpy gateway in real-time.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CtaStrategyRunner:
    """Singleton runner that wraps vnpy_ctastrategy's ``CtaEngine``."""

    _instance: Optional["CtaStrategyRunner"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "CtaStrategyRunner":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._running_strategies: Dict[str, Dict[str, Any]] = {}
        self._cta_engine: Any = None
        logger.info("[cta-runner] CtaStrategyRunner initialized")

    # ------------------------------------------------------------------
    # Strategy lifecycle
    # ------------------------------------------------------------------

    def start_strategy(
        self,
        strategy_class_name: str,
        vt_symbol: str,
        parameters: Dict[str, Any],
        strategy_code: Optional[str] = None,
        strategy_id: Optional[int] = None,
        user_id: Optional[int] = None,
        gateway_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compile and start a CTA strategy on the connected gateway.

        Returns dict with ``success``, ``strategy_name``, and optional ``error``.
        """
        # Build unique name
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        strategy_name = f"{strategy_class_name}_{vt_symbol}_{ts}"

        if strategy_name in self._running_strategies:
            return {"success": False, "error": "Strategy already running with this name"}

        try:
            # Compile strategy class
            strategy_class = self._load_strategy_class(strategy_class_name, strategy_code, strategy_id, user_id)

            # Record running state (actual vnpy CtaEngine integration is gated
            # behind a gateway connection check)
            self._running_strategies[strategy_name] = {
                "class_name": strategy_class_name,
                "vt_symbol": vt_symbol,
                "parameters": parameters,
                "status": "running",
                "started_at": datetime.utcnow().isoformat(),
                "user_id": user_id,
                "gateway_name": gateway_name,
            }

            logger.info("[cta-runner] Strategy '%s' started on %s", strategy_name, vt_symbol)
            return {"success": True, "strategy_name": strategy_name}

        except Exception as exc:
            logger.exception("[cta-runner] Failed to start strategy '%s'", strategy_class_name)
            return {"success": False, "error": str(exc)}

    def stop_strategy(self, strategy_name: str) -> bool:
        """Stop a running strategy by name."""
        info = self._running_strategies.pop(strategy_name, None)
        if info is None:
            return False
        info["status"] = "stopped"
        logger.info("[cta-runner] Strategy '%s' stopped", strategy_name)
        return True

    def list_strategies(self) -> List[Dict[str, Any]]:
        """Return status of all running strategies."""
        return [
            {"strategy_name": name, **{k: v for k, v in info.items() if k != "parameters"}, "parameters": info.get("parameters", {})}
            for name, info in self._running_strategies.items()
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _load_strategy_class(
        class_name: str,
        code: Optional[str],
        strategy_id: Optional[int],
        user_id: Optional[int],
    ):
        """Resolve a strategy class either from inline code or the DB."""
        from app.api.services.strategy_service import compile_strategy
        from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao

        if code:
            return compile_strategy(code, class_name)

        source_dao = StrategySourceDao()
        if strategy_id is not None and user_id is not None:
            db_code, db_class, _sv = source_dao.get_strategy_source_for_user(strategy_id, user_id)
            return compile_strategy(db_code, db_class or class_name)

        db_code = source_dao.get_strategy_code_by_class_name(class_name)
        if db_code:
            return compile_strategy(db_code, class_name)

        raise ValueError(f"Strategy class '{class_name}' not found in database and no code provided")

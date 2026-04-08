"""Audit service for structured logging of key actions."""

from app.domains.audit.dao.audit_log_dao import AuditLogDao
from typing import Optional


class AuditService:
    """Service for writing audit logs with consistent action naming."""
    
    # Action name constants - matching P0 spec
    STRATEGY_VERSION_CREATE = "STRATEGY_VERSION_CREATE"
    STRATEGY_VERSION_RESTORE = "STRATEGY_VERSION_RESTORE"
    BACKTEST_SUBMIT_FROM_VERSION = "BACKTEST_SUBMIT_FROM_VERSION"
    PAPER_DEPLOY_FROM_BACKTEST = "PAPER_DEPLOY_FROM_BACKTEST"
    RISK_CHECK_PRE_LIVE = "RISK_CHECK_PRE_LIVE"
    
    def __init__(self):
        self._dao = AuditLogDao()
    
    def log_version_create(
        self,
        user_id: int,
        username: str,
        strategy_id: int,
        version_id: int,
        version_number: int,
    ) -> None:
        """Log strategy version creation."""
        self._dao.insert(
            user_id=user_id,
            username=username,
            operation_type=self.STRATEGY_VERSION_CREATE,
            resource_type="strategy_version",
            resource_id=str(version_id),
            details={
                "strategy_id": strategy_id,
                "version_number": version_number,
            },
        )
    
    def log_version_restore(
        self,
        user_id: int,
        username: str,
        strategy_id: int,
        source_version_id: int,
        new_version_id: int,
        new_version_number: int,
    ) -> None:
        """Log strategy version restore (creates new version)."""
        self._dao.insert(
            user_id=user_id,
            username=username,
            operation_type=self.STRATEGY_VERSION_RESTORE,
            resource_type="strategy_version",
            resource_id=str(new_version_id),
            details={
                "strategy_id": strategy_id,
                "source_version_id": source_version_id,
                "new_version_number": new_version_number,
            },
        )
    
    def log_backtest_submit(
        self,
        user_id: int,
        username: str,
        strategy_id: int,
        version_id: int,
        job_id: str,
    ) -> None:
        """Log backtest submission from strategy version."""
        self._dao.insert(
            user_id=user_id,
            username=username,
            operation_type=self.BACKTEST_SUBMIT_FROM_VERSION,
            resource_type="backtest",
            resource_id=job_id,
            details={
                "strategy_id": strategy_id,
                "version_id": version_id,
            },
        )
    
    def log_paper_deploy(
        self,
        user_id: int,
        username: str,
        deployment_id: int,
        strategy_id: int,
        source_backtest_job_id: Optional[str],
        source_version_id: Optional[int],
    ) -> None:
        """Log paper deployment creation from backtest."""
        self._dao.insert(
            user_id=user_id,
            username=username,
            operation_type=self.PAPER_DEPLOY_FROM_BACKTEST,
            resource_type="paper_deployment",
            resource_id=str(deployment_id),
            details={
                "strategy_id": strategy_id,
                "source_backtest_job_id": source_backtest_job_id,
                "source_version_id": source_version_id,
            },
        )
    
    def log_risk_check(
        self,
        user_id: int,
        username: str,
        result: str,  # pass/warn/block
        strategy_id: Optional[int] = None,
        version_id: Optional[int] = None,
        deployment_id: Optional[int] = None,
        triggered_rules: Optional[list] = None,
    ) -> None:
        """Log pre-live risk check execution."""
        self._dao.insert(
            user_id=user_id,
            username=username,
            operation_type=self.RISK_CHECK_PRE_LIVE,
            resource_type="risk_check",
            resource_id=str(deployment_id) if deployment_id else None,
            details={
                "result": result,
                "strategy_id": strategy_id,
                "version_id": version_id,
                "deployment_id": deployment_id,
                "triggered_rules": triggered_rules,
            },
        )


def get_audit_service() -> AuditService:
    """Get audit service singleton."""
    return AuditService()

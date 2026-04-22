from __future__ import annotations

from unittest.mock import MagicMock, patch


def _registry_for(*keys: tuple[str, str]) -> MagicMock:
    registry = MagicMock()
    supported = set(keys)
    registry.get_interface.side_effect = lambda source, item_key: object() if (source, item_key) in supported else None
    return registry


class TestIsItemSyncSupported:
    def test_tushare_points_threshold_controls_support(self):
        from app.datasync.capabilities import is_item_sync_supported

        registry = _registry_for(("tushare", "stock_daily"), ("tushare", "bak_daily"))
        source_configs = {"tushare": {"config_json": {"token_points": 2000}}}

        assert is_item_sync_supported(
            registry,
            {"source": "tushare", "item_key": "stock_daily", "permission_points": 120, "api_name": "daily"},
            source_configs=source_configs,
        ) is True
        assert is_item_sync_supported(
            registry,
            {"source": "tushare", "item_key": "bak_daily", "permission_points": 5000, "api_name": "bak_daily"},
            source_configs=source_configs,
        ) is False

    def test_tushare_special_permission_requires_explicit_grant(self):
        from app.datasync.capabilities import is_item_sync_supported

        registry = _registry_for(("tushare", "rt_daily"))

        assert is_item_sync_supported(
            registry,
            {
                "source": "tushare",
                "item_key": "rt_daily",
                "permission_points": 0,
                "requires_permission": "1",
                "api_name": "rt_daily",
            },
            source_configs={"tushare": {"config_json": {"token_points": 10000}}},
        ) is False

        assert is_item_sync_supported(
            registry,
            {
                "source": "tushare",
                "item_key": "rt_daily",
                "permission_points": 0,
                "requires_permission": "1",
                "api_name": "rt_daily",
            },
            source_configs={"tushare": {"config_json": {"granted_api_names": ["rt_daily"]}}},
        ) is True

    def test_tushare_granted_api_names_matches_item_key_aliases(self):
        from app.datasync.capabilities import is_item_sync_supported

        registry = _registry_for(("tushare", "stock_daily"))

        assert is_item_sync_supported(
            registry,
            {
                "source": "tushare",
                "item_key": "stock_daily",
                "permission_points": 0,
                "requires_permission": "1",
                "api_name": "daily",
            },
            source_configs={
                "tushare": {
                    "config_json": {"token_points": 120, "granted_api_names": ["stock_daily"]}
                }
            },
        ) is True

    def test_explicit_grant_does_not_bypass_numeric_thresholds(self):
        from app.datasync.capabilities import is_item_sync_supported

        registry = _registry_for(("tushare", "bak_daily"))

        assert is_item_sync_supported(
            registry,
            {
                "source": "tushare",
                "item_key": "bak_daily",
                "permission_points": 5000,
                "requires_permission": "0",
                "api_name": "bak_daily",
            },
            source_configs={
                "tushare": {
                    "config_json": {"token_points": 120, "granted_api_names": ["bak_daily"]}
                }
            },
        ) is False

    def test_non_tushare_interface_only_requires_registry_support(self):
        from app.datasync.capabilities import is_item_sync_supported

        registry = _registry_for(("akshare", "fund_etf_daily"))

        with patch.dict("os.environ", {}, clear=True):
            assert is_item_sync_supported(
                registry,
                {"source": "akshare", "item_key": "fund_etf_daily"},
                source_configs={},
            ) is True

    def test_runtime_unsupported_interface_is_not_supported(self):
        from app.datasync.capabilities import is_item_sync_supported

        registry = MagicMock()
        iface = MagicMock()
        iface.supports_scheduled_sync.return_value = False
        registry.get_interface.return_value = iface

        assert is_item_sync_supported(
            registry,
            {"source": "tushare", "item_key": "fina_indicator", "api_name": "fina_indicator"},
            source_configs={"tushare": {"config_json": {"token_points": 2000}}},
        ) is False

    def test_runtime_unsupported_interface_still_reports_capability_support(self):
        from app.datasync.capabilities import get_item_support_state

        registry = MagicMock()
        iface = MagicMock()
        iface.supports_scheduled_sync.return_value = False
        registry.get_interface.return_value = iface

        state = get_item_support_state(
            registry,
            {
                "source": "tushare",
                "item_key": "fina_indicator",
                "api_name": "fina_indicator",
                "permission_points": 2000,
                "requires_permission": "0",
            },
            source_configs={"tushare": {"config_json": {"token_points": 2000}}},
        )

        assert state == {
            "capability_supported": True,
            "auto_sync_supported": False,
            "sync_supported": False,
        }

    def test_pledge_detail_uses_custom_sync_support(self):
        from app.datasync.capabilities import get_item_support_state
        from app.datasync.sources.tushare.interfaces import TusharePledgeDetailInterface

        registry = MagicMock()
        registry.get_interface.return_value = TusharePledgeDetailInterface()

        state = get_item_support_state(
            registry,
            {
                "source": "tushare",
                "item_key": "pledge_detail",
                "api_name": "pledge_detail",
                "permission_points": 0,
                "requires_permission": "0",
            },
            source_configs={"tushare": {"config_json": {"token_points": 2000}}},
        )

        assert state == {
            "capability_supported": True,
            "auto_sync_supported": True,
            "sync_supported": True,
        }

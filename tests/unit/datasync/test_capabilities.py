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
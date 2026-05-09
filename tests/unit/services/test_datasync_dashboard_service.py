from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


def _engine_with_rows(*, item_rows, status_rows, init_rows):
    conn = MagicMock()

    def _result(rows):
        return MagicMock(fetchall=MagicMock(return_value=rows))

    def _dispatch(statement, *_args, **_kwargs):
        sql = getattr(statement, "text", str(statement))
        if "FROM data_source_items" in sql:
            return _result(item_rows)
        if "FROM data_sync_status" in sql:
            return _result(status_rows)
        if "FROM sync_status_init" in sql:
            return _result(init_rows)
        return _result([])

    conn.execute.side_effect = _dispatch
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = ctx
    return engine


def test_get_interface_coverage_deduplicates_duplicate_item_rows():
    from app.domains.extdata.service import DataSyncDashboardService

    item_rows = [
        ("tushare", "Tushare Pro", "stock_daily", "Stock Daily", 1, "daily", 120, "0", "backfill", "date"),
        ("tushare", "Tushare Pro", "stock_daily", "Stock Daily", 2, "daily", 120, "0", "backfill", "date"),
    ]
    status_rows = [
        ("tushare", "stock_daily", 70, 5, 5, 0, 0, 2, 80, date(2026, 4, 22)),
    ]
    init_rows = [
        ("tushare", "stock_daily", date(2015, 1, 1), date(2026, 4, 22)),
    ]
    engine = _engine_with_rows(item_rows=item_rows, status_rows=status_rows, init_rows=init_rows)

    registry = MagicMock()
    iface = MagicMock()
    iface.supports_backfill.return_value = True
    registry.get_interface.return_value = iface

    with patch("app.infrastructure.db.connections.get_quantmate_engine", return_value=engine), \
         patch("app.datasync.registry.build_default_registry", return_value=registry), \
         patch("app.datasync.capabilities.load_source_config_map", return_value={}), \
         patch("app.datasync.capabilities.is_item_sync_supported", return_value=True), \
         patch(
             "app.datasync.service.init_service.get_coverage_window",
             return_value={"start_date": date(2015, 1, 1), "end_date": date(2026, 4, 22)},
         ), \
         patch(
             "app.datasync.service.sync_engine.get_trade_calendar",
             return_value=[date(2026, 4, 21), date(2026, 4, 22)],
         ):
        result = DataSyncDashboardService().get_interface_coverage(source="tushare")

    assert len(result["items"]) == 1
    assert result["summary"]["items"] == 1
    assert result["items"][0]["item_key"] == "stock_daily"
    assert result["items"][0]["backfill_mode"] == "date"
    assert result["items"][0]["counts"]["rate_limited"] == 2

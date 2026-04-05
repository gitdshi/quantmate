from datetime import date

from app.domains.extdata.dao import data_sync_status_dao as dao


def test_step_to_source_interface_mapping_handles_known_legacy_steps():
    assert dao._step_to_source_interface("akshare_index") == ("akshare", "index_daily")
    assert dao._step_to_source_interface("tushare_stock_daily") == ("tushare", "stock_daily")
    assert dao._step_to_source_interface("vnpy_sync") == ("vnpy", "vnpy_sync")


def test_source_interface_to_step_mapping_handles_refactored_rows():
    assert dao._source_interface_to_step("akshare", "index_daily") == "akshare_index"
    assert dao._source_interface_to_step("tushare", "index_weekly") == "tushare_index_weekly"
    assert dao._source_interface_to_step("vnpy", "vnpy_sync") == "vnpy_sync"


def test_unknown_mapping_falls_back_to_composite_key():
    assert dao._source_interface_to_step("custom", "foo") == "custom:foo"
    assert dao._step_to_source_interface("custom_step") == ("legacy", "custom_step")

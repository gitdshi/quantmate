"""Coverage batch 5 — comprehensive backend gap coverage.

Targets: qlib_tasks, qlib_model_service, backtest_service helpers, sync_engine,
data_converter, init_service, scheduler, realtime_quote_service, akshare_ingest,
tushare/akshare interfaces, vnpy_trading_service, paper_strategy_executor.
"""

from __future__ import annotations

import json
import sys
import time
import os
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call, ANY

import numpy as np
import pandas as pd
import pytest


# ═══════════════════════════════════════════════════════════════
#  qlib_tasks
# ═══════════════════════════════════════════════════════════════


class TestQlibTasks:
    MOD = "app.worker.service.qlib_tasks"

    def _m(self):
        import app.worker.service.qlib_tasks as m
        return m

    @patch("app.worker.service.qlib_tasks._get_qlib_model_service")
    def test_training_success(self, mock_get):
        m = self._m()
        mock_get.return_value.return_value.train_model.return_value = {
            "training_run_id": 42, "status": "completed", "metrics": {"ic": 0.05}
        }
        r = m.run_qlib_training_task(user_id=1, model_type="LightGBM")
        assert r["training_run_id"] == 42

    @patch("app.worker.service.qlib_tasks._get_qlib_model_service")
    def test_training_error(self, mock_get):
        m = self._m()
        mock_get.return_value.return_value.train_model.side_effect = RuntimeError("boom")
        r = m.run_qlib_training_task(user_id=1)
        assert r["status"] == "failed"
        assert "boom" in r["error"]

    @patch("app.worker.service.qlib_tasks._get_data_converter")
    def test_conversion_success_no_dates(self, mock_get):
        m = self._m()
        mock_get.return_value.return_value = {"status": "completed", "instrument_count": 50}
        r = m.run_data_conversion_task()
        assert r["status"] == "completed"

    @patch("app.worker.service.qlib_tasks._get_data_converter")
    def test_conversion_with_dates(self, mock_get):
        m = self._m()
        mock_get.return_value.return_value = {"status": "completed"}
        r = m.run_data_conversion_task(start_date="2024-01-01", end_date="2024-06-30")
        assert r["status"] == "completed"

    @patch("app.worker.service.qlib_tasks._get_data_converter")
    def test_conversion_with_akshare(self, mock_get):
        m = self._m()
        mock_get.return_value.return_value = {"status": "completed"}
        r = m.run_data_conversion_task(use_akshare_supplement=True)
        assert r["status"] == "completed"

    @patch("app.worker.service.qlib_tasks._get_data_converter")
    def test_conversion_error(self, mock_get):
        m = self._m()
        mock_get.return_value.side_effect = Exception("fail")
        r = m.run_data_conversion_task()
        assert r["status"] == "failed"

    @patch("app.domains.factors.service.FactorService")
    def test_factor_eval_success(self, mock_cls):
        m = self._m()
        mock_cls.return_value.run_evaluation.return_value = {"id": 99, "ic": 0.05}
        r = m.run_factor_evaluation_task(1, 10, "2024-01-01", "2024-12-31")
        assert r["status"] == "completed"
        assert r["evaluation"]["id"] == 99

    @patch("app.domains.factors.service.FactorService")
    def test_factor_eval_error(self, mock_cls):
        m = self._m()
        mock_cls.return_value.run_evaluation.side_effect = RuntimeError("nope")
        r = m.run_factor_evaluation_task(1, 10, "2024-01-01", "2024-12-31")
        assert r["status"] == "failed"
        assert r["factor_id"] == 10

    @patch("app.infrastructure.db.connections.connection")
    def test_create_backtest_record(self, mock_cf):
        m = self._m()
        ctx = mock_cf.return_value.__enter__.return_value
        m._create_qlib_backtest_record(
            user_id=1, job_id="j1", training_run_id=None,
            strategy_type="TopkDropout", topk=50, n_drop=5,
            universe="csi300", start_date="2024-01-01", end_date="2024-12-31",
            benchmark="SH000300",
        )
        ctx.execute.assert_called_once()
        ctx.commit.assert_called_once()

    @patch("app.infrastructure.db.connections.connection")
    def test_create_backtest_record_exception(self, mock_cf):
        m = self._m()
        mock_cf.return_value.__enter__.return_value.execute.side_effect = RuntimeError("db")
        m._create_qlib_backtest_record(
            user_id=1, job_id="j1", start_date="2024-01-01", end_date="2024-12-31",
        )  # Should not raise — swallowed

    @patch("app.infrastructure.db.connections.connection")
    def test_update_backtest_status_no_error(self, mock_cf):
        m = self._m()
        ctx = mock_cf.return_value.__enter__.return_value
        m._update_qlib_backtest_status("j1", "running")
        ctx.execute.assert_called_once()

    @patch("app.infrastructure.db.connections.connection")
    def test_update_backtest_status_with_error(self, mock_cf):
        m = self._m()
        ctx = mock_cf.return_value.__enter__.return_value
        m._update_qlib_backtest_status("j1", "failed", error="oops")
        ctx.execute.assert_called_once()

    @patch("app.infrastructure.db.connections.connection")
    def test_update_backtest_status_exception(self, mock_cf):
        m = self._m()
        mock_cf.return_value.__enter__.return_value.execute.side_effect = Exception("x")
        m._update_qlib_backtest_status("j1", "running")  # Should not raise

    @patch("app.infrastructure.db.connections.connection")
    def test_complete_qlib_backtest(self, mock_cf):
        m = self._m()
        ctx = mock_cf.return_value.__enter__.return_value
        m._complete_qlib_backtest("j1", {"sharpe": 1.5}, None)
        ctx.execute.assert_called_once()

    @patch("app.infrastructure.db.connections.connection")
    def test_complete_qlib_backtest_exception(self, mock_cf):
        m = self._m()
        mock_cf.return_value.__enter__.return_value.execute.side_effect = Exception("x")
        m._complete_qlib_backtest("j1", {}, None)  # Should not raise


# ═══════════════════════════════════════════════════════════════
#  qlib_model_service
# ═══════════════════════════════════════════════════════════════


class TestQlibModelService:
    def _cls(self):
        from app.domains.ai.qlib_model_service import QlibModelService
        return QlibModelService

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_predictions_no_date(self, mock_cf):
        svc = self._cls()()
        row = MagicMock()
        row._mapping = {"instrument": "SZ000001", "trade_date": "2024-01-15", "score": 0.8, "rank_pct": 0.95}
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [row]
        result = svc.get_predictions(training_run_id=1)
        assert len(result) == 1
        assert result[0]["instrument"] == "SZ000001"

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_predictions_with_date(self, mock_cf):
        svc = self._cls()()
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
        result = svc.get_predictions(training_run_id=1, trade_date="2024-01-15", top_n=10)
        assert result == []

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_list_training_runs_no_status(self, mock_cf):
        svc = self._cls()()
        row = MagicMock()
        row._mapping = {"id": 1, "model_type": "LightGBM", "status": "completed"}
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [row]
        result = svc.list_training_runs(user_id=1)
        assert len(result) == 1

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_list_training_runs_with_status(self, mock_cf):
        svc = self._cls()()
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
        result = svc.list_training_runs(user_id=1, status="completed")
        assert result == []

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_training_run_found(self, mock_cf):
        svc = self._cls()()
        row = MagicMock()
        row._mapping = {"id": 5, "model_type": "LSTM"}
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = row
        result = svc.get_training_run(5)
        assert result["id"] == 5

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_training_run_not_found(self, mock_cf):
        svc = self._cls()()
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
        result = svc.get_training_run(999)
        assert result is None

    def test_list_supported_models(self):
        svc = self._cls()()
        models = svc.list_supported_models()
        assert any(m["name"] == "LightGBM" for m in models)
        assert len(models) >= 4

    def test_list_supported_datasets(self):
        svc = self._cls()()
        datasets = svc.list_supported_datasets()
        assert any(d["name"] == "Alpha158" for d in datasets)

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_create_training_run(self, mock_cf):
        svc = self._cls()()
        ctx = mock_cf.return_value.__enter__.return_value
        ctx.execute.return_value.lastrowid = 42
        run_id = svc._create_training_run(
            user_id=1, model_type="LightGBM", factor_set="Alpha158",
            universe="csi300", train_start="2020-01-01", train_end="2022-12-31",
        )
        assert run_id == 42
        ctx.commit.assert_called_once()

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_create_training_run_with_hyperparams(self, mock_cf):
        svc = self._cls()()
        ctx = mock_cf.return_value.__enter__.return_value
        ctx.execute.return_value.lastrowid = 50
        run_id = svc._create_training_run(
            user_id=1, model_type="LightGBM", factor_set="Alpha158",
            universe="csi300", train_start="2020-01-01", train_end="2022-12-31",
            hyperparams={"lr": 0.01},
        )
        assert run_id == 50

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_update_training_status(self, mock_cf):
        svc = self._cls()()
        ctx = mock_cf.return_value.__enter__.return_value
        svc._update_training_status(42, "running")
        ctx.execute.assert_called_once()
        ctx.commit.assert_called_once()

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_complete_training_run(self, mock_cf):
        svc = self._cls()()
        ctx = mock_cf.return_value.__enter__.return_value
        svc._complete_training_run(42, {"ic": 0.05, "prediction_count": 1000})
        ctx.execute.assert_called_once()

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_fail_training_run(self, mock_cf):
        svc = self._cls()()
        ctx = mock_cf.return_value.__enter__.return_value
        svc._fail_training_run(42, "Some error message")
        ctx.execute.assert_called_once()

    def test_save_predictions_none(self):
        svc = self._cls()()
        svc._save_predictions(1, None)  # Should not raise

    def test_save_predictions_empty_df(self):
        svc = self._cls()()
        svc._save_predictions(1, pd.DataFrame())  # empty — should not raise

    def test_calculate_metrics_none_test_data(self):
        svc = self._cls()()
        dataset = MagicMock()
        dataset.prepare.return_value = None
        result = svc._calculate_metrics(pd.Series([1, 2, 3]), dataset)
        assert result == {}

    def test_calculate_metrics_no_common_idx(self):
        svc = self._cls()()
        pred = pd.Series([1, 2, 3], index=pd.MultiIndex.from_tuples([("A", 1), ("A", 2), ("A", 3)]))
        label = pd.Series([4, 5], index=pd.MultiIndex.from_tuples([("B", 10), ("B", 11)]))
        dataset = MagicMock()
        dataset.prepare.return_value = pd.DataFrame(label)
        result = svc._calculate_metrics(pred, dataset)
        assert result == {}

    def test_train_model_unsupported_model(self):
        svc = self._cls()()
        with pytest.raises(ValueError, match="Unsupported model type"):
            svc.train_model(user_id=1, model_type="UnsupportedModel")

    def test_train_model_unsupported_factor_set(self):
        svc = self._cls()()
        with pytest.raises(ValueError, match="Unsupported factor set"):
            svc.train_model(user_id=1, model_type="LightGBM", factor_set="BadFactorSet")


# ═══════════════════════════════════════════════════════════════
#  data_converter
# ═══════════════════════════════════════════════════════════════


class TestDataConverter:
    def _m(self):
        from app.infrastructure.qlib import data_converter as m
        return m

    def test_ts_code_to_qlib_sz(self):
        m = self._m()
        assert m._ts_code_to_qlib_instrument("000001.SZ") == "SZ000001"

    def test_ts_code_to_qlib_sh(self):
        m = self._m()
        assert m._ts_code_to_qlib_instrument("600000.SH") == "SH600000"

    def test_ts_code_to_qlib_bj(self):
        m = self._m()
        assert m._ts_code_to_qlib_instrument("430047.BJ") == "BJ430047"

    def test_ts_code_to_qlib_no_dot(self):
        m = self._m()
        assert m._ts_code_to_qlib_instrument("000001") == "000001"

    def test_normalize_empty(self):
        m = self._m()
        df = pd.DataFrame()
        assert m._normalize_daily_dataframe(df).empty

    def test_normalize_with_ts_code(self):
        m = self._m()
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240115"],
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
            "vol": [1000], "amount": [100000], "adj_factor": [1.05],
        })
        result = m._normalize_daily_dataframe(df)
        assert "instrument" in result.columns
        assert result.iloc[0]["instrument"] == "SZ000001"

    def test_normalize_with_symbol_col(self):
        m = self._m()
        df = pd.DataFrame({
            "symbol": ["SZ000001"],
            "trade_date": ["20240115"],
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
            "volume": [1000],
        })
        result = m._normalize_daily_dataframe(df)
        assert "instrument" in result.columns
        assert "factor" in result.columns  # Should get default 1.0

    def test_normalize_missing_instrument_col(self):
        m = self._m()
        df = pd.DataFrame({
            "trade_date": ["20240115"],
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        })
        with pytest.raises(KeyError):
            m._normalize_daily_dataframe(df)

    @patch("app.infrastructure.qlib.data_converter.connection")
    def test_fetch_tushare_daily_no_dates(self, mock_cf):
        m = self._m()
        mock_cf.return_value.__enter__.return_value = MagicMock()
        with patch("pandas.read_sql", return_value=pd.DataFrame()) as mock_sql:
            result = m.fetch_tushare_daily()
            assert result.empty

    @patch("app.infrastructure.qlib.data_converter.connection")
    def test_fetch_tushare_daily_with_dates(self, mock_cf):
        m = self._m()
        mock_cf.return_value.__enter__.return_value = MagicMock()
        with patch("pandas.read_sql", return_value=pd.DataFrame()) as mock_sql:
            result = m.fetch_tushare_daily(start_date=date(2024, 1, 1), end_date=date(2024, 6, 30))
            assert result.empty

    @patch("app.infrastructure.qlib.data_converter.connection")
    def test_fetch_akshare_daily(self, mock_cf):
        m = self._m()
        mock_cf.return_value.__enter__.return_value = MagicMock()
        with patch("pandas.read_sql", return_value=pd.DataFrame()) as mock_sql:
            result = m.fetch_akshare_daily()
            assert result.empty

    @patch("app.infrastructure.qlib.data_converter.get_qlib_engine")
    def test_log_conversion(self, mock_engine):
        m = self._m()
        ctx = mock_engine.return_value.connect.return_value.__enter__.return_value
        m._log_conversion("tushare", "stock_daily", 100, date(2024, 1, 1), date(2024, 6, 30))
        ctx.execute.assert_called_once()

    @patch("app.infrastructure.qlib.data_converter.get_qlib_engine")
    def test_log_conversion_exception(self, mock_engine):
        m = self._m()
        mock_engine.return_value.connect.return_value.__enter__.return_value.execute.side_effect = Exception("x")
        m._log_conversion("tushare", "stock_daily", 100, date(2024, 1, 1), date(2024, 6, 30))

    @patch("app.infrastructure.qlib.data_converter._log_conversion")
    @patch("app.infrastructure.qlib.data_converter.fetch_tushare_daily")
    def test_convert_to_qlib_format_empty(self, mock_fetch, mock_log):
        m = self._m()
        mock_fetch.return_value = pd.DataFrame()
        result = m.convert_to_qlib_format()
        assert result["status"] == "empty"

    @patch("app.infrastructure.qlib.data_converter._log_conversion")
    @patch("app.infrastructure.qlib.data_converter.fetch_tushare_daily")
    def test_convert_to_qlib_format_with_data(self, mock_fetch, mock_log, tmp_path):
        m = self._m()
        df = pd.DataFrame({
            "instrument": ["SZ000001", "SZ000001"],
            "date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
            "open": [10.0, 10.5], "high": [11.0, 11.5], "low": [9.5, 10.0],
            "close": [10.5, 11.0], "volume": [1000, 1200],
            "amount": [50000, 60000], "factor": [1.0, 1.0],
        })
        mock_fetch.return_value = df
        result = m.convert_to_qlib_format(data_dir=str(tmp_path))
        assert result["status"] == "completed"
        assert result["instrument_count"] == 1
        assert (tmp_path / "calendars" / "day.txt").exists()
        assert (tmp_path / "instruments" / "all.txt").exists()

    @patch("app.infrastructure.qlib.data_converter._log_conversion")
    @patch("app.infrastructure.qlib.data_converter.fetch_akshare_daily")
    @patch("app.infrastructure.qlib.data_converter.fetch_tushare_daily")
    def test_convert_with_akshare_supplement(self, mock_ts, mock_ak, mock_log, tmp_path):
        m = self._m()
        df = pd.DataFrame({
            "instrument": ["SZ000001"],
            "date": pd.to_datetime(["2024-01-15"]),
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
            "volume": [1000], "amount": [50000], "factor": [1.0],
        })
        ak_df = pd.DataFrame({
            "instrument": ["SH600000"],
            "date": pd.to_datetime(["2024-01-15"]),
            "open": [20.0], "high": [21.0], "low": [19.5], "close": [20.5],
            "volume": [2000], "amount": [100000], "factor": [1.0],
        })
        mock_ts.return_value = df
        mock_ak.return_value = ak_df
        result = m.convert_to_qlib_format(data_dir=str(tmp_path), use_akshare_supplement=True)
        assert result["instrument_count"] == 2


# ═══════════════════════════════════════════════════════════════
#  sync_engine
# ═══════════════════════════════════════════════════════════════


class TestSyncEngine:
    def _m(self):
        from app.datasync.service import sync_engine as m
        return m

    @patch("app.datasync.service.sync_engine.get_quantmate_engine")
    def test_write_status(self, mock_engine):
        m = self._m()
        ctx = mock_engine.return_value.begin.return_value.__enter__.return_value
        m._write_status(date(2024, 1, 15), "tushare", "stock_daily", "success", 1000)
        ctx.execute.assert_called_once()

    @patch("app.datasync.service.sync_engine.get_quantmate_engine")
    def test_get_status_found(self, mock_engine):
        m = self._m()
        row = MagicMock()
        row.__getitem__ = lambda s, idx: {0: "success", 1: 0}[idx]
        mock_engine.return_value.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = row
        s = m._get_status(date(2024, 1, 15), "tushare", "stock_daily")
        assert s == "success"

    @patch("app.datasync.service.sync_engine.get_quantmate_engine")
    def test_get_status_not_found(self, mock_engine):
        m = self._m()
        mock_engine.return_value.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
        s = m._get_status(date(2024, 1, 15), "tushare", "stock_daily")
        assert s is None

    @patch("app.datasync.service.sync_engine.get_quantmate_engine")
    def test_get_failed_records(self, mock_engine):
        m = self._m()
        rows = [(date(2024, 1, 10), "tushare", "stock_daily", 1)]
        mock_engine.return_value.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = rows
        result = m._get_failed_records(30)
        assert len(result) == 1
        assert result[0][1] == "tushare"

    @patch("app.datasync.service.sync_engine.get_quantmate_engine")
    def test_get_enabled_items(self, mock_engine):
        m = self._m()
        rows = [("tushare", "stock_daily", "tushare", "stock_daily", True, 20)]
        mock_engine.return_value.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = rows
        result = m._get_enabled_items()
        assert len(result) == 1
        assert result[0]["source"] == "tushare"

    @patch("app.datasync.service.sync_engine._write_status")
    @patch("app.datasync.service.sync_engine._get_status_snapshot")
    @patch("app.datasync.service.sync_engine._get_enabled_items")
    def test_daily_sync_skip_already_synced(self, mock_items, mock_snapshot, mock_write):
        m = self._m()
        mock_items.return_value = [{
            "source": "tushare", "item_key": "stock_daily", "target_database": "tushare",
            "target_table": "stock_daily", "table_created": True, "sync_priority": 20,
        }]
        mock_snapshot.return_value = ("success", 1)
        registry = MagicMock()
        results = m.daily_sync(registry, target_date=date(2024, 1, 15))
        assert results["tushare/stock_daily"]["skipped"] is True

    @patch("app.datasync.service.sync_engine._write_status")
    @patch("app.datasync.service.sync_engine._get_status_snapshot")
    @patch("app.datasync.service.sync_engine._get_enabled_items")
    def test_daily_sync_no_interface(self, mock_items, mock_snapshot, mock_write):
        m = self._m()
        mock_items.return_value = [{
            "source": "tushare", "item_key": "unknown", "target_database": "tushare",
            "target_table": "unknown", "table_created": True, "sync_priority": 99,
        }]
        mock_snapshot.return_value = (None, 0)
        registry = MagicMock()
        registry.get_interface.return_value = None
        results = m.daily_sync(registry, target_date=date(2024, 1, 15))
        assert results["tushare/unknown"]["status"] == "skipped"

    @patch("app.datasync.service.sync_engine._write_status")
    @patch("app.datasync.service.sync_engine._get_status_snapshot")
    @patch("app.datasync.service.sync_engine._get_enabled_items")
    @patch("app.datasync.service.sync_engine.ensure_table")
    def test_daily_sync_run_interface(self, mock_ensure, mock_items, mock_snapshot, mock_write):
        from app.datasync.base import SyncResult, SyncStatus
        m = self._m()
        mock_items.return_value = [{
            "source": "tushare", "item_key": "stock_daily", "target_database": "tushare",
            "target_table": "stock_daily", "table_created": True, "sync_priority": 20,
        }]
        mock_snapshot.return_value = (None, 0)
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(SyncStatus.SUCCESS, 500)
        registry = MagicMock()
        registry.get_interface.return_value = iface
        results = m.daily_sync(registry, target_date=date(2024, 1, 15))
        assert results["tushare/stock_daily"]["status"] == "success"
        assert results["tushare/stock_daily"]["rows"] == 500

    @patch("app.datasync.service.sync_engine._write_status")
    @patch("app.datasync.service.sync_engine._get_status_snapshot")
    @patch("app.datasync.service.sync_engine._get_enabled_items")
    @patch("app.datasync.service.sync_engine.ensure_table")
    def test_daily_sync_table_creation_failure(self, mock_ensure, mock_items, mock_snapshot, mock_write):
        m = self._m()
        mock_items.return_value = [{
            "source": "tushare", "item_key": "stock_daily", "target_database": "tushare",
            "target_table": "stock_daily", "table_created": False, "sync_priority": 20,
        }]
        mock_snapshot.return_value = (None, 0)
        iface = MagicMock()
        registry = MagicMock()
        registry.get_interface.return_value = iface
        mock_ensure.side_effect = RuntimeError("DDL fail")
        results = m.daily_sync(registry, target_date=date(2024, 1, 15))
        assert results["tushare/stock_daily"]["status"] == "error"

    @patch("app.datasync.service.sync_engine._write_status")
    @patch("app.datasync.service.sync_engine._get_status_snapshot")
    @patch("app.datasync.service.sync_engine._get_enabled_items")
    def test_daily_sync_exception_in_sync(self, mock_items, mock_snapshot, mock_write):
        m = self._m()
        mock_items.return_value = [{
            "source": "tushare", "item_key": "stock_daily", "target_database": "tushare",
            "target_table": "stock_daily", "table_created": True, "sync_priority": 20,
        }]
        mock_snapshot.return_value = (None, 0)
        iface = MagicMock()
        iface.sync_date.side_effect = RuntimeError("sync fail")
        registry = MagicMock()
        registry.get_interface.return_value = iface
        results = m.daily_sync(registry, target_date=date(2024, 1, 15))
        assert results["tushare/stock_daily"]["status"] == "error"

    @patch("app.datasync.service.sync_engine._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")})
    @patch("app.datasync.service.sync_engine._write_status")
    @patch("app.datasync.service.sync_engine._get_failed_records")
    @patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False)
    @patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock")
    @patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock")
    def test_backfill_retry_max_retries(self, mock_rel, mock_acq, mock_locked, mock_failed, mock_write, mock_enabled):
        m = self._m()
        mock_failed.return_value = [(date(2024, 1, 10), "tushare", "stock_daily", 5)]  # 5 >= MAX_RETRIES(3)
        registry = MagicMock()
        results = m.backfill_retry(registry, lookback_days=30)
        assert results == {}  # All skipped due to max retries

    @patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=True)
    def test_backfill_retry_locked(self, mock_locked):
        m = self._m()
        registry = MagicMock()
        results = m.backfill_retry(registry)
        assert results == {}

    @patch("app.datasync.service.sync_engine._get_enabled_backfill_keys", return_value={("tushare", "stock_daily")})
    @patch("app.datasync.service.sync_engine._write_status")
    @patch("app.datasync.service.sync_engine._get_failed_records")
    @patch("app.domains.extdata.dao.data_sync_status_dao.is_backfill_locked", return_value=False)
    @patch("app.domains.extdata.dao.data_sync_status_dao.acquire_backfill_lock")
    @patch("app.domains.extdata.dao.data_sync_status_dao.release_backfill_lock")
    def test_backfill_retry_success(self, mock_rel, mock_acq, mock_locked, mock_failed, mock_write, mock_enabled):
        from app.datasync.base import SyncResult, SyncStatus
        m = self._m()
        mock_failed.return_value = [(date(2024, 1, 10), "tushare", "stock_daily", 0)]
        iface = MagicMock()
        iface.sync_date.return_value = SyncResult(SyncStatus.SUCCESS, 100)
        registry = MagicMock()
        registry.get_interface.return_value = iface
        results = m.backfill_retry(registry, lookback_days=30)
        assert len(results) == 1


# ═══════════════════════════════════════════════════════════════
#  init_service
# ═══════════════════════════════════════════════════════════════


class TestInitService:
    def _m(self):
        from app.datasync.service import init_service as m
        return m

    def test_get_env_default(self):
        m = self._m()
        with patch.dict(os.environ, {}, clear=True):
            env = m._get_env()
            assert env == "dev"

    def test_get_env_staging(self):
        m = self._m()
        with patch.dict(os.environ, {"APP_ENV": "staging"}):
            env = m._get_env()
            assert env == "staging"

    def test_lookback_days_dev(self):
        m = self._m()
        with patch.object(m, "_get_env", return_value="dev"):
            assert m._get_env_window_years() == 1

    def test_lookback_days_prod(self):
        m = self._m()
        with patch.object(m, "_get_env", return_value="production"):
            assert m._get_env_window_years() == 20

    @patch("app.datasync.service.init_service._reconcile_pending_records", return_value={"pending_records": 100, "items_reconciled": 5, "skipped_unsupported": []})
    @patch("app.datasync.service.init_service._ensure_tables", return_value=3)
    @patch("app.datasync.service.init_service._seed_items")
    @patch("app.datasync.service.init_service._seed_configs")
    @patch("app.datasync.service.init_service.ensure_sync_status_init_table")
    @patch("app.datasync.service.init_service.ensure_backfill_lock_table")
    @patch("app.datasync.service.init_service.ensure_tables")
    @patch("app.datasync.service.init_service.get_quantmate_engine")
    def test_initialize(self, mock_engine, mock_et0, mock_ebl, mock_essi, mock_sc, mock_si, mock_et, mock_gp):
        m = self._m()
        registry = MagicMock()
        result = m.initialize(registry)
        assert result["tables_created"] == 3
        assert result["pending_records"] == 100

    @patch("app.datasync.service.init_service.get_quantmate_engine")
    def test_seed_configs(self, mock_engine):
        m = self._m()
        ctx = mock_engine.return_value.begin.return_value.__enter__.return_value
        source = MagicMock(source_key="tushare", display_name="Tushare", requires_token=True)
        registry = MagicMock()
        registry.all_sources.return_value = [source]
        m._seed_configs(mock_engine.return_value, registry)
        ctx.execute.assert_called()

    @patch("app.datasync.service.init_service.get_quantmate_engine")
    def test_seed_items(self, mock_engine):
        m = self._m()
        ctx = mock_engine.return_value.begin.return_value.__enter__.return_value
        iface = MagicMock()
        iface.info = MagicMock(
            source_key="tushare", interface_key="stock_daily", display_name="日线",
            enabled_by_default=True, description="", requires_permission="",
            target_database="tushare", target_table="stock_daily", sync_priority=20,
        )
        registry = MagicMock()
        registry.all_interfaces.return_value = [iface]
        m._seed_items(mock_engine.return_value, registry)
        ctx.execute.assert_called()

    @patch("app.datasync.service.init_service.ensure_table", return_value=True)
    @patch("app.datasync.service.init_service.get_quantmate_engine")
    def test_ensure_tables(self, mock_engine, mock_et):
        m = self._m()
        registry = MagicMock()
        iface = MagicMock()
        iface.info.source_key = "tushare"
        iface.info.target_database = "tushare"
        iface.info.target_table = "stock_daily"
        iface.get_ddl.return_value = "CREATE TABLE ..."
        registry.all_interfaces.return_value = [iface]
        count = m._ensure_tables(mock_engine.return_value, registry)
        assert count == 1


# ═══════════════════════════════════════════════════════════════
#  scheduler
# ═══════════════════════════════════════════════════════════════


class TestScheduler:
    def _m(self):
        from app.datasync import scheduler as m
        return m

    @patch("app.datasync.scheduler._build_registry")
    @patch("app.datasync.service.sync_engine.daily_sync", return_value={"tushare/stock_daily": {"status": "success"}})
    @patch("app.datasync.service.vnpy_sync.run_vnpy_sync_job")
    def test_run_daily_sync(self, mock_vnpy, mock_sync, mock_reg):
        from app.datasync.base import SyncResult, SyncStatus
        m = self._m()
        mock_vnpy.return_value = SyncResult(SyncStatus.SUCCESS, 50)
        result = m.run_daily_sync(target_date=date(2024, 1, 15))
        assert "vnpy/vnpy_sync" in result

    @patch("app.datasync.scheduler._build_registry")
    @patch("app.datasync.service.sync_engine.backfill_retry", return_value={})
    def test_run_backfill(self, mock_retry, mock_reg):
        m = self._m()
        result = m.run_backfill()
        assert result == {}

    @patch("app.datasync.service.vnpy_sync.run_vnpy_sync_job")
    def test_run_vnpy(self, mock_run):
        from app.datasync.base import SyncResult, SyncStatus
        m = self._m()
        mock_run.return_value = SyncResult(SyncStatus.SUCCESS, 10)
        result = m.run_vnpy()
        assert result.rows_synced == 10

    @patch("app.datasync.scheduler._build_registry")
    @patch("app.datasync.service.init_service.initialize", return_value={"env": "dev"})
    def test_run_init(self, mock_init, mock_reg):
        m = self._m()
        result = m.run_init()
        assert result["env"] == "dev"


# ═══════════════════════════════════════════════════════════════
#  realtime_quote_service
# ═══════════════════════════════════════════════════════════════


class TestRealtimeQuoteHelpers:
    def _m(self):
        from app.domains.market import realtime_quote_service as m
        return m

    def test_get_cached_df_miss(self):
        m = self._m()
        assert m._get_cached_df("nonexistent_key_xyz") is None

    def test_set_and_get_cached_df(self):
        m = self._m()
        df = pd.DataFrame({"a": [1, 2]})
        m._set_cached_df("__test_cache_key__", df)
        result = m._get_cached_df("__test_cache_key__")
        assert result is not None
        assert len(result) == 2
        # Clean up
        with m._BULK_CACHE_LOCK:
            m._BULK_CACHE.pop("__test_cache_key__", None)

    def test_get_cached_df_expired(self):
        m = self._m()
        df = pd.DataFrame({"a": [1]})
        with m._BULK_CACHE_LOCK:
            m._BULK_CACHE["__test_expired__"] = (time.monotonic() - 200, df)
        assert m._get_cached_df("__test_expired__") is None
        with m._BULK_CACHE_LOCK:
            m._BULK_CACHE.pop("__test_expired__", None)

    def test_fetch_akshare_with_timeout_cache_hit(self):
        m = self._m()
        df = pd.DataFrame({"x": [10]})
        m._set_cached_df("__fetch_test__", df)
        result = m._fetch_akshare_with_timeout(lambda: None, "__fetch_test__")
        assert len(result) == 1
        with m._BULK_CACHE_LOCK:
            m._BULK_CACHE.pop("__fetch_test__", None)


class TestRealtimeQuoteService:
    def _svc(self):
        from app.domains.market.realtime_quote_service import RealtimeQuoteService
        return RealtimeQuoteService()

    def test_to_float_none(self):
        svc = self._svc()
        assert svc._to_float(None) is None

    def test_to_float_string(self):
        svc = self._svc()
        assert svc._to_float("12.5%") == 12.5

    def test_to_float_empty(self):
        svc = self._svc()
        assert svc._to_float("") is None

    def test_to_float_comma(self):
        svc = self._svc()
        assert svc._to_float("1,234.5") == 1234.5

    def test_to_int_none(self):
        svc = self._svc()
        assert svc._to_int(None) is None

    def test_to_int_string(self):
        svc = self._svc()
        assert svc._to_int("1,000") == 1000

    def test_to_int_empty(self):
        svc = self._svc()
        assert svc._to_int("") is None

    def test_normalize_symbol(self):
        svc = self._svc()
        assert svc._normalize_symbol("usd/cny") == "USDCNY"

    def test_pick_first_match(self):
        svc = self._svc()
        row = pd.Series({"a": None, "b": 42, "c": 99}, dtype=object)
        assert svc._pick(row, ["a", "b", "c"]) == 42

    def test_pick_no_match(self):
        svc = self._svc()
        row = pd.Series({"a": None})
        assert svc._pick(row, ["x", "y"]) is None

    def test_now_iso(self):
        svc = self._svc()
        result = svc._now_iso()
        assert "T" in result

    def test_get_quote_empty_symbol(self):
        svc = self._svc()
        with pytest.raises(ValueError, match="Symbol is required"):
            svc.get_quote("")

    def test_get_quote_unsupported_market(self):
        svc = self._svc()
        with pytest.raises(ValueError, match="Unsupported market"):
            svc.get_quote("000001", market="MOON")

    @patch.object(
        __import__("app.domains.market.realtime_quote_service", fromlist=["RealtimeQuoteService"]).RealtimeQuoteService,
        "_quote_cn",
        return_value={"price": 10.5, "market": "CN"},
    )
    def test_get_quote_cn_routing(self, mock_cn):
        svc = self._svc()
        result = svc.get_quote("000001", market="CN")
        assert result["market"] == "CN"

    def test_build_tencent_quote_basic(self):
        svc = self._svc()
        # Tencent parts: index 0=?, 1=name, 3=price, 4=prev_close, 5=open, 6=volume, 33=high, 34=low
        parts = [""] * 36
        parts[1] = "平安银行"
        parts[3] = "12.50"
        parts[4] = "12.30"
        parts[5] = "12.40"
        parts[6] = "500000"
        parts[33] = "12.80"
        parts[34] = "12.10"
        parts[35] = "2024/01/15/100000000"
        result = svc._build_tencent_quote("000001", parts, "CN")
        assert result["price"] == 12.50
        assert result["name"] == "平安银行"
        assert result["market"] == "CN"

    def test_build_tencent_quote_no_amount(self):
        svc = self._svc()
        parts = [""] * 36
        parts[3] = "10.0"
        parts[4] = "9.5"
        parts[35] = "no_slash_data"
        result = svc._build_tencent_quote("600000", parts, "CN")
        assert result["amount"] is None


# ═══════════════════════════════════════════════════════════════
#  akshare_ingest
# ═══════════════════════════════════════════════════════════════


class TestAkshareIngest:
    def _m(self):
        from app.datasync.service import akshare_ingest as m
        return m

    def test_min_interval_for_default(self):
        m = self._m()
        interval = m._min_interval_for("unknown_api")
        assert interval == 60.0 / m.DEFAULT_CALLS_PER_MIN

    def test_min_interval_for_known(self):
        m = self._m()
        interval = m._min_interval_for("stock_zh_index_daily")
        assert interval >= 0

    def test_call_ak_success(self):
        m = self._m()
        fn = MagicMock(return_value=pd.DataFrame({"x": [1]}))
        result = m.call_ak("test_api", fn)
        assert len(result) == 1

    def test_call_ak_retry_on_rate_limit(self):
        m = self._m()
        fn = MagicMock(side_effect=[Exception("429 rate limit"), pd.DataFrame({"x": [1]})])
        result = m.call_ak("test_api", fn, max_retries=3, backoff_base=0)
        assert len(result) == 1

    def test_call_ak_exhaust_retries(self):
        m = self._m()
        fn = MagicMock(side_effect=RuntimeError("fatal"))
        with pytest.raises(RuntimeError, match="fatal"):
            m.call_ak("test_api", fn, max_retries=2, backoff_base=0)

    def test_call_ak_with_metrics_hook(self):
        m = self._m()
        hook = MagicMock()
        old_hook = getattr(m.call_ak, "_metrics_hook", None)
        m.call_ak._metrics_hook = hook
        try:
            fn = MagicMock(return_value=pd.DataFrame())
            m.call_ak("test_api_hook", fn)
            hook.assert_called_once()
        finally:
            m.call_ak._metrics_hook = old_hook

    def test_set_metrics_hook(self):
        m = self._m()
        hook = lambda x: None
        m.set_metrics_hook(hook)
        assert m.call_ak._metrics_hook is hook
        m.call_ak._metrics_hook = None  # Clean up

    @patch("app.datasync.service.akshare_ingest.upsert_index_daily_rows", return_value=100)
    @patch("app.datasync.service.akshare_ingest.audit_finish")
    @patch("app.datasync.service.akshare_ingest.audit_start", return_value=1)
    @patch("app.datasync.service.akshare_ingest.call_ak")
    def test_ingest_index_daily_success(self, mock_call, mock_start, mock_finish, mock_upsert):
        m = self._m()
        df = pd.DataFrame({
            "date": ["2024-01-15"], "open": [3000.0], "high": [3050.0],
            "low": [2980.0], "close": [3020.0], "volume": [1000000],
        })
        mock_call.return_value = df
        rows = m.ingest_index_daily(symbol="sh000300")
        assert rows == 100

    @patch("app.datasync.service.akshare_ingest.audit_finish")
    @patch("app.datasync.service.akshare_ingest.audit_start", return_value=1)
    @patch("app.datasync.service.akshare_ingest.call_ak")
    def test_ingest_index_daily_empty(self, mock_call, mock_start, mock_finish):
        m = self._m()
        mock_call.return_value = pd.DataFrame()
        rows = m.ingest_index_daily(symbol="sh000300")
        assert rows == 0

    @patch("app.datasync.service.akshare_ingest.ingest_index_daily")
    def test_ingest_all_indexes(self, mock_ingest):
        m = self._m()
        mock_ingest.return_value = 50
        results = m.ingest_all_indexes()
        assert all(r["status"] == "success" for r in results.values())

    @patch("app.datasync.service.akshare_ingest.ingest_index_daily")
    def test_ingest_all_indexes_partial_failure(self, mock_ingest):
        m = self._m()
        mock_ingest.side_effect = [10, Exception("fail"), 20, 30, 40, 50]
        results = m.ingest_all_indexes()
        assert any(r["status"] == "error" for r in results.values())


# ═══════════════════════════════════════════════════════════════
#  tushare interfaces
# ═══════════════════════════════════════════════════════════════


class TestTushareInterfaces:
    def test_stock_basic_info(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        info = TushareStockBasicInterface().info
        assert info.interface_key == "stock_basic"
        assert info.source_key == "tushare"
        assert info.enabled_by_default is True

    def test_stock_basic_ddl(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        ddl = TushareStockBasicInterface().get_ddl()
        assert len(ddl) > 0

    @patch("app.domains.extdata.dao.data_sync_status_dao.get_stock_basic_count", return_value=5000)
    @patch("app.datasync.service.tushare_ingest.ingest_stock_basic")
    def test_stock_basic_sync_date(self, mock_ingest, mock_count):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        from app.datasync.base import SyncStatus
        result = TushareStockBasicInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 5000

    @patch("app.datasync.service.tushare_ingest.ingest_stock_basic", side_effect=RuntimeError("fail"))
    def test_stock_basic_sync_error(self, mock_ingest):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        from app.datasync.base import SyncStatus
        result = TushareStockBasicInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.ERROR

    def test_stock_basic_sync_range(self):
        from app.datasync.sources.tushare.interfaces import TushareStockBasicInterface
        iface = TushareStockBasicInterface()
        with patch.object(iface, "sync_date", return_value=MagicMock()) as mock_sd:
            iface.sync_range(date(2024, 1, 1), date(2024, 1, 31))
            mock_sd.assert_called_once_with(date(2024, 1, 31))

    def test_stock_daily_info(self):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        info = TushareStockDailyInterface().info
        assert info.interface_key == "stock_daily"
        assert info.sync_priority == 20

    @patch("app.datasync.service.tushare_ingest.upsert_daily", return_value=500)
    @patch("app.datasync.service.tushare_ingest.call_pro")
    def test_stock_daily_sync_success(self, mock_call, mock_upsert):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        from app.datasync.base import SyncStatus
        mock_call.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
        result = TushareStockDailyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS

    @patch("app.datasync.service.tushare_ingest.call_pro")
    def test_stock_daily_sync_no_data(self, mock_call):
        from app.datasync.sources.tushare.interfaces import TushareStockDailyInterface
        from app.datasync.base import SyncStatus
        mock_call.return_value = pd.DataFrame()
        result = TushareStockDailyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 0

    def test_adj_factor_info(self):
        from app.datasync.sources.tushare.interfaces import TushareAdjFactorInterface
        info = TushareAdjFactorInterface().info
        assert info.interface_key == "adj_factor"

    @patch("app.domains.extdata.dao.data_sync_status_dao.get_adj_factor_count_for_date", return_value=200)
    @patch("app.datasync.service.tushare_ingest.ingest_adj_factor")
    def test_adj_factor_sync(self, mock_ingest, mock_count):
        from app.datasync.sources.tushare.interfaces import TushareAdjFactorInterface
        from app.datasync.base import SyncStatus
        result = TushareAdjFactorInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 200

    def test_dividend_info(self):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        info = TushareDividendInterface().info
        assert info.enabled_by_default is False
        assert info.requires_permission == "0"

    @patch("app.domains.extdata.dao.tushare_dao.upsert_dividend_df", return_value=10)
    @patch("app.datasync.service.tushare_ingest.call_pro")
    def test_dividend_sync_success(self, mock_call, mock_upsert):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        from app.datasync.base import SyncStatus
        mock_call.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
        result = TushareDividendInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS

    @patch("app.datasync.service.tushare_ingest.call_pro")
    def test_dividend_sync_permission_denied(self, mock_call):
        from app.datasync.sources.tushare.interfaces import TushareDividendInterface
        from app.datasync.base import SyncStatus
        mock_call.side_effect = Exception("没有接口访问权限")
        result = TushareDividendInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.PARTIAL

    def test_top10_holders_info(self):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        info = TushareTop10HoldersInterface().info
        assert info.interface_key == "top10_holders"

    @patch("app.datasync.service.tushare_ingest.get_all_ts_codes", return_value=["000001.SZ", "600000.SH"])
    @patch("app.datasync.service.tushare_ingest.ingest_top10_holders")
    def test_top10_holders_sync(self, mock_ingest, mock_codes):
        from app.datasync.sources.tushare.interfaces import TushareTop10HoldersInterface
        from app.datasync.base import SyncStatus
        result = TushareTop10HoldersInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS

    def test_stock_weekly_info(self):
        from app.datasync.sources.tushare.interfaces import TushareStockWeeklyInterface
        info = TushareStockWeeklyInterface().info
        assert info.interface_key == "stock_weekly"

    @patch("app.datasync.service.tushare_ingest.ingest_weekly", return_value=100)
    def test_stock_weekly_sync(self, mock_ingest):
        from app.datasync.sources.tushare.interfaces import TushareStockWeeklyInterface
        from app.datasync.base import SyncStatus
        result = TushareStockWeeklyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS
        assert result.rows_synced == 100

    def test_stock_monthly_info(self):
        from app.datasync.sources.tushare.interfaces import TushareStockMonthlyInterface
        info = TushareStockMonthlyInterface().info
        assert info.interface_key == "stock_monthly"

    @patch("app.datasync.service.tushare_ingest.ingest_monthly", return_value=50)
    def test_stock_monthly_sync(self, mock_ingest):
        from app.datasync.sources.tushare.interfaces import TushareStockMonthlyInterface
        from app.datasync.base import SyncStatus
        result = TushareStockMonthlyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS

    @patch("app.datasync.service.tushare_ingest.ingest_index_daily", return_value=10)
    def test_index_daily_sync_success(self, mock_ingest):
        from app.datasync.sources.tushare.interfaces import TushareIndexDailyInterface
        from app.datasync.base import SyncStatus
        result = TushareIndexDailyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS

    @patch("app.datasync.service.tushare_ingest.ingest_index_daily")
    def test_index_daily_sync_partial_failure(self, mock_ingest):
        from app.datasync.sources.tushare.interfaces import TushareIndexDailyInterface
        from app.datasync.base import SyncStatus
        mock_ingest.side_effect = [10, RuntimeError("fail"), 10, 10, 10]
        result = TushareIndexDailyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.PARTIAL

    @patch("app.datasync.service.tushare_ingest.ingest_index_weekly", return_value=5)
    def test_index_weekly_sync(self, mock_ingest):
        from app.datasync.sources.tushare.interfaces import TushareIndexWeeklyInterface
        from app.datasync.base import SyncStatus
        result = TushareIndexWeeklyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS


# ═══════════════════════════════════════════════════════════════
#  akshare interfaces
# ═══════════════════════════════════════════════════════════════


class TestAkshareInterfaces:
    def test_index_daily_info(self):
        from app.datasync.sources.akshare.interfaces import AkShareIndexDailyInterface
        info = AkShareIndexDailyInterface().info
        assert info.interface_key == "index_daily"
        assert info.source_key == "akshare"

    @patch("app.datasync.service.akshare_ingest.ingest_index_daily", return_value=100)
    def test_index_daily_sync_success(self, mock_ingest):
        from app.datasync.sources.akshare.interfaces import AkShareIndexDailyInterface
        from app.datasync.base import SyncStatus
        result = AkShareIndexDailyInterface().sync_date(date(2024, 1, 15))
        assert result.status == SyncStatus.SUCCESS

    @patch("app.datasync.service.akshare_ingest.ingest_index_daily")
    def test_index_daily_sync_partial(self, mock_ingest):
        from app.datasync.sources.akshare.interfaces import AkShareIndexDailyInterface
        from app.datasync.base import SyncStatus
        mock_ingest.side_effect = [10, RuntimeError("fail"), 20, 30, 40, 50]
        result = AkShareIndexDailyInterface().sync_date(date(2024, 1, 15))
        assert result.status in (SyncStatus.PARTIAL, SyncStatus.SUCCESS)

    def test_index_spot_info(self):
        from app.datasync.sources.akshare.interfaces import AkShareIndexSpotInterface
        info = AkShareIndexSpotInterface().info
        assert info.interface_key == "stock_zh_index_spot"

    def test_etf_daily_info(self):
        from app.datasync.sources.akshare.interfaces import AkShareETFDailyInterface
        info = AkShareETFDailyInterface().info
        assert info.interface_key == "fund_etf_daily"
        assert len(AkShareETFDailyInterface.ETF_SYMBOLS) >= 5


# ═══════════════════════════════════════════════════════════════
#  vnpy_trading_service
# ═══════════════════════════════════════════════════════════════


class TestVnpyTradingService:
    def test_gateway_type_enum(self):
        from app.domains.trading.vnpy_trading_service import GatewayType
        assert GatewayType.CTP == "ctp"
        assert GatewayType.XTP == "xtp"
        assert GatewayType.SIMULATED == "sim"

    def test_order_event_dataclass(self):
        from app.domains.trading.vnpy_trading_service import OrderEvent
        ev = OrderEvent(order_id="o1", symbol="000001", direction="buy", status="filled")
        assert ev.order_id == "o1"
        assert ev.filled_quantity == 0
        assert ev.fee == 0

    def test_trade_event_dataclass(self):
        from app.domains.trading.vnpy_trading_service import TradeEvent
        ev = TradeEvent(trade_id="t1", order_id="o1", symbol="000001", direction="buy", price=10.0, volume=100)
        assert ev.trade_id == "t1"

    def test_position_snapshot(self):
        from app.domains.trading.vnpy_trading_service import PositionSnapshot
        p = PositionSnapshot(symbol="000001", direction="long", volume=100)
        assert p.frozen == 0

    def test_account_snapshot(self):
        from app.domains.trading.vnpy_trading_service import AccountSnapshot
        a = AccountSnapshot(balance=100000, available=80000)
        assert a.frozen == 0
        assert a.margin == 0


# ═══════════════════════════════════════════════════════════════
#  paper_strategy_executor
# ═══════════════════════════════════════════════════════════════


class TestPaperCtaEngine:
    def _cls(self):
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine
        return _PaperCtaEngine

    def test_init(self):
        cls = self._cls()
        executor = MagicMock()
        eng = cls(executor, 1, 2, 3, "000001.SSE", "auto")
        assert eng.deployment_id == 1
        assert eng.paper_account_id == 2
        assert eng.user_id == 3
        assert eng._order_counter == 0

    def test_cancel_order(self):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        eng.cancel_order(MagicMock(), "vt1")  # Should not raise

    def test_cancel_all(self):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        eng.cancel_all(MagicMock())  # Should not raise

    def test_write_log(self):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        eng.write_log("test message")  # Should not raise

    def test_get_pricetick(self):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        assert eng.get_pricetick("000001.SSE") == 0.01

    def test_put_event(self):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        eng.put_event()  # noop

    def test_send_email(self):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        eng.send_email("test")  # noop

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_get_market(self, mock_cf):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        row = MagicMock()
        row.market = "CN"
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = row
        assert eng._get_market() == "CN"

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_get_market_not_found(self, mock_cf):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
        assert eng._get_market() == "CN"

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_get_strategy_id(self, mock_cf):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        row = MagicMock()
        row.strategy_id = 42
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = row
        assert eng._get_strategy_id() == 42

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_get_strategy_id_not_found(self, mock_cf):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "auto")
        mock_cf.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
        assert eng._get_strategy_id() is None

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_write_signal(self, mock_cf):
        cls = self._cls()
        eng = cls(MagicMock(), 1, 2, 3, "000001.SSE", "semi_auto")
        ctx = mock_cf.return_value.__enter__.return_value
        eng._write_signal("buy", 100, 10.5, "test signal")
        ctx.execute.assert_called_once()
        ctx.commit.assert_called_once()


class TestPaperStrategyExecutor:
    def _cls(self):
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
        return PaperStrategyExecutor

    def test_is_running_false(self):
        cls = self._cls()
        inst = cls.__new__(cls)
        inst._initialized = True
        inst._threads = {}
        inst._stop_events = {}
        assert inst.is_running(999) is False

    def test_stop_deployment_no_event(self):
        cls = self._cls()
        inst = cls.__new__(cls)
        inst._initialized = True
        inst._threads = {}
        inst._stop_events = {}
        assert inst.stop_deployment(999) is False

    def test_quote_to_bar_valid(self):
        cls = self._cls()
        quote = {"last_price": 10.5, "open": 10.0, "high": 11.0, "low": 9.5, "volume": 1000}
        bar = cls._quote_to_bar(quote, "000001.SSE")
        if bar is not None:  # vnpy might not be installed
            assert bar.close_price == 10.5

    def test_quote_to_bar_zero_price(self):
        cls = self._cls()
        quote = {"last_price": 0}
        bar = cls._quote_to_bar(quote, "000001.SSE")
        assert bar is None

    def test_quote_to_bar_no_price(self):
        cls = self._cls()
        quote = {}
        bar = cls._quote_to_bar(quote, "000001.SSE")
        assert bar is None


# ═══════════════════════════════════════════════════════════════
#  backtest_service helpers
# ═══════════════════════════════════════════════════════════════


class TestBacktestServiceHelpers:
    def _m(self):
        from app.api.services import backtest_service as m
        return m

    def test_calculate_alpha_beta_normal(self):
        m = self._m()
        s = np.array([0.01, 0.02, -0.01, 0.03, 0.0])
        b = np.array([0.005, 0.015, -0.005, 0.02, 0.01])
        alpha, beta = m.calculate_alpha_beta(s, b)
        assert alpha is not None
        assert beta is not None

    def test_calculate_alpha_beta_short(self):
        m = self._m()
        alpha, beta = m.calculate_alpha_beta(np.array([0.01]), np.array([0.02]))
        assert alpha is None and beta is None

    def test_calculate_alpha_beta_all_nan(self):
        m = self._m()
        alpha, beta = m.calculate_alpha_beta(np.array([np.nan, np.nan]), np.array([np.nan, np.nan]))
        assert alpha is None and beta is None

    def test_calculate_alpha_beta_diff_lengths(self):
        m = self._m()
        s = np.array([0.01, 0.02, -0.01])
        b = np.array([0.005, 0.015])
        alpha, beta = m.calculate_alpha_beta(s, b)
        # min_len=2, after truncation both have 2 elements → polyfit works
        assert alpha is not None and beta is not None

    def test_convert_to_tushare_symbol_szse(self):
        m = self._m()
        assert m.convert_to_tushare_symbol("000001.SZSE") == "000001.SZ"

    def test_convert_to_tushare_symbol_sse(self):
        m = self._m()
        assert m.convert_to_tushare_symbol("600000.SSE") == "600000.SH"

    def test_convert_to_tushare_symbol_bse(self):
        m = self._m()
        assert m.convert_to_tushare_symbol("430047.BSE") == "430047.BJ"

    def test_convert_to_tushare_symbol_no_dot(self):
        m = self._m()
        assert m.convert_to_tushare_symbol("000001") == "000001"

    def test_convert_to_tushare_symbol_empty(self):
        m = self._m()
        assert m.convert_to_tushare_symbol("") == ""

    def test_convert_to_tushare_symbol_already_ts(self):
        m = self._m()
        assert m.convert_to_tushare_symbol("000001.SZ") == "000001.SZ"

    @patch("app.api.services.backtest_service.AkshareBenchmarkDao")
    def test_get_benchmark_data_success(self, mock_dao):
        m = self._m()
        mock_dao.return_value.get_benchmark_data.return_value = {"total_return": 0.15, "returns": []}
        result = m.get_benchmark_data(date(2024, 1, 1), date(2024, 6, 30))
        assert result["total_return"] == 0.15

    @patch("app.api.services.backtest_service.AkshareBenchmarkDao")
    def test_get_benchmark_data_exception(self, mock_dao):
        m = self._m()
        mock_dao.return_value.get_benchmark_data.side_effect = RuntimeError("db")
        result = m.get_benchmark_data(date(2024, 1, 1), date(2024, 6, 30))
        assert result is None

    @patch("app.api.services.backtest_service.MarketService")
    def test_get_stock_name_success(self, mock_svc):
        m = self._m()
        mock_svc.return_value.resolve_symbol_name.return_value = "平安银行"
        assert m.get_stock_name("000001.SZ") == "平安银行"

    @patch("app.api.services.backtest_service.MarketService")
    def test_get_stock_name_exception(self, mock_svc):
        m = self._m()
        mock_svc.return_value.resolve_symbol_name.side_effect = Exception("err")
        assert m.get_stock_name("000001.SZ") is None


# ═══════════════════════════════════════════════════════════════
#  worker/tasks helpers
# ═══════════════════════════════════════════════════════════════


class TestWorkerTaskHelpers:
    def _m(self):
        from app.worker.service import tasks as m
        return m

    def test_convert_to_vnpy_symbol_sz(self):
        m = self._m()
        assert m.convert_to_vnpy_symbol("000001.SZ") == "000001.SZSE"

    def test_convert_to_vnpy_symbol_sh(self):
        m = self._m()
        assert m.convert_to_vnpy_symbol("600000.SH") == "600000.SSE"

    def test_convert_to_vnpy_symbol_already_vnpy(self):
        m = self._m()
        assert m.convert_to_vnpy_symbol("000001.SZSE") == "000001.SZSE"

    def test_convert_to_vnpy_symbol_no_dot(self):
        m = self._m()
        assert m.convert_to_vnpy_symbol("000001") == "000001"

    def test_convert_to_tushare_symbol(self):
        m = self._m()
        assert m.convert_to_tushare_symbol("000001.SZSE") == "000001.SZ"

    @patch("app.worker.service.tasks.MarketService")
    def test_resolve_symbol_name(self, mock_svc):
        m = self._m()
        mock_svc.return_value.resolve_symbol_name.return_value = "平安银行"
        assert m.resolve_symbol_name("000001.SZ") == "平安银行"

    @patch("app.worker.service.tasks.MarketService")
    def test_resolve_symbol_name_error(self, mock_svc):
        m = self._m()
        mock_svc.return_value.resolve_symbol_name.side_effect = Exception("err")
        assert m.resolve_symbol_name("000001.SZ") == ""

    def test_calculate_alpha_beta_for_worker_normal(self):
        m = self._m()
        s = np.array([0.01, 0.02, -0.01, 0.03])
        b = np.array([0.005, 0.015, -0.005, 0.02])
        alpha, beta = m.calculate_alpha_beta_for_worker(s, b)
        assert alpha is not None

    def test_calculate_alpha_beta_for_worker_short(self):
        m = self._m()
        alpha, beta = m.calculate_alpha_beta_for_worker(np.array([0.01]), np.array([0.02]))
        assert alpha is None

    @patch("app.worker.service.tasks.AkshareBenchmarkDao")
    def test_get_benchmark_data_for_worker(self, mock_dao):
        m = self._m()
        mock_dao.return_value.get_benchmark_data.return_value = {"total_return": 0.1}
        result = m.get_benchmark_data_for_worker("2024-01-01", "2024-06-30")
        assert result["total_return"] == 0.1

    @patch("app.worker.service.tasks.AkshareBenchmarkDao")
    def test_get_benchmark_data_for_worker_error(self, mock_dao):
        m = self._m()
        mock_dao.return_value.get_benchmark_data.side_effect = Exception("db")
        result = m.get_benchmark_data_for_worker("2024-01-01", "2024-06-30")
        assert result is None

    @patch("app.worker.service.tasks.BacktestHistoryDao")
    def test_save_backtest_to_db(self, mock_dao):
        m = self._m()
        m.save_backtest_to_db(
            job_id="j1", user_id=1, strategy_id=None, strategy_class="Test",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-30",
            parameters={}, status="completed", result={"total_return": 0.1},
        )
        mock_dao.return_value.upsert_history.assert_called_once()

    @patch("app.worker.service.tasks.BacktestHistoryDao")
    def test_save_backtest_to_db_error(self, mock_dao):
        m = self._m()
        mock_dao.return_value.upsert_history.side_effect = Exception("db")
        m.save_backtest_to_db(
            job_id="j1", user_id=1, strategy_id=None, strategy_class="Test",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-30",
            parameters={}, status="failed", result=None,
        )  # Should not raise

    def test_build_optimization_setting_basic(self):
        m = self._m()
        setting = m._build_optimization_setting(
            {"fast_window": {"min": 5, "max": 20, "step": 5}},
            "sharpe_ratio",
        )
        assert setting.target_name == "sharpe_ratio"

    def test_build_optimization_setting_fixed_value(self):
        m = self._m()
        setting = m._build_optimization_setting({"x": 10.0})
        assert setting is not None

    def test_build_optimization_setting_bad_step(self):
        m = self._m()
        setting = m._build_optimization_setting(
            {"x": {"min": 5, "max": 20, "step": -1}},
        )
        assert setting is not None  # Should skip the param but not fail

    def test_build_optimization_setting_non_finite(self):
        m = self._m()
        setting = m._build_optimization_setting(
            {"x": {"min": float("nan"), "max": 20, "step": 1}},
        )
        assert setting is not None

    def test_build_optimization_setting_empty(self):
        m = self._m()
        setting = m._build_optimization_setting({})
        assert setting is not None

    @patch("app.worker.service.tasks.BacktestHistoryDao")
    def test_resolve_optimization_context_from_history(self, mock_dao):
        m = self._m()
        mock_dao.return_value.get_latest_strategy_run.return_value = {
            "vt_symbol": "000001.SZ",
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 6, 30),
        }
        sym, sd, ed = m._resolve_optimization_context(1, 1)
        assert sym == "000001.SZ"
        assert "2024-01-01" in sd

    @patch("app.worker.service.tasks.BacktestHistoryDao")
    def test_resolve_optimization_context_fallback(self, mock_dao):
        m = self._m()
        mock_dao.return_value.get_latest_strategy_run.return_value = None
        sym, sd, ed = m._resolve_optimization_context(1, 1)
        assert sym == "000001.SZ"  # Default fallback

    def test_normalize_optimization_results(self):
        m = self._m()
        raw = [
            ({"x": 10}, 1.5, {"sharpe_ratio": 1.5, "total_return": 0.2, "annual_return": 0.1}),
            ({"x": 20}, 1.2, {"sharpe_ratio": 1.2, "total_return": 0.15, "max_drawdown": 0.05}),
        ]
        results = m._normalize_optimization_results(raw, "sharpe_ratio")
        assert len(results) == 2
        assert results[0]["rank_order"] == 1
        assert results[0]["parameters"]["x"] == 10

    def test_normalize_optimization_results_bad_row(self):
        m = self._m()
        raw = [(None,), {"x": 1}]  # Bad entries
        results = m._normalize_optimization_results(raw, "sharpe_ratio")
        assert results == []

    @patch("app.worker.service.tasks.BulkBacktestDao")
    def test_update_bulk_row(self, mock_dao):
        m = self._m()
        m._update_bulk_row("j1", 5, 0.15, "000001.SZ", "平安银行")
        mock_dao.return_value.update_progress.assert_called_once()

    @patch("app.worker.service.tasks.BulkBacktestDao")
    def test_finish_bulk_row(self, mock_dao):
        m = self._m()
        m._finish_bulk_row("j1", "completed", 0.15, "000001.SZ", "平安银行", 10)
        mock_dao.return_value.finish.assert_called_once()

    @patch("app.worker.service.tasks.BulkBacktestDao")
    def test_update_bulk_row_error(self, mock_dao):
        m = self._m()
        mock_dao.return_value.update_progress.side_effect = Exception("db")
        m._update_bulk_row("j1", 5, 0.15, "000001.SZ")  # Should not raise

    @patch("app.worker.service.tasks.BulkBacktestDao")
    def test_finish_bulk_row_error(self, mock_dao):
        m = self._m()
        mock_dao.return_value.finish.side_effect = Exception("db")
        m._finish_bulk_row("j1", "failed", None, None, None, 0)  # Should not raise

    @patch("app.datasync.scheduler.run_daily_sync", return_value={"ok": True})
    def test_run_datasync_task(self, mock_sync):
        m = self._m()
        r = m.run_datasync_task()
        assert r["status"] == "ok"

    @patch("app.datasync.scheduler.run_daily_sync", return_value={"ok": True})
    def test_run_datasync_task_with_date(self, mock_sync):
        m = self._m()
        r = m.run_datasync_task(target_date_str="2024-01-15")
        assert r["status"] == "ok"

    @patch("app.datasync.scheduler.run_daily_sync", side_effect=RuntimeError("fail"))
    def test_run_datasync_task_error(self, mock_sync):
        m = self._m()
        r = m.run_datasync_task()
        assert r["status"] == "error"

    @patch("app.worker.service.tasks.OptimizationTaskDao")
    def test_run_optimization_record_task_not_found(self, mock_dao):
        m = self._m()
        mock_dao.return_value.get_task_for_worker.return_value = None
        r = m.run_optimization_record_task(999)
        assert r["status"] == "failed"
        assert "not found" in r["error"]


# ═══════════════════════════════════════════════════════════════
#  backtest_service V2 methods
# ═══════════════════════════════════════════════════════════════


class TestBacktestServiceV2:
    def _svc(self):
        from app.api.services.backtest_service import BacktestServiceV2
        with patch("app.api.services.backtest_service.get_job_storage") as mock_storage:
            svc = BacktestServiceV2()
            svc._mock_storage = mock_storage
            return svc

    @patch("app.api.services.backtest_service.MarketService")
    @patch("app.api.services.backtest_service.get_queue")
    @patch("app.api.services.backtest_service.get_job_storage")
    def test_submit_backtest(self, mock_storage, mock_queue, mock_market):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_market.return_value.resolve_symbol_name.return_value = "平安银行"
        svc = BacktestServiceV2()
        job_id = svc.submit_backtest(
            user_id=1, strategy_id=None, version_id=None, source=None,
            strategy_class_name="TestStrategy",
            symbol="000001.SZ", start_date=date(2024, 1, 1), end_date=date(2024, 6, 30),
        )
        assert job_id.startswith("bt_")

    @patch("app.api.services.backtest_service.get_job_storage")
    def test_list_user_jobs(self, mock_storage):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_storage.return_value.list_user_jobs.return_value = [{"job_id": "j1"}]
        svc = BacktestServiceV2()
        jobs = svc.list_user_jobs(user_id=1)
        assert len(jobs) == 1

    @patch("app.api.services.backtest_service.get_queue")
    @patch("app.api.services.backtest_service.get_job_storage")
    def test_cancel_job_not_found(self, mock_storage, mock_queue):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_storage.return_value.get_job_metadata.return_value = None
        svc = BacktestServiceV2()
        assert svc.cancel_job("j1", user_id=1) is False

    @patch("app.api.services.backtest_service.get_queue")
    @patch("app.api.services.backtest_service.get_job_storage")
    def test_cancel_job_wrong_user(self, mock_storage, mock_queue):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_storage.return_value.get_job_metadata.return_value = {"user_id": 999}
        svc = BacktestServiceV2()
        assert svc.cancel_job("j1", user_id=1) is False

    @patch("app.api.services.backtest_service.StrategySourceDao")
    @patch("app.api.services.backtest_service.get_job_storage")
    def test_get_strategy_from_db_not_found(self, mock_storage, mock_dao):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_dao.return_value.get_strategy_source_for_user.side_effect = KeyError("not found")
        svc = BacktestServiceV2()
        with pytest.raises(ValueError, match="not found"):
            svc._get_strategy_from_db(999, 1)

    @patch("app.api.services.backtest_service.get_job_storage")
    def test_get_job_status_not_found(self, mock_storage):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_storage.return_value.get_job_metadata.return_value = None
        with patch("app.api.services.backtest_service.BacktestHistoryDao") as mock_dao:
            mock_dao.return_value.get_job_row.return_value = None
            svc = BacktestServiceV2()
            result = svc.get_job_status("j1", user_id=1)
            assert result is None

    @patch("app.api.services.backtest_service.get_job_storage")
    def test_get_job_status_wrong_user(self, mock_storage):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_storage.return_value.get_job_metadata.return_value = {"user_id": 999, "status": "completed"}
        svc = BacktestServiceV2()
        result = svc.get_job_status("j1", user_id=1)
        assert result is None

    @patch("app.api.services.backtest_service.get_job_storage")
    def test_get_job_status_completed(self, mock_storage):
        from app.api.services.backtest_service import BacktestServiceV2
        mock_storage.return_value.get_job_metadata.return_value = {
            "user_id": 1, "status": "completed", "type": "backtest",
            "progress": 100, "created_at": "2024-01-01", "symbol": "000001.SZ",
        }
        mock_storage.return_value.get_result.return_value = {"total_return": 0.1}
        svc = BacktestServiceV2()
        result = svc.get_job_status("j1", user_id=1)
        assert result["status"] == "completed"
        assert result["result"]["total_return"] == 0.1

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


@dataclass
class _Info:
    interface_key: str
    source_key: str
    target_table: str


class _Iface:
    def __init__(self, *, interface_key: str, source_key: str, target_table: str, date_column=None, key_fields=()):
        self.info = _Info(interface_key=interface_key, source_key=source_key, target_table=target_table)
        self._date_column = date_column
        self._key_fields = tuple(key_fields)

    def _date_param(self):
        return self._date_column

    def _payload_key_fields(self):
        return self._key_fields


class _Ctx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, stmt, params=None):
        self.statements.append(str(stmt))
        return MagicMock()


class _Engine:
    def __init__(self):
        self.conn = _Conn()

    def connect(self):
        return _Ctx(self.conn)

    def begin(self):
        return _Ctx(self.conn)


class TestTushareDynamicTableMigration:
    def test_list_dynamic_migration_targets_filters_sample_inferred_tables(self):
        from app.datasync.service import tushare_dynamic_table_migration as mod

        registry = MagicMock()
        registry.all_interfaces.return_value = [
            _Iface(interface_key="stock_daily", source_key="tushare", target_table="stock_daily"),
            _Iface(
                interface_key="report_rc",
                source_key="tushare",
                target_table="report_rc",
                date_column="ann_date",
                key_fields=("ts_code", "ann_date"),
            ),
            _Iface(interface_key="index_daily", source_key="akshare", target_table="index_daily"),
        ]

        targets = mod.list_dynamic_migration_targets(registry=registry)

        assert targets == [
            mod.DynamicTableMigrationTarget(
                interface_key="report_rc",
                table_name="report_rc",
                preferred_date_column="ann_date",
                preferred_key_fields=("ts_code", "ann_date"),
            )
        ]

    def test_legacy_row_to_record_prefers_existing_columns_over_payload(self):
        from app.datasync.service import tushare_dynamic_table_migration as mod

        row = {
            "data": '{"ann_date":"20240105","ts_code":"000001.SZ","name":"old"}',
            "name": "new",
            "eps": 1.23,
        }

        record = mod._legacy_row_to_record(row, ["name", "eps"])

        assert record == {
            "ann_date": "20240105",
            "ts_code": "000001.SZ",
            "name": "new",
            "eps": 1.23,
        }

    def test_migrate_dynamic_table_dry_run_reports_plan(self, monkeypatch):
        from app.datasync.service import tushare_dynamic_table_migration as mod

        target = mod.DynamicTableMigrationTarget(
            interface_key="report_rc",
            table_name="report_rc",
            preferred_date_column="ann_date",
            preferred_key_fields=("ts_code", "ann_date"),
        )
        engine = _Engine()
        monkeypatch.setattr(mod, "get_tushare_engine", lambda: engine)
        monkeypatch.setattr(
            mod,
            "_get_table_columns",
            lambda conn, table_name: [{"name": "id", "type": "bigint"}, {"name": "key_hash", "type": "char(64)"}, {"name": "data", "type": "json"}],
        )
        monkeypatch.setattr(mod, "_count_rows", lambda conn, table_name: 5)
        monkeypatch.setattr(
            mod,
            "_load_sample_records",
            lambda conn, table_name, column_names, sample_size: [{"ann_date": "20240105", "ts_code": "000001.SZ", "eps": 1.23}],
        )

        result = mod.migrate_dynamic_table(target, dry_run=True)

        assert result.status == "dry_run"
        assert result.table_name == "report_rc"
        assert result.temp_table == "report_rc__parsed_tmp"
        assert result.backup_table == "report_rc__legacy_payload"

    def test_copy_into_replacement_table_replays_parsed_records(self, monkeypatch):
        from app.datasync.service import tushare_dynamic_table_migration as mod

        engine = _Engine()
        monkeypatch.setattr(mod, "get_tushare_engine", lambda: engine)
        monkeypatch.setattr(
            mod,
            "_get_table_columns",
            lambda conn, table_name: [
                {"name": "id", "type": "bigint"},
                {"name": "key_hash", "type": "char(64)"},
                {"name": "data", "type": "json"},
            ],
        )
        monkeypatch.setattr(
            mod,
            "_iter_source_batches",
            lambda engine, table_name, column_names, batch_size: [
                [SimpleNamespace(_mapping={"id": 1, "data": '{"ann_date":"20240105","ts_code":"000001.SZ","eps":1.23}'})]
            ],
        )
        seen: dict[str, object] = {}

        def _fake_upsert_rows(table_name, rows, *, column_specs, key_columns):
            seen["table_name"] = table_name
            seen["rows"] = rows
            seen["column_specs"] = column_specs
            seen["key_columns"] = key_columns
            return len(rows)

        monkeypatch.setattr(mod, "upsert_rows", _fake_upsert_rows)

        processed = mod._copy_into_replacement_table(
            source_table="report_rc",
            temp_table="report_rc__parsed_tmp",
            temp_ddl="CREATE TABLE IF NOT EXISTS `report_rc__parsed_tmp` (`ann_date` DATE NULL)",
            column_specs=[{"name": "ann_date", "source_fields": ["ann_date"], "normalizer": "date"}],
            key_columns=("ann_date", "ts_code"),
            batch_size=100,
        )

        assert processed == 1
        assert seen["table_name"] == "report_rc__parsed_tmp"
        assert seen["rows"] == [{"ann_date": "20240105", "ts_code": "000001.SZ", "eps": 1.23}]

    def test_swap_replacement_tables_can_drop_backup(self, monkeypatch):
        from app.datasync.service import tushare_dynamic_table_migration as mod

        engine = _Engine()
        monkeypatch.setattr(mod, "get_tushare_engine", lambda: engine)

        mod._swap_replacement_tables(
            source_table="report_rc",
            temp_table="report_rc__parsed_tmp",
            backup_table="report_rc__legacy_payload",
            keep_legacy_backup=False,
        )

        assert any("RENAME TABLE" in stmt for stmt in engine.conn.statements)
        assert any("DROP TABLE `report_rc__legacy_payload`" in stmt for stmt in engine.conn.statements)

    def test_migrate_dynamic_tables_collects_per_table_errors(self, monkeypatch):
        from app.datasync.service import tushare_dynamic_table_migration as mod

        target = mod.DynamicTableMigrationTarget(
            interface_key="report_rc",
            table_name="report_rc",
            preferred_date_column="ann_date",
            preferred_key_fields=("ts_code",),
        )
        monkeypatch.setattr(mod, "list_dynamic_migration_targets", lambda registry=None: [target])
        monkeypatch.setattr(mod, "migrate_dynamic_table", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

        results = mod.migrate_dynamic_tables()

        assert len(results) == 1
        assert results[0].status == "error"
        assert "boom" in results[0].message

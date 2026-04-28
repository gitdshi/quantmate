from unittest.mock import MagicMock

import pytest

import app.datasync.table_manager as _tm


@pytest.mark.unit
class TestStaticTableSchemaReconciliation:
    def test_ensure_table_adds_missing_static_index(self, monkeypatch):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 1
        mock_eng = MagicMock()
        mock_eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_eng.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_begin_conn = MagicMock()
        mock_eng.begin.return_value.__enter__ = MagicMock(return_value=mock_begin_conn)
        mock_eng.begin.return_value.__exit__ = MagicMock(return_value=False)

        monkeypatch.setattr(_tm, "_get_engine", lambda db: mock_eng)
        monkeypatch.setattr(_tm, "_mark_table_created", lambda db, tbl: None)
        monkeypatch.setattr(
            _tm,
            "_get_existing_columns",
            lambda db, tbl: {
                "id": {"is_nullable": "NO", "column_type": "bigint"},
                "ts_code": {"is_nullable": "NO", "column_type": "varchar(32)"},
                "ann_date": {"is_nullable": "YES", "column_type": "date"},
                "created_at": {"is_nullable": "YES", "column_type": "timestamp"},
            },
        )
        monkeypatch.setattr(_tm, "_get_existing_indexes", lambda db, tbl: [])

        ddl = """
CREATE TABLE IF NOT EXISTS dividend (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    ann_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY ux_dividend_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

        result = _tm.ensure_table("tushare", "dividend", ddl)

        assert result is False
        assert mock_begin_conn.execute.call_count == 1
        assert "ADD UNIQUE KEY `ux_dividend_ts_ann` (`ts_code`, `ann_date`)" in str(mock_begin_conn.execute.call_args.args[0])

    def test_parse_static_table_schema_deduplicates_columns(self):
        ddl = """
CREATE TABLE IF NOT EXISTS trade_cal (
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_trade_cal_date (cal_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

        column_specs, _key_columns, index_specs = _tm._parse_static_table_schema(ddl)

        assert [spec["name"] for spec in column_specs] == ["created_at", "updated_at"]
        assert index_specs == [{"name": "idx_trade_cal_date", "unique": False, "columns": ("cal_date",)}]
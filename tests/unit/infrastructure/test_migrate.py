"""Unit tests for app.infrastructure.db.migrate — SQL migration runner."""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from app.infrastructure.db.migrate import (
    _split_sql_statements,
    _strip_leading_sql_comments,
    _discover_migrations,
    _file_checksum,
    _ensure_migration_table,
    _get_applied,
    apply_migrations,
)


# ── _split_sql_statements ────────────────────────────────────────

def test_split_simple():
    result = _split_sql_statements("SELECT 1; SELECT 2;")
    assert result == ["SELECT 1", "SELECT 2"]


def test_split_no_trailing_semicolon():
    result = _split_sql_statements("SELECT 1")
    assert result == ["SELECT 1"]


def test_split_ignores_semicolons_in_single_quotes():
    result = _split_sql_statements("INSERT INTO t VALUES ('a;b'); SELECT 1")
    assert len(result) == 2
    assert "'a;b'" in result[0]


def test_split_ignores_semicolons_in_double_quotes():
    result = _split_sql_statements('INSERT INTO t VALUES ("a;b"); SELECT 1')
    assert len(result) == 2


def test_split_ignores_semicolons_in_line_comments():
    sql = "-- comment with ;\nSELECT 1; SELECT 2"
    result = _split_sql_statements(sql)
    assert len(result) == 2


def test_split_ignores_semicolons_in_block_comments():
    sql = "/* comment; with semicolon */ SELECT 1; SELECT 2"
    result = _split_sql_statements(sql)
    assert len(result) == 2


def test_split_empty():
    assert _split_sql_statements("") == []
    assert _split_sql_statements("   ") == []


def test_split_escaped_single_quotes():
    sql = "INSERT INTO t VALUES ('it''s ok'); SELECT 1"
    result = _split_sql_statements(sql)
    assert len(result) == 2


def test_split_escaped_double_quotes():
    sql = 'INSERT INTO t VALUES ("a""b"); SELECT 1'
    result = _split_sql_statements(sql)
    assert len(result) == 2


def test_split_complex_strategy_code():
    sql = textwrap.dedent("""\
        INSERT INTO strategies (code) VALUES ('
        class S:
            def on_init(self):
                pass;
                x = "hello; world"
        ');
        SELECT 1;
    """)
    result = _split_sql_statements(sql)
    assert len(result) == 2


# ── _strip_leading_sql_comments ──────────────────────────────────

def test_strip_line_comment():
    result = _strip_leading_sql_comments("-- hello\nSELECT 1")
    assert result == "SELECT 1"


def test_strip_block_comment():
    result = _strip_leading_sql_comments("/* block */  SELECT 1")
    assert result == "SELECT 1"


def test_strip_multiple_comments():
    result = _strip_leading_sql_comments("-- a\n/* b */\nSELECT 1")
    assert result == "SELECT 1"


def test_strip_only_comment():
    result = _strip_leading_sql_comments("-- only comment")
    assert result == ""


def test_strip_only_block_comment_unclosed():
    result = _strip_leading_sql_comments("/* unclosed")
    assert result == ""


def test_strip_no_comment():
    result = _strip_leading_sql_comments("SELECT 1")
    assert result == "SELECT 1"


def test_strip_whitespace_before_comment():
    result = _strip_leading_sql_comments("  -- hi\n  SELECT 1")
    assert result == "SELECT 1"


# ── _file_checksum ───────────────────────────────────────────────

def test_file_checksum(tmp_path):
    f = tmp_path / "test.sql"
    f.write_text("SELECT 1")
    checksum = _file_checksum(f)
    assert isinstance(checksum, str)
    assert len(checksum) == 64  # sha256 hex


# ── _discover_migrations ─────────────────────────────────────────

def test_discover_migrations_missing_dir():
    with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", Path("/nonexistent")):
        assert _discover_migrations() == []


def test_discover_migrations(tmp_path):
    (tmp_path / "001_init.sql").write_text("CREATE TABLE t (id INT)")
    (tmp_path / "002_add_col.sql").write_text("ALTER TABLE t ADD col INT")
    (tmp_path / "readme.txt").write_text("ignore")
    with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path):
        result = _discover_migrations()
    assert len(result) == 2
    assert result[0][0] == "001"
    assert result[1][0] == "002"


def test_discover_migrations_rejects_duplicate_versions(tmp_path):
    (tmp_path / "029_alpha.sql").write_text("SELECT 1")
    (tmp_path / "029_beta.sql").write_text("SELECT 2")
    with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path):
        with pytest.raises(ValueError, match="Duplicate migration version '029'"):
            _discover_migrations()


# ── _ensure_migration_table / _get_applied ────────────────────────

def test_ensure_migration_table():
    conn = MagicMock()
    _ensure_migration_table(conn)
    conn.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_get_applied():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [("001",), ("002",)]
    result = _get_applied(conn)
    assert result == {"001", "002"}


# ── apply_migrations ─────────────────────────────────────────────

def test_apply_migrations_dry_run(tmp_path):
    (tmp_path / "001_init.sql").write_text("CREATE TABLE t (id INT);")
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = []  # no applied

    with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path), \
         patch("app.infrastructure.db.migrate.get_quantmate_engine", return_value=mock_engine):
        applied = apply_migrations(dry_run=True)
    assert applied == ["001"]
    mock_conn.exec_driver_sql.assert_not_called()


def test_apply_migrations_real(tmp_path):
    (tmp_path / "001_init.sql").write_text("CREATE TABLE t (id INT);")
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = []

    with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path), \
         patch("app.infrastructure.db.migrate.get_quantmate_engine", return_value=mock_engine):
        applied = apply_migrations(dry_run=False)
    assert applied == ["001"]
    mock_conn.exec_driver_sql.assert_called_once()
    mock_conn.commit.assert_called()


def test_apply_migrations_skips_already_applied(tmp_path):
    (tmp_path / "001_init.sql").write_text("SELECT 1;")
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = [("001",)]

    with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path), \
         patch("app.infrastructure.db.migrate.get_quantmate_engine", return_value=mock_engine):
        applied = apply_migrations()
    assert applied == []

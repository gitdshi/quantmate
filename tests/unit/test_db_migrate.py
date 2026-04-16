"""Tests for Issue #15: DB Migration framework."""
import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.infrastructure.db.migrate import (
    _discover_migrations,
    _file_checksum,
    MIGRATIONS_DIR,
)


class TestDiscoverMigrations:
    def test_discovers_sql_files_sorted(self, tmp_path):
        """Discovers and sorts migration files by name."""
        (tmp_path / "002_add_table.sql").write_text("CREATE TABLE t2;")
        (tmp_path / "001_init.sql").write_text("CREATE TABLE t1;")
        (tmp_path / "003_alter.sql").write_text("ALTER TABLE t1;")

        with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path):
            migrations = _discover_migrations()

        assert len(migrations) == 3
        assert migrations[0][0] == "001"
        assert migrations[1][0] == "002"
        assert migrations[2][0] == "003"

    def test_empty_directory(self, tmp_path):
        with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path):
            migrations = _discover_migrations()
        assert migrations == []

    def test_nonexistent_directory(self, tmp_path):
        missing = tmp_path / "missing"
        with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", missing):
            migrations = _discover_migrations()
        assert migrations == []

    def test_ignores_non_sql_files(self, tmp_path):
        (tmp_path / "001_init.sql").write_text("CREATE TABLE t;")
        (tmp_path / "README.md").write_text("Docs")
        (tmp_path / "notes.txt").write_text("Notes")

        with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path):
            migrations = _discover_migrations()
        assert len(migrations) == 1

    def test_rejects_duplicate_versions(self, tmp_path):
        (tmp_path / "029_alpha.sql").write_text("SELECT 1;")
        (tmp_path / "029_beta.sql").write_text("SELECT 2;")

        with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path):
            with pytest.raises(ValueError, match="Duplicate migration version '029'"):
                _discover_migrations()


class TestFileChecksum:
    def test_consistent_checksum(self, tmp_path):
        f = tmp_path / "test.sql"
        f.write_text("SELECT 1;")
        c1 = _file_checksum(f)
        c2 = _file_checksum(f)
        assert c1 == c2
        assert len(c1) == 64  # SHA-256 hex

    def test_different_content_different_checksum(self, tmp_path):
        f1 = tmp_path / "a.sql"
        f2 = tmp_path / "b.sql"
        f1.write_text("SELECT 1;")
        f2.write_text("SELECT 2;")
        assert _file_checksum(f1) != _file_checksum(f2)


class TestMigrationsDirExists:
    def test_migrations_dir_points_to_mysql(self):
        """MIGRATIONS_DIR should point to mysql/migrations/."""
        assert MIGRATIONS_DIR.name == "migrations"
        assert MIGRATIONS_DIR.parent.name == "mysql"

    def test_initial_migration_exists(self):
        """The migration table creation script should exist."""
        init_file = MIGRATIONS_DIR / "000_create_migration_table.sql"
        assert init_file.exists()


class TestApplyMigrations:
    """Test apply_migrations with mock DB."""

    def test_dry_run_returns_pending(self, tmp_path):
        (tmp_path / "001_init.sql").write_text("CREATE TABLE t;")
        (tmp_path / "002_add.sql").write_text("ALTER TABLE t;")

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path), \
             patch("app.infrastructure.db.migrate.get_quantmate_engine", return_value=mock_engine):
            from app.infrastructure.db.migrate import apply_migrations
            applied = apply_migrations(dry_run=True)

        assert len(applied) == 2
        assert "001" in applied
        assert "002" in applied

    def test_skip_already_applied(self, tmp_path):
        (tmp_path / "001_init.sql").write_text("CREATE TABLE t;")
        (tmp_path / "002_add.sql").write_text("ALTER TABLE t;")

        mock_conn = MagicMock()
        # Simulate "001" already applied
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: "001"
        mock_conn.execute.return_value.fetchall.return_value = [mock_row]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("app.infrastructure.db.migrate.MIGRATIONS_DIR", tmp_path), \
             patch("app.infrastructure.db.migrate.get_quantmate_engine", return_value=mock_engine):
            from app.infrastructure.db.migrate import apply_migrations
            applied = apply_migrations(dry_run=True)

        assert len(applied) == 1
        assert "002" in applied

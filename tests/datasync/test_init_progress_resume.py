"""Unit tests for init_market_data.py checkpoint/resume logic.

Target: P1-DSYNC-CODE-001 - Init 断点续跑 + 自适应限流实现
"""

import os
import pytest
from unittest.mock import Mock, patch
from datetime import date, datetime

# Set required environment variables before importing
os.environ.setdefault('MYSQL_URL', 'mysql+pymysql://test:test@localhost:3306')
os.environ.setdefault('SECRET_KEY', 'test-secret-key-for-tests')
os.environ.setdefault('MYSQL_PASSWORD', 'test-password')
os.environ.setdefault('TUSHARE_DATABASE_URL', 'mysql+pymysql://test:test@localhost:3306/testdb')
os.environ.setdefault('TUSHARE_TOKEN', 'test_token_abc123')

# Import module under test using importlib to avoid import errors with missing DB
import importlib.util
spec = importlib.util.spec_from_file_location("init_market_data", "scripts/init_market_data.py")
imd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(imd)


class TestPhaseRank:
    """Test phase_rank ordering matches PHASES list."""

    def test_phase_order(self):
        expected_order = [
            'schema', 'stock_basic', 'daily', 'indexes', 'adj_factor',
            'dividend', 'top10_holders', 'vnpy', 'sync_status', 'finished'
        ]
        for i, phase in enumerate(expected_order):
            assert imd.phase_rank(phase) == i

    def test_unknown_phase(self):
        assert imd.phase_rank('unknown') == -1


class TestShouldRunPhase:
    """Test should_run_phase determines correct execution flow."""

    def test_no_progress_resume_true(self):
        """When resume=True but no previous progress, should run all phases."""
        assert imd.should_run_phase(None, 'stock_basic', resume=True) is True
        assert imd.should_run_phase(None, 'daily', resume=True) is True

    def test_resume_false_always_runs(self):
        """When resume=False, should always run regardless of progress."""
        progress = {'phase': 'stock_basic', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=False) is True
        assert imd.should_run_phase(progress, 'daily', resume=False) is True

    def test_completed_phase_skipped(self):
        """A phase with status='completed' should not run again."""
        progress = {'phase': 'stock_basic', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is False

    def test_incomplete_phase_continues(self):
        """If phase was incomplete (error or running), should run again."""
        progress = {'phase': 'stock_basic', 'status': 'error'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is True
        progress = {'phase': 'stock_basic', 'status': 'running'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is True

    def test_subsequent_phases_run(self):
        """Phases after saved phase should run."""
        progress = {'phase': 'stock_basic', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'daily', resume=True) is True
        assert imd.should_run_phase(progress, 'indexes', resume=True) is True

    def test_prior_phases_skipped(self):
        """Phases before saved phase should NOT run (can't go backwards)."""
        progress = {'phase': 'daily', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is False

    def test_future_phases_from_middle(self):
        """From a middle phase, later phases should run."""
        progress = {'phase': 'daily', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'indexes', resume=True) is True
        assert imd.should_run_phase(progress, 'adj_factor', resume=True) is True

    def test_finished_phase_blocks_all(self):
        """If finished, no phases should run."""
        progress = {'phase': 'finished', 'status': 'completed'}
        assert imd.should_run_phase(progress, 'stock_basic', resume=True) is False
        assert imd.should_run_phase(progress, 'daily', resume=True) is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

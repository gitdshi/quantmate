"""Tests for CtaStrategyRunner singleton and lifecycle."""
import pytest
from unittest.mock import patch, MagicMock

from app.domains.trading.cta_strategy_runner import CtaStrategyRunner


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton instance before each test."""
    CtaStrategyRunner._instance = None
    yield
    CtaStrategyRunner._instance = None


class TestSingleton:

    def test_is_singleton(self):
        r1 = CtaStrategyRunner()
        r2 = CtaStrategyRunner()
        assert r1 is r2

    def test_initialized_once(self):
        runner = CtaStrategyRunner()
        assert runner._initialized is True
        assert runner._running_strategies == {}


class TestStartStrategy:

    @patch.object(CtaStrategyRunner, "_load_strategy_class")
    def test_start_strategy_success(self, mock_load):
        mock_load.return_value = MagicMock()  # A compiled strategy class
        runner = CtaStrategyRunner()
        result = runner.start_strategy(
            strategy_class_name="DoubleMaStrategy",
            vt_symbol="IF2406.CFFEX",
            parameters={"fast_window": 10, "slow_window": 30},
            user_id=1,
        )
        assert result["success"] is True
        assert "strategy_name" in result
        assert "DoubleMaStrategy" in result["strategy_name"]

    @patch.object(CtaStrategyRunner, "_load_strategy_class")
    def test_start_strategy_adds_to_running(self, mock_load):
        mock_load.return_value = MagicMock()
        runner = CtaStrategyRunner()
        result = runner.start_strategy(
            strategy_class_name="TestStrat",
            vt_symbol="rb2406.SHFE",
            parameters={},
        )
        strategies = runner.list_strategies()
        assert len(strategies) == 1
        assert strategies[0]["class_name"] == "TestStrat"
        assert strategies[0]["status"] == "running"

    @patch.object(CtaStrategyRunner, "_load_strategy_class", side_effect=Exception("Compile error"))
    def test_start_strategy_compile_failure(self, mock_load):
        runner = CtaStrategyRunner()
        result = runner.start_strategy(
            strategy_class_name="BadStrat",
            vt_symbol="IF2406.CFFEX",
            parameters={},
        )
        assert result["success"] is False
        assert "Compile error" in result["error"]


class TestStopStrategy:

    @patch.object(CtaStrategyRunner, "_load_strategy_class")
    def test_stop_running_strategy(self, mock_load):
        mock_load.return_value = MagicMock()
        runner = CtaStrategyRunner()
        result = runner.start_strategy(
            strategy_class_name="TestStrat",
            vt_symbol="IF2406.CFFEX",
            parameters={},
        )
        name = result["strategy_name"]
        assert runner.stop_strategy(name) is True
        assert len(runner.list_strategies()) == 0

    def test_stop_nonexistent_strategy(self):
        runner = CtaStrategyRunner()
        assert runner.stop_strategy("no_such_strategy") is False


class TestListStrategies:

    def test_empty_initially(self):
        runner = CtaStrategyRunner()
        assert runner.list_strategies() == []

    @patch.object(CtaStrategyRunner, "_load_strategy_class")
    def test_lists_multiple(self, mock_load):
        mock_load.return_value = MagicMock()
        runner = CtaStrategyRunner()
        runner.start_strategy("Strat1", "IF2406.CFFEX", {})
        runner.start_strategy("Strat2", "rb2406.SHFE", {"window": 20})
        strategies = runner.list_strategies()
        assert len(strategies) == 2
        names = {s["class_name"] for s in strategies}
        assert names == {"Strat1", "Strat2"}

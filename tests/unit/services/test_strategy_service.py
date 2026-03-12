"""
Unit tests for Strategy validation service.

Tests the StrategyValidation logic for code parsing and security checks.
"""
import pytest
from app.api.services.strategy_service import validate_strategy_code
from app.api.models.strategy import StrategyValidation


@pytest.mark.unit
class TestValidateStrategyCode:
    """Tests for strategy code validation."""

    def test_valid_simple_strategy(self):
        """Test validation passes for a minimal valid strategy."""
        # Arrange
        code = """
class MyStrategy:
    def on_init(self):
        pass

    def on_bar(self, bar):
        pass
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is True
        assert len(result.errors) == 0

    def test_syntax_error(self):
        """Test validation catches Python syntax errors."""
        # Arrange
        code = """
class MyStrategy:
    def on_init(self)
        pass  # Missing colon
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is False
        assert any("syntax error" in e.lower() for e in result.errors)

    def test_missing_required_method(self):
        """Test validation warns about missing required methods."""
        # Arrange
        code = """
class MyStrategy:
    def on_bar(self, bar):
        pass
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is False
        assert any("Missing required method: on_init" in e for e in result.errors)

    def test_no_on_bar_or_on_tick_warns(self):
        """Test validation warns if neither on_bar nor on_tick is present."""
        # Arrange
        code = """
class MyStrategy:
    def on_init(self):
        pass
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is True  # Still valid (only warning)
        assert any("should implement on_bar or on_tick" in w for w in result.warnings)

    def test_class_name_mismatch(self):
        """Test validation fails when class name doesn't match."""
        # Arrange
        code = """
class DifferentName:
    def on_init(self):
        pass
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is False
        assert any("Class 'MyStrategy' not found" in e for e in result.errors)

    def test_dangerous_import_warns(self):
        """Test validation warns about potentially dangerous imports."""
        # Arrange
        code = """
import os
import subprocess
import pandas as pd

class MyStrategy:
    def on_init(self):
        pass
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is True  # Warning only, not invalid
        assert any("Import 'os' may be restricted" in w for w in result.warnings)
        assert any("Import 'subprocess' may be restricted" in w for w in result.warnings)

    def test_exec_eval_disallowed(self):
        """Test validation disallows exec/eval/compile."""
        # Arrange
        code = """
class MyStrategy:
    def on_init(self):
        exec("print('hello')")
        eval("1+1")
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is False
        assert any("Use of 'exec' is not allowed" in e for e in result.errors)
        assert any("Use of 'eval' is not allowed" in e for e in result.errors)

    def test_valid_with_pandas(self):
        """Test that pandas import is allowed."""
        # Arrange
        code = """
import pandas as pd
import numpy as np

class MyStrategy:
    def on_init(self):
        self.df = pd.DataFrame()

    def on_bar(self, bar):
        pass
"""
        # Act
        result = validate_strategy_code(code, "MyStrategy")

        # Assert
        assert result.valid is True
        # Should not have warnings about pandas/numpy

    def test_empty_code(self):
        """Test validation of empty code."""
        # Act
        result = validate_strategy_code("", "MyStrategy")

        # Assert
        assert result.valid is False
        # Empty code results in class not found (no syntax error due to empty AST)
        assert any("Class 'MyStrategy' not found" in e for e in result.errors)

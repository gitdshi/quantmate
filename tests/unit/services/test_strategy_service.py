"""
Unit tests for Strategy validation service.

Tests the StrategyValidation logic for code parsing and security checks.
"""
import pytest
from app.api.services.strategy_service import compile_strategy, parse_strategy_file, validate_strategy_code


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


@pytest.mark.unit
class TestCompileStrategy:
    def test_compile_strategy_returns_class(self):
        code = """
class MyStrategy:
    pass
"""
        compiled = compile_strategy(code, "MyStrategy")
        assert compiled.__name__ == "MyStrategy"

    def test_compile_strategy_raises_when_class_missing(self):
        code = "x = 1"
        with pytest.raises(RuntimeError, match="did not expose class"):
            compile_strategy(code, "MissingStrategy")


@pytest.mark.unit
class TestParseStrategyFile:
    def test_parse_strategy_file_extracts_class_defaults_and_parameter_order(self):
        content = """
DEFAULT_WINDOW = 20
parameters = ["global_threshold"]

class DemoStrategy:
    parameters = ["window", ("threshold", 1.5)]
    window = 10
    mode = "fast"

    def __init__(self):
        self.dynamic = 3
"""
        result = parse_strategy_file(content)

        assert result["classes"][0]["name"] == "DemoStrategy"
        assert result["classes"][0]["defaults"] == {"window": 10, "threshold": 1.5}
        assert result["classes"][0]["parameter_order"] == ["window", "threshold"]

    def test_parse_strategy_file_uses_module_parameters_when_class_missing(self):
        content = """
parameters = {"window": 5, "threshold": 2}
threshold = 9

class DemoStrategy:
    window = 7
"""
        result = parse_strategy_file(content)

        assert result["classes"][0]["defaults"] == {"window": 5, "threshold": 2}
        assert result["classes"][0]["parameter_order"] == ["window", "threshold"]

    def test_parse_strategy_file_falls_back_to_all_defaults_without_parameters_list(self):
        content = """
GLOBAL_LIMIT = 8

class DemoStrategy:
    alpha = 1

    def setup(self):
        self.beta = 2
"""
        result = parse_strategy_file(content)

        assert result["classes"][0]["defaults"] == {"alpha": 1, "GLOBAL_LIMIT": 8, "beta": 2}
        assert result["classes"][0]["parameter_order"] == ["alpha", "beta", "GLOBAL_LIMIT"]

    def test_parse_strategy_file_returns_empty_on_syntax_error(self):
        assert parse_strategy_file("class Broken(:") == {"classes": []}

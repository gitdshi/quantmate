"""Test configuration loading and validation."""

import os

import pytest
from pydantic import ValidationError

from app.infrastructure.config.config import Settings


def test_missing_required_env_vars():
    """Test that missing required environment variables raise ValidationError."""
    env_vars_to_clear = ["SECRET_KEY", "MYSQL_PASSWORD"]
    original_env = {}
    for var in env_vars_to_clear:
        original_env[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]

    try:
        # Should raise ValidationError because required fields are missing
        with pytest.raises(ValidationError):
            Settings(_env_file=None)
    finally:
        # Restore original environment
        for var, value in original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]


def test_valid_env_loading():
    """Test that settings load correctly with proper environment variables."""
    test_env = {
        "SECRET_KEY": "test-secret-key-1234567890",
        "MYSQL_PASSWORD": "test-mysql-password",
        "MYSQL_HOST": "127.0.0.1",
        "MYSQL_USER": "root",
        "MYSQL_PORT": "3306",
    }
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        settings = Settings(_env_file=None)

        assert settings.secret_key == "test-secret-key-1234567890"
        assert settings.mysql_password == "test-mysql-password"
        assert settings.mysql_host == "127.0.0.1"
        assert settings.mysql_port == 3306
        assert settings.mysql_user == "root"
        assert settings.algorithm == "HS256"

        expected_mysql_url = "mysql+pymysql://root:test-mysql-password@127.0.0.1:3306"
        assert settings.mysql_url == expected_mysql_url
    finally:
        for key in test_env:
            if key in original_env:
                if original_env[key] is not None:
                    os.environ[key] = original_env[key]
                else:
                    os.environ.pop(key, None)
            elif key in os.environ:
                del os.environ[key]


def test_env_alias_loading():
    """Test that staging-style environment variable aliases are honored."""
    test_env = {
        "SECRET_KEY": "alias-secret-key-1234567890",
        "MYSQL_PASSWORD": "alias-mysql-password",
        "APP_ENV": "staging",
        "TUSHARE_DATABASE": "tushare_alias",
    }
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        settings = Settings(_env_file=None)

        assert settings.environment == "staging"
        assert settings.tushare_db == "tushare_alias"
        assert settings.tushare_db_url.endswith("/tushare_alias?charset=utf8mb4")
    finally:
        for key in test_env:
            if original_env[key] is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_env[key]


def test_no_hardcoded_defaults():
    """Verify that sensitive fields have no default values."""
    from pydantic_core import PydanticUndefined

    sensitive_fields = ["secret_key", "mysql_password"]
    for field_name in sensitive_fields:
        field = Settings.model_fields.get(field_name)
        assert field is not None, f"{field_name} not found in model_fields"
        default = field.default
        assert default is PydanticUndefined, f"{field_name} has a default value: {default}"


def test_worker_default_queue_names_accepts_csv():
    """Test that queue names can be provided as a comma-separated env var."""
    test_env = {
        "SECRET_KEY": "csv-secret-key-1234567890",
        "MYSQL_PASSWORD": "csv-mysql-password",
        "WORKER_DEFAULT_QUEUE_NAMES": "backtest, optimization, default , low, rdagent",
    }
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        settings = Settings(_env_file=None)

        assert settings.worker_default_queue_names == ["backtest", "optimization", "default", "low", "rdagent"]
    finally:
        for key in test_env:
            if original_env[key] is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_env[key]


def test_worker_default_queue_names_accepts_json():
    """Test that queue names still accept JSON list input."""
    test_env = {
        "SECRET_KEY": "json-secret-key-1234567890",
        "MYSQL_PASSWORD": "json-mysql-password",
        "WORKER_DEFAULT_QUEUE_NAMES": '["backtest", "optimization", "default", "low", "rdagent"]',
    }
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        settings = Settings(_env_file=None)

        assert settings.worker_default_queue_names == ["backtest", "optimization", "default", "low", "rdagent"]
    finally:
        for key in test_env:
            if original_env[key] is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_env[key]


def test_worker_default_queue_names_accepts_bracketed_csv():
    """Test that queue names tolerate bracketed non-JSON values from legacy env files."""
    test_env = {
        "SECRET_KEY": "legacy-secret-key-1234567890",
        "MYSQL_PASSWORD": "legacy-mysql-password",
        "WORKER_DEFAULT_QUEUE_NAMES": "[backtest,optimization,default,low,rdagent]",
    }
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        settings = Settings(_env_file=None)

        assert settings.worker_default_queue_names == ["backtest", "optimization", "default", "low", "rdagent"]
    finally:
        for key in test_env:
            if original_env[key] is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_env[key]
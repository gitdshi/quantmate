"""Test configuration loading and validation."""

import os
import pytest
from pydantic import ValidationError
from app.infrastructure.config.config import Settings


def test_missing_required_env_vars():
    """Test that missing required environment variables raise ValidationError."""
    env_vars_to_clear = ['SECRET_KEY', 'MYSQL_PASSWORD']
    original_env = {}
    for var in env_vars_to_clear:
        original_env[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]
    
    try:
        # Should raise ValidationError because required fields are missing
        with pytest.raises(ValidationError):
            settings = Settings(_env_file=None)
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
        'SECRET_KEY': 'test-secret-key-1234567890',
        'MYSQL_PASSWORD': 'test-mysql-password',
        'MYSQL_HOST': '127.0.0.1',
        'MYSQL_USER': 'root',
        'MYSQL_PORT': '3306',
    }
    original_env = {}
    for k, v in test_env.items():
        original_env[k] = os.environ.get(k)
        os.environ[k] = v
    
    try:
        settings = Settings(_env_file=None)
        
        assert settings.secret_key == 'test-secret-key-1234567890'
        assert settings.mysql_password == 'test-mysql-password'
        assert settings.mysql_host == '127.0.0.1'
        assert settings.mysql_port == 3306
        assert settings.mysql_user == 'root'
        assert settings.algorithm == 'HS256'
        
        expected_mysql_url = 'mysql+pymysql://root:test-mysql-password@127.0.0.1:3306'
        assert settings.mysql_url == expected_mysql_url
    finally:
        for k in test_env:
            if k in original_env:
                os.environ[k] = original_env[k] if original_env[k] is not None else None
            elif k in os.environ:
                del os.environ[k]


def test_no_hardcoded_defaults():
    """Verify that sensitive fields have no default values."""
    from pydantic_core import PydanticUndefined
    
    sensitive_fields = ['secret_key', 'mysql_password']
    for field_name in sensitive_fields:
        field = Settings.model_fields.get(field_name)
        assert field is not None, f"{field_name} not found in model_fields"
        default = field.default
        assert default is PydanticUndefined, f"{field_name} has a default value: {default}"

from app.infrastructure.config.runtime import clear_runtime_config_cache, resolve_runtime_config_value


def test_db_managed_runtime_key_prefers_database(monkeypatch):
    clear_runtime_config_cache()
    monkeypatch.setenv("SYNC_HOUR", "7")
    monkeypatch.setattr(
        "app.infrastructure.config.runtime._load_system_config_value",
        lambda key: "5" if key == "datasync.sync_hour" else None,
    )

    value, source = resolve_runtime_config_value(
        env_keys="SYNC_HOUR",
        db_key="datasync.sync_hour",
        default="2",
    )

    assert value == "5"
    assert source == "db"


def test_db_managed_runtime_key_falls_back_to_legacy_env(monkeypatch):
    clear_runtime_config_cache()
    monkeypatch.setenv("SYNC_HOUR", "7")
    monkeypatch.setattr("app.infrastructure.config.runtime._load_system_config_value", lambda key: None)

    value, source = resolve_runtime_config_value(
        env_keys="SYNC_HOUR",
        db_key="datasync.sync_hour",
        default="2",
    )

    assert value == "7"
    assert source == "legacy_env"


def test_env_only_runtime_key_ignores_database(monkeypatch):
    clear_runtime_config_cache()
    monkeypatch.setenv("API_PORT", "9000")
    monkeypatch.setattr(
        "app.infrastructure.config.runtime._load_system_config_value",
        lambda key: "7000" if key == "api.port" else None,
    )

    value, source = resolve_runtime_config_value(
        env_keys="API_PORT",
        db_key="api.port",
        default="8000",
    )

    assert value == "9000"
    assert source == "env"

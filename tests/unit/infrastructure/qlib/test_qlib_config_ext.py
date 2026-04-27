from app.infrastructure.qlib import qlib_config


class TestQlibConfigExt:
    def test_is_qlib_available_handles_import_error(self, monkeypatch):
        original_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "qlib":
                raise ImportError("missing")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import)
        assert qlib_config.is_qlib_available() is False

    def test_init_qlib_returns_false_on_import_error_and_exception(self, monkeypatch):
        qlib_config._qlib_initialized = False
        original_import = __import__

        def fake_import_missing(name, *args, **kwargs):
            if name == "qlib":
                raise ImportError("missing")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import_missing)
        assert qlib_config.init_qlib() is False

        class FakeQlib:
            @staticmethod
            def init(**kwargs):
                raise RuntimeError("boom")

        class FakeConfig:
            REG_CN = "CN"
            REG_US = "US"

        def fake_import_runtime(name, *args, **kwargs):
            if name == "qlib":
                return FakeQlib
            if name == "qlib.config":
                return FakeConfig
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import_runtime)
        qlib_config._qlib_initialized = False
        assert qlib_config.init_qlib() is False

    def test_init_qlib_success_and_idempotent(self, monkeypatch):
        qlib_config._qlib_initialized = False
        calls = []
        original_import = __import__

        class FakeQlib:
            @staticmethod
            def init(**kwargs):
                calls.append(kwargs)

        class FakeConfig:
            REG_CN = "CN"
            REG_US = "US"

        def fake_import(name, *args, **kwargs):
            if name == "qlib":
                return FakeQlib
            if name == "qlib.config":
                return FakeConfig
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import)

        assert qlib_config.init_qlib(data_dir="/tmp/qlib", region="us") is True
        assert calls == [{"provider_uri": "/tmp/qlib", "region_config": "US"}]
        assert qlib_config.init_qlib() is True
        assert len(calls) == 1

    def test_ensure_qlib_initialized_raises_when_init_fails(self, monkeypatch):
        qlib_config._qlib_initialized = False
        monkeypatch.setattr(qlib_config, "init_qlib", lambda *args, **kwargs: False)

        try:
            qlib_config.ensure_qlib_initialized()
        except RuntimeError as exc:
            assert "not initialized" in str(exc)
        else:
            raise AssertionError("expected RuntimeError")

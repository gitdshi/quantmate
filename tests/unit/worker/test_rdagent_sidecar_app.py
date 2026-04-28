from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pandas as pd


def _load_sidecar_module():
    module_path = Path(__file__).resolve().parents[3] / "rdagent-service" / "app.py"
    spec = importlib.util.spec_from_file_location("rdagent_sidecar_app", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_rdagent_command_uses_cli_hyphenated_step_flag():
    module = _load_sidecar_module()

    command = module._build_rdagent_command("fin_factor", 3)

    assert command == ["rdagent", "fin_factor", "--step-n", "3"]


def test_build_rdagent_command_keeps_fin_quant_subcommand():
    module = _load_sidecar_module()

    command = module._build_rdagent_command("fin_quant", 2)

    assert command == ["rdagent", "fin_quant", "--step-n", "2"]


def test_build_rdagent_command_falls_back_to_fin_factor():
    module = _load_sidecar_module()

    command = module._build_rdagent_command("unknown", 1)

    assert command == ["rdagent", "fin_factor", "--step-n", "1"]


def test_build_rdagent_env_prefers_local_ollama_without_openai_key(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")

    assert env["CHAT_MODEL"] == "ollama/mistral:7b"
    assert env["EMBEDDING_MODEL"] == "ollama/nomic-embed-text:latest"
    assert env["CONDA_DEFAULT_ENV"] == "base"
    assert env["FACTOR_CoSTEER_python_bin"] == sys.executable


def test_build_rdagent_env_preserves_openai_model_when_key_present(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("OLLAMA_API_BASE", raising=False)

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")

    assert env["CHAT_MODEL"] == "gpt-4o-mini"
    assert "EMBEDDING_MODEL" not in env or env["EMBEDDING_MODEL"] != "ollama/nomic-embed-text:latest"


def test_build_seed_factor_frame_shapes():
    module = _load_sidecar_module()

    full_df = module._build_seed_factor_frame(False)
    debug_df = module._build_seed_factor_frame(True)

    assert list(full_df.index.names) == ["datetime", "instrument"]
    assert "$close" in full_df.columns
    assert debug_df.index.get_level_values("instrument").nunique() == 1


def test_seed_factor_prompt_data_creates_expected_assets(tmp_path, monkeypatch):
    module = _load_sidecar_module()

    written_paths = []

    def fake_to_hdf(self, path, key="data", mode="w"):
        path = Path(path)
        path.write_text("seeded", encoding="utf-8")
        written_paths.append(path)

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")
    monkeypatch.setattr(pd.DataFrame, "to_hdf", fake_to_hdf, raising=False)
    module._seed_factor_prompt_data(tmp_path, env)

    data_path = tmp_path / "git_ignore_folder" / "factor_implementation_source_data" / "daily_pv.h5"
    debug_path = tmp_path / "git_ignore_folder" / "factor_implementation_source_data_debug" / "daily_pv.h5"
    readme_path = tmp_path / "git_ignore_folder" / "factor_implementation_source_data" / "README.md"

    assert data_path.exists()
    assert debug_path.exists()
    assert readme_path.exists()
    assert written_paths == [data_path, debug_path]
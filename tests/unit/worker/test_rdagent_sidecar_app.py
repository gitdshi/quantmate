from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest.mock import MagicMock

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
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_KEY", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_BASE", raising=False)
    monkeypatch.setenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")

    assert env["CHAT_MODEL"] == "ollama/qwen2.5:0.5b"
    assert env["EMBEDDING_MODEL"] == "ollama/nomic-embed-text:latest"
    assert env["LITELLM_CHAT_STREAM"] == "false"
    assert env["LITELLM_ENABLE_RESPONSE_SCHEMA"] == "false"
    assert env["CONDA_DEFAULT_ENV"] == "base"
    assert env["FACTOR_CoSTEER_python_bin"] == sys.executable
    assert env["BACKEND"] == "rdagent.oai.backend.LiteLLMAPIBackend"


def test_build_rdagent_env_maps_opencode_key_to_openai(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OLLAMA_API_BASE", raising=False)
    monkeypatch.setenv("OPENCODE_AI_API_KEY", "opencode-key")

    env = module._build_rdagent_env(tmp_path, "")

    assert env["OPENAI_API_KEY"] == "opencode-key"
    assert env["OPENAI_API_BASE"] == "https://opencode.ai/zen/v1"
    assert env["OPENAI_BASE_URL"] == "https://opencode.ai/zen/v1"
    assert env["LITELLM_OPENAI_API_KEY"] == "opencode-key"
    assert env["LITELLM_CHAT_OPENAI_API_KEY"] == "opencode-key"
    assert env["LITELLM_CHAT_OPENAI_BASE_URL"] == "https://opencode.ai/zen/v1"
    assert env["LITELLM_EMBEDDING_OPENAI_API_KEY"] == "opencode-key"
    assert env["LITELLM_EMBEDDING_OPENAI_BASE_URL"] == "https://opencode.ai/zen/v1"
    assert env["CHAT_MODEL"] == "openai/minimax-m2.5-free"


def test_build_rdagent_env_prefixes_requested_opencode_model_for_litellm(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OLLAMA_API_BASE", raising=False)
    monkeypatch.setenv("OPENCODE_AI_API_KEY", "opencode-key")

    env = module._build_rdagent_env(tmp_path, "minimax-m2.5-free")

    assert env["CHAT_MODEL"] == "openai/minimax-m2.5-free"


def test_build_rdagent_env_drops_empty_openai_vars(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setenv("OPENAI_API_BASE", "")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_KEY", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_BASE", raising=False)
    monkeypatch.setenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")

    env = module._build_rdagent_env(tmp_path, "")

    assert env["CHAT_MODEL"] == "ollama/qwen2.5:0.5b"
    assert "OPENAI_API_KEY" not in env
    assert "OPENAI_API_BASE" not in env


def test_build_rdagent_env_preserves_openai_model_when_key_present(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_KEY", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_BASE", raising=False)
    monkeypatch.delenv("OLLAMA_API_BASE", raising=False)

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")

    assert env["CHAT_MODEL"] == "gpt-4o-mini"
    assert "EMBEDDING_MODEL" not in env or env["EMBEDDING_MODEL"] != "ollama/nomic-embed-text:latest"


def test_sidecar_uses_longer_default_timeout_when_env_missing(monkeypatch):
    monkeypatch.delenv("RDAGENT_TIMEOUT_PER_ITERATION_SECONDS", raising=False)

    module = _load_sidecar_module()

    assert module._TIMEOUT_PER_ITERATION_SECONDS == 1800


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


def test_cancel_endpoint_terminates_running_process(monkeypatch):
    module = _load_sidecar_module()

    process = MagicMock()
    process.poll.return_value = None
    module._RUNNING_PROCESSES.clear()
    module._RUNNING_PROCESSES["run-1"] = process

    terminated = []
    monkeypatch.setattr(module, "_terminate_process", lambda proc: terminated.append(proc))

    client = module.app.test_client()
    response = client.post("/runs/run-1/cancel")

    assert response.status_code == 200
    assert response.get_json() == {"run_id": "run-1", "status": "cancelled"}
    assert terminated == [process]


def test_parse_discovered_factors_falls_back_to_selector_log(tmp_path):
    module = _load_sidecar_module()

    (tmp_path / "selector.log").write_text(
        """
factor_name: volatility_20d
factor_description: 20-day rolling volatility
factor_formulation: std(return_1d, 20)

factor_name: volume_ratio_20d
factor_description: 20-day volume ratio
factor_formulation: volume / mean(volume, 20)
""".strip(),
        encoding="utf-8",
    )

    factors = module._parse_discovered_factors(tmp_path)

    assert factors == [
        {
            "name": "volatility_20d",
            "expression": "std(return_1d, 20)",
            "description": "20-day rolling volatility",
        },
        {
            "name": "volume_ratio_20d",
            "expression": "volume / mean(volume, 20)",
            "description": "20-day volume ratio",
        },
    ]


def test_parse_iterations_synthesizes_iteration_from_selector_log_and_factor_code(tmp_path):
    module = _load_sidecar_module()

    selector_log = tmp_path / "selector.log"
    selector_log.write_text(
        """
factor_name: volatility_20d
factor_description: 20-day rolling volatility
factor_formulation: std(return_1d, 20)
{"final_feedback": "The factor implementation now runs successfully."}
""".strip(),
        encoding="utf-8",
    )

    factor_dir = tmp_path / "git_ignore_folder" / "RD-Agent_workspace" / "abc123"
    factor_dir.mkdir(parents=True)
    (factor_dir / "factor.py").write_text(
        "def calculate_volatility_20d(df):\n    return df\n",
        encoding="utf-8",
    )

    iterations = module._parse_iterations(tmp_path, max_iters=1)

    assert len(iterations) == 1
    assert iterations[0]["iteration"] == 1
    assert iterations[0]["hypothesis"] == "Generated factors: volatility_20d"
    assert iterations[0]["metrics"] == {"generated_factor_count": 1}
    assert "runs successfully" in iterations[0]["feedback"]
    assert "calculate_volatility_20d" in iterations[0]["code"]
from __future__ import annotations

import importlib
from pathlib import Path
import signal
import subprocess
import sys
from unittest.mock import MagicMock

import pandas as pd


def _load_sidecar_module():
    module = importlib.import_module("app.rdagent.service")
    return importlib.reload(module)


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


def test_redact_sensitive_error_text_masks_api_keys():
    module = _load_sidecar_module()

    sanitized = module._redact_sensitive_error_text(
        "openai_api_key='sk-secret-token' Authorization: Bearer sk-another-secret"
    )

    assert sanitized is not None
    assert "sk-secret-token" not in sanitized
    assert "sk-another-secret" not in sanitized
    assert "[REDACTED]" in sanitized


def test_summarize_process_error_prefers_tail_and_redacts_secrets():
    module = _load_sidecar_module()

    stderr = "\n".join(
        [
            "2026-05-12 06:53:01 INFO init openai_api_key='sk-secret-token'",
            "2026-05-12 06:53:02 INFO token count: 370",
            "2026-05-12 06:53:03 ERROR upstream returned 401 unauthorized",
        ]
    )

    summary = module._summarize_process_error(stderr)

    assert summary is not None
    assert "401 unauthorized" in summary
    assert "sk-secret-token" not in summary


def test_build_rdagent_env_prefers_local_ollama_without_openai_key(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_KEY", raising=False)
    monkeypatch.delenv("OPENCODE_AI_API_BASE", raising=False)
    monkeypatch.setenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")
    support_dir = module._sitecustomize_support_dir(tmp_path)

    assert env["CHAT_MODEL"] == "ollama/qwen2.5:0.5b"
    assert env["EMBEDDING_MODEL"] == "ollama/nomic-embed-text:latest"
    assert env["LITELLM_CHAT_STREAM"] == "false"
    assert env["LITELLM_ENABLE_RESPONSE_SCHEMA"] == "false"
    assert env["CONDA_DEFAULT_ENV"] == "base"
    assert env["PYTHONPATH"].split(":", 2)[:2] == [str(support_dir), str(tmp_path)]
    assert env["FACTOR_CoSTEER_python_bin"] == sys.executable
    assert env["BACKEND"] == "rdagent.oai.backend.LiteLLMAPIBackend"


def test_build_rdagent_env_maps_opencode_key_to_openai(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.setenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")
    monkeypatch.setenv("OPENCODE_AI_API_KEY", "opencode-key")

    env = module._build_rdagent_env(tmp_path, "")

    assert env["OPENAI_API_KEY"] == "opencode-key"
    assert env["OPENAI_API_BASE"] == "https://opencode.ai/zen/v1"
    assert env["OPENAI_BASE_URL"] == "https://opencode.ai/zen/v1"
    assert env["LITELLM_OPENAI_API_KEY"] == "opencode-key"
    assert env["LITELLM_CHAT_OPENAI_API_KEY"] == "opencode-key"
    assert env["LITELLM_CHAT_OPENAI_BASE_URL"] == "https://opencode.ai/zen/v1"
    assert env["EMBEDDING_MODEL"] == "ollama/nomic-embed-text:latest"
    assert "LITELLM_EMBEDDING_OPENAI_API_KEY" not in env
    assert env["LITELLM_EMBEDDING_OPENAI_BASE_URL"] == "http://127.0.0.1:11434"
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
    monkeypatch.setenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")

    assert env["CHAT_MODEL"] == "gpt-4o-mini"
    assert env["EMBEDDING_MODEL"] == "ollama/nomic-embed-text:latest"
    assert env["LITELLM_EMBEDDING_OPENAI_BASE_URL"] == "http://127.0.0.1:11434"
    assert "LITELLM_EMBEDDING_OPENAI_API_KEY" not in env


def test_build_rdagent_env_explicit_ollama_request_drops_openai_provider_vars(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.setenv("OPENAI_API_KEY", "openai-key")
    monkeypatch.setenv("OPENAI_API_BASE", "https://example.com/v1")
    monkeypatch.setenv("OPENCODE_AI_API_KEY", "opencode-key")
    monkeypatch.setenv("OPENCODE_AI_API_BASE", "https://opencode.ai/zen/v1")
    monkeypatch.setenv("OLLAMA_API_BASE", "http://127.0.0.1:11434")

    env = module._build_rdagent_env(tmp_path, "ollama/qwen2.5:0.5b")

    assert env["CHAT_MODEL"] == "ollama/qwen2.5:0.5b"
    assert env["EMBEDDING_MODEL"] == "ollama/nomic-embed-text:latest"
    assert "OPENAI_API_KEY" not in env
    assert "OPENAI_API_BASE" not in env
    assert "OPENCODE_AI_API_KEY" not in env
    assert "LITELLM_OPENAI_API_KEY" not in env


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
        written_paths.append((path, key, mode))

    env = module._build_rdagent_env(tmp_path, "gpt-4o-mini")
    monkeypatch.setattr(pd.DataFrame, "to_hdf", fake_to_hdf, raising=False)
    module._seed_factor_prompt_data(tmp_path, env)

    data_path = tmp_path / "git_ignore_folder" / "factor_implementation_source_data" / "daily_pv.h5"
    debug_path = tmp_path / "git_ignore_folder" / "factor_implementation_source_data_debug" / "daily_pv.h5"
    readme_path = tmp_path / "git_ignore_folder" / "factor_implementation_source_data" / "README.md"
    support_sitecustomize_path = module._sitecustomize_support_dir(tmp_path) / "sitecustomize.py"

    assert data_path.exists()
    assert debug_path.exists()
    assert readme_path.exists()
    assert written_paths == [
        (data_path, "data", "w"),
        (data_path, "df", "a"),
        (debug_path, "data", "w"),
        (debug_path, "df", "a"),
    ]
    readme_text = readme_path.read_text(encoding="utf-8")
    assert "Do not import or use h5py" in readme_text
    assert "pd.read_hdf('daily_pv.h5', key='data')" in readme_text
    assert (tmp_path / "sitecustomize.py").exists()
    assert support_sitecustomize_path.exists()


def test_sitecustomize_falls_back_to_default_hdf_key(tmp_path, monkeypatch):
    module = _load_sidecar_module()

    index = pd.MultiIndex.from_product(
        [pd.to_datetime(["2024-01-02"]), ["SH600000"]],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({"$close": [10.2]}, index=index)
    data_path = tmp_path / "daily_pv.h5"

    def fake_read_hdf(path_or_buf, key=None, *args, **kwargs):
        if Path(path_or_buf).name != "daily_pv.h5":
            raise AssertionError("unexpected path")
        if key is None:
            raise ValueError("key must be provided when HDF5 file contains multiple datasets.")
        if key == "data":
            return frame
        raise KeyError(key)

    original_read_hdf = pd.read_hdf
    try:
        monkeypatch.setattr(pd, "read_hdf", fake_read_hdf)
        exec(module._build_sitecustomize_content(), {})
        reloaded = pd.read_hdf(data_path)
    finally:
        monkeypatch.setattr(pd, "read_hdf", original_read_hdf)

    pd.testing.assert_frame_equal(reloaded, frame)


def test_build_prompt_context_appends_factor_io_guidance():
    module = _load_sidecar_module()

    prompt = module._build_prompt_context("Existing prompt context")

    assert "Existing prompt context" in prompt
    assert "Use pandas only for HDF5 IO" in prompt
    assert "volume_ratio_20d" in prompt


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


def test_mine_retries_with_ollama_after_rate_limit(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    module.WORKSPACE = tmp_path
    initial_env = {
        "CHAT_MODEL": "openai/minimax-m2.5-free",
        "OLLAMA_API_BASE": "http://127.0.0.1:11434",
        "RDAGENT_OLLAMA_CHAT_MODEL": "ollama/qwen2.5:0.5b",
    }
    fallback_env = {
        "CHAT_MODEL": "ollama/qwen2.5:0.5b",
        "OLLAMA_API_BASE": "http://127.0.0.1:11434",
        "RDAGENT_OLLAMA_CHAT_MODEL": "ollama/qwen2.5:0.5b",
    }
    build_env = MagicMock(side_effect=[initial_env, fallback_env])
    monkeypatch.setattr(module, "_build_rdagent_env", build_env)
    monkeypatch.setattr(module, "_seed_factor_prompt_data", lambda run_dir, env: None)
    monkeypatch.setattr(module, "_parse_iterations", lambda run_dir, max_iters: [{"iteration": 1, "status": "completed"}])
    monkeypatch.setattr(module, "_parse_discovered_factors", lambda run_dir: [{"name": "f1", "expression": "close"}])

    rate_limited = MagicMock()
    rate_limited.communicate.return_value = ("", "litellm.RateLimitError: Rate limit exceeded")
    rate_limited.returncode = 1

    recovered = MagicMock()
    recovered.communicate.return_value = ("", "")
    recovered.returncode = 0

    monkeypatch.setattr(module.subprocess, "Popen", MagicMock(side_effect=[rate_limited, recovered]))

    client = module.app.test_client()
    response = client.post(
        "/mine",
        json={
            "run_id": "run-1",
            "config": {
                "scenario": "fin_factor",
                "max_iterations": 1,
                "llm_model": "minimax-m2.5-free",
            },
            "prompt_context": "factor context",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "completed"
    assert len(payload["discovered_factors"]) == 1
    assert build_env.call_count == 2
    assert build_env.call_args_list[1].args[1] == "ollama/qwen2.5:0.5b"


def test_mine_retries_with_ollama_when_selector_log_has_rate_limit(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    module.WORKSPACE = tmp_path
    initial_env = {
        "CHAT_MODEL": "openai/minimax-m2.5-free",
        "OLLAMA_API_BASE": "http://127.0.0.1:11434",
        "RDAGENT_OLLAMA_CHAT_MODEL": "ollama/qwen2.5:0.5b",
    }
    fallback_env = {
        "CHAT_MODEL": "ollama/qwen2.5:0.5b",
        "OLLAMA_API_BASE": "http://127.0.0.1:11434",
        "RDAGENT_OLLAMA_CHAT_MODEL": "ollama/qwen2.5:0.5b",
    }
    build_env = MagicMock(side_effect=[initial_env, fallback_env])
    monkeypatch.setattr(module, "_build_rdagent_env", build_env)
    monkeypatch.setattr(module, "_seed_factor_prompt_data", lambda run_dir, env: None)
    monkeypatch.setattr(module, "_parse_iterations", lambda run_dir, max_iters: [{"iteration": 1, "status": "completed"}])
    monkeypatch.setattr(module, "_parse_discovered_factors", lambda run_dir: [{"name": "f1", "expression": "close"}])

    calls = []

    def fake_run_process(run_id, run_dir, scenario, max_iterations, env):
        calls.append(env["CHAT_MODEL"])
        if len(calls) == 1:
            (run_dir / "selector.log").write_text(
                "litellm.RateLimitError: RateLimitError: OpenAIException - Rate limit exceeded. Please try again later.",
                encoding="utf-8",
            )
            return 1, "RuntimeError: Failed to create chat completion after 10 retries."
        return 0, None

    monkeypatch.setattr(module, "_run_rdagent_process", fake_run_process)

    client = module.app.test_client()
    response = client.post(
        "/mine",
        json={
            "run_id": "run-1",
            "config": {
                "scenario": "fin_factor",
                "max_iterations": 1,
                "llm_model": "minimax-m2.5-free",
            },
            "prompt_context": "factor context",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "completed"
    assert calls == ["openai/minimax-m2.5-free", "ollama/qwen2.5:0.5b"]
    assert build_env.call_count == 2


def test_run_rdagent_process_stalls_without_workspace_activity(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    monkeypatch.setattr(module, "_IDLE_TIMEOUT_SECONDS", 1)
    monotonic_values = iter([0.0, 0.0, 0.0, 2.0])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module, "_workspace_activity_marker", lambda run_dir: (0, 0))

    terminated = []

    class FakeProcess:
        def __init__(self):
            self.pid = 123
            self.args = ["rdagent", "fin_factor", "--step-n", "1"]
            self.returncode = None
            self._communicate_calls = 0

        def communicate(self, timeout=None):
            self._communicate_calls += 1
            if self._communicate_calls == 1:
                raise subprocess.TimeoutExpired(self.args, timeout)
            self.returncode = -signal.SIGTERM
            return "", ""

    process = FakeProcess()
    monkeypatch.setattr(module.subprocess, "Popen", MagicMock(return_value=process))

    def fake_terminate(proc):
        terminated.append(proc.pid)
        proc.returncode = -signal.SIGTERM

    monkeypatch.setattr(module, "_terminate_process", fake_terminate)

    returncode, error_summary = module._run_rdagent_process(
        "run-1",
        tmp_path,
        "fin_factor",
        1,
        {"CHAT_MODEL": "openai/minimax-m2.5-free"},
    )

    assert returncode == 124
    assert "stalled waiting for activity" in error_summary
    assert terminated == [123]


def test_mine_retries_with_ollama_after_stalled_primary_run(monkeypatch, tmp_path):
    module = _load_sidecar_module()

    module.WORKSPACE = tmp_path
    initial_env = {
        "CHAT_MODEL": "openai/minimax-m2.5-free",
        "OLLAMA_API_BASE": "http://127.0.0.1:11434",
        "RDAGENT_OLLAMA_CHAT_MODEL": "ollama/qwen2.5:0.5b",
    }
    fallback_env = {
        "CHAT_MODEL": "ollama/qwen2.5:0.5b",
        "OLLAMA_API_BASE": "http://127.0.0.1:11434",
        "RDAGENT_OLLAMA_CHAT_MODEL": "ollama/qwen2.5:0.5b",
    }
    build_env = MagicMock(side_effect=[initial_env, fallback_env])
    monkeypatch.setattr(module, "_build_rdagent_env", build_env)
    monkeypatch.setattr(module, "_seed_factor_prompt_data", lambda run_dir, env: None)
    monkeypatch.setattr(module, "_parse_iterations", lambda run_dir, max_iters: [{"iteration": 1, "status": "completed"}])
    monkeypatch.setattr(module, "_parse_discovered_factors", lambda run_dir: [{"name": "f1", "expression": "close"}])

    calls = []

    def fake_run_process(run_id, run_dir, scenario, max_iterations, env):
        calls.append(env["CHAT_MODEL"])
        if len(calls) == 1:
            return 124, "RD-Agent process stalled waiting for activity for 300 seconds"
        return 0, None

    monkeypatch.setattr(module, "_run_rdagent_process", fake_run_process)

    client = module.app.test_client()
    response = client.post(
        "/mine",
        json={
            "run_id": "run-1",
            "config": {
                "scenario": "fin_factor",
                "max_iterations": 1,
                "llm_model": "minimax-m2.5-free",
            },
            "prompt_context": "factor context",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "completed"
    assert calls == ["openai/minimax-m2.5-free", "ollama/qwen2.5:0.5b"]
    assert build_env.call_count == 2


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
                        "ic_mean": 0.0,
                        "icir": 0.0,
                        "sharpe": 0.0,
        },
        {
            "name": "volume_ratio_20d",
            "expression": "volume / mean(volume, 20)",
            "description": "20-day volume ratio",
                        "ic_mean": 0.0,
                        "icir": 0.0,
                        "sharpe": 0.0,
        },
    ]


def test_parse_discovered_factors_normalizes_nested_metrics_from_results_json(tmp_path):
        module = _load_sidecar_module()

        results_dir = tmp_path / "results"
        results_dir.mkdir()
        (results_dir / "factors.json").write_text(
                """
{
    "factors": [
        {
            "factor_name": "volume_ratio_20d",
            "factor_formulation": "volume / mean(volume, 20)",
            "factor_description": "20-day volume ratio",
            "evaluation": {
                "metrics": {
                    "ic_mean": "0.1234",
                    "ic_ir": 1.234,
                    "sharpe_ratio": "0.88"
                }
            }
        }
    ]
}
""".strip(),
                encoding="utf-8",
        )

        factors = module._parse_discovered_factors(tmp_path)

        assert factors == [
                {
                        "name": "volume_ratio_20d",
                        "expression": "volume / mean(volume, 20)",
                        "description": "20-day volume ratio",
                        "ic_mean": 0.1234,
                        "icir": 1.234,
                        "sharpe": 0.88,
                }
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


def test_parse_discovered_factors_reads_embedded_log_blocks(tmp_path):
    module = _load_sidecar_module()

    (tmp_path / "selector.log").write_text(
        """
2026-05-11 12:05:43.724 | INFO | embed - Creating embedding for: ["factor_name: daily_return\nfactor_description: [Momentum Factor] Daily return from open to close\nfactor_formulation: (close - open) / open\nvariables: {'open': 'Opening price', 'close': 'Closing price'}"]
2026-05-11 12:05:43.800 | INFO | embed - Creating embedding for: ["factor_name: intraday_volatility\nfactor_description: [Volatility Factor] Intraday range scaled by open\nfactor_formulation: (high - low) / open\nvariables: {'high': 'High price', 'low': 'Low price', 'open': 'Opening price'}"]
""".strip(),
        encoding="utf-8",
    )

    factors = module._parse_discovered_factors(tmp_path)

    assert factors == [
        {
            "name": "daily_return",
            "expression": "(close - open) / open",
            "description": "[Momentum Factor] Daily return from open to close",
            "ic_mean": 0.0,
            "icir": 0.0,
            "sharpe": 0.0,
        },
        {
            "name": "intraday_volatility",
            "expression": "(high - low) / open",
            "description": "[Volatility Factor] Intraday range scaled by open",
            "ic_mean": 0.0,
            "icir": 0.0,
            "sharpe": 0.0,
        },
    ]


def test_parse_discovered_factors_reads_past_default_text_limit(tmp_path):
    module = _load_sidecar_module()

    filler = "x" * 12050
    (tmp_path / "selector.log").write_text(
        filler
        + "\n"
        + '2026-05-11 12:42:25.183 | INFO | embed - Creating embedding for: ["factor_name: Volatility_5d\\nfactor_description: [Volatility Factor] 5-day rolling volatility\\nfactor_formulation: rolling_std(return, 5)\\nvariables: {}"]',
        encoding="utf-8",
    )

    factors = module._parse_discovered_factors(tmp_path)

    assert factors == [
        {
            "name": "Volatility_5d",
            "expression": "rolling_std(return, 5)",
            "description": "[Volatility Factor] 5-day rolling volatility",
            "ic_mean": 0.0,
            "icir": 0.0,
            "sharpe": 0.0,
        }
    ]
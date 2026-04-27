from __future__ import annotations

import importlib.util
from pathlib import Path


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
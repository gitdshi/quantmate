"""RD-Agent Sidecar — thin Flask wrapper around rdagent CLI.

Accepts mining requests from QuantMate's RQ worker and executes the
RD-Agent R&D loop. Returns iteration results and discovered factors.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import traceback
from pathlib import Path

from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rdagent-sidecar")

WORKSPACE = Path(os.getenv("RDAGENT_WORKSPACE", "/tmp/rdagent_workspace"))
WORKSPACE.mkdir(parents=True, exist_ok=True)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/mine", methods=["POST"])
def mine():
    """Execute an RD-Agent factor mining run.

    Expected JSON body:
    {
        "run_id": "uuid",
        "config": { "scenario": "fin_factor", "max_iterations": 10, ... },
        "prompt_context": "Available data fields..."
    }
    """
    data = request.get_json(force=True)
    run_id = data.get("run_id", "unknown")
    config = data.get("config", {})
    prompt_context = data.get("prompt_context", "")

    scenario = config.get("scenario", "fin_factor")
    max_iterations = config.get("max_iterations", 10)
    llm_model = config.get("llm_model", "gpt-4o-mini")

    logger.info("Starting mining run %s scenario=%s iters=%d", run_id, scenario, max_iterations)

    run_dir = WORKSPACE / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Write prompt context as an additional data description file
    (run_dir / "data_description.txt").write_text(prompt_context, encoding="utf-8")

    try:
        # Set up environment for rdagent
        env = os.environ.copy()
        env["RDAGENT_WORKSPACE"] = str(run_dir)
        env["CHAT_MODEL"] = llm_model

        # Build rdagent command
        scenario_map = {
            "fin_factor": "rdagent fin_factor",
            "fin_model": "rdagent fin_model",
            "fin_quant": "rdagent fin_factor_combined_with_model",
        }
        cmd = scenario_map.get(scenario, "rdagent fin_factor")
        cmd_parts = cmd.split() + ["--step_n", str(max_iterations)]

        result = subprocess.run(
            cmd_parts,
            cwd=str(run_dir),
            env=env,
            capture_output=True,
            text=True,
            timeout=max_iterations * 300,  # 5 min per iteration max
        )

        logger.info("rdagent exited with code %d", result.returncode)
        if result.returncode != 0:
            logger.warning("rdagent stderr: %s", result.stderr[:2000])

        # Parse results from workspace
        iterations = _parse_iterations(run_dir, max_iterations)
        discovered = _parse_discovered_factors(run_dir)

        return jsonify({
            "status": "completed" if result.returncode == 0 else "failed",
            "iterations": iterations,
            "discovered_factors": discovered,
            "error": result.stderr[:1000] if result.returncode != 0 else None,
        })

    except subprocess.TimeoutExpired:
        logger.error("Mining run %s timed out", run_id)
        return jsonify({"status": "failed", "error": "Mining run timed out", "iterations": [], "discovered_factors": []})
    except Exception as e:
        logger.exception("Mining run %s failed: %s", run_id, e)
        return jsonify({"status": "failed", "error": str(e), "traceback": traceback.format_exc(), "iterations": [], "discovered_factors": []})


def _parse_iterations(run_dir: Path, max_iters: int) -> list[dict]:
    """Parse RD-Agent iteration outputs from the workspace."""
    iterations = []
    for i in range(1, max_iters + 1):
        iter_dir = run_dir / f"iteration_{i}"
        if not iter_dir.exists():
            iter_dir = run_dir / f"round_{i}"
        if not iter_dir.exists():
            continue

        hypothesis_file = iter_dir / "hypothesis.txt"
        code_file = iter_dir / "factor.py"
        metrics_file = iter_dir / "metrics.json"
        feedback_file = iter_dir / "feedback.txt"

        iteration = {
            "iteration": i,
            "hypothesis": _read_text(hypothesis_file),
            "code": _read_text(code_file),
            "metrics": _read_json(metrics_file),
            "feedback": _read_text(feedback_file),
            "status": "completed",
        }
        iterations.append(iteration)

    return iterations


def _parse_discovered_factors(run_dir: Path) -> list[dict]:
    """Parse discovered factors from the final results."""
    factors = []
    results_file = run_dir / "results" / "factors.json"
    if not results_file.exists():
        results_file = run_dir / "final_factors.json"
    if results_file.exists():
        try:
            data = json.loads(results_file.read_text(encoding="utf-8"))
            if isinstance(data, list):
                factors = data
            elif isinstance(data, dict):
                factors = data.get("factors", [])
        except (json.JSONDecodeError, OSError):
            pass
    return factors


def _read_text(path: Path) -> str | None:
    if path.exists():
        try:
            return path.read_text(encoding="utf-8")[:10000]
        except OSError:
            return None
    return None


def _read_json(path: Path) -> dict | None:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
    return None


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8001"))
    app.run(host="0.0.0.0", port=port)

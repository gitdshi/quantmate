from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any

import requests


DEFAULT_BASE_URL = os.getenv("QUANTMATE_API_BASE_URL", "https://test.quantmate.net/api/v1")
DEFAULT_USERNAME = os.getenv("QUANTMATE_API_USERNAME", "admin")
DEFAULT_PASSWORD = os.getenv("QUANTMATE_API_PASSWORD")
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or inspect an RD-Agent run and print factor metrics.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL, including /api/v1.")
    parser.add_argument("--username", default=DEFAULT_USERNAME, help="Username for login when no access token is provided.")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Password for login. Defaults to QUANTMATE_API_PASSWORD.")
    parser.add_argument("--access-token", help="Use an existing access token instead of logging in.")
    parser.add_argument("--run-id", help="Inspect an existing run instead of creating a new one.")
    parser.add_argument("--scenario", default="fin_factor", help="Scenario used when creating a run.")
    parser.add_argument("--max-iterations", type=int, default=1, help="Max iterations for a new run.")
    parser.add_argument("--llm-model", default="minimax-m2.5-free", help="LLM model for a new run.")
    parser.add_argument("--universe", default="csi300", help="Universe for a new run.")
    parser.add_argument("--start-date", default="2024-01-01", help="Start date for a new run.")
    parser.add_argument("--end-date", default="2024-03-31", help="End date for a new run.")
    parser.add_argument("--timeout-seconds", type=int, default=1200, help="Polling timeout in seconds.")
    parser.add_argument("--poll-interval", type=int, default=10, help="Polling interval in seconds.")
    parser.add_argument(
        "--fail-on-zero-metrics",
        action="store_true",
        help="Exit non-zero if any discovered factor has all metrics equal to zero.",
    )
    return parser.parse_args()


def _coerce_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _authenticate(session: requests.Session, args: argparse.Namespace) -> None:
    if args.access_token:
        session.headers.update({"Authorization": f"Bearer {args.access_token}"})
        return

    if not args.password:
        raise SystemExit("Password is required. Pass --password or set QUANTMATE_API_PASSWORD.")

    response = session.post(
        f"{args.base_url}/auth/login",
        json={"username": args.username, "password": args.password},
        timeout=30,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise SystemExit("Login succeeded but no access_token was returned.")
    session.headers.update({"Authorization": f"Bearer {token}"})


def _create_run(session: requests.Session, args: argparse.Namespace) -> str:
    payload = {
        "scenario": args.scenario,
        "max_iterations": args.max_iterations,
        "llm_model": args.llm_model,
        "universe": args.universe,
        "feature_columns": [],
        "start_date": args.start_date,
        "end_date": args.end_date,
    }
    response = session.post(f"{args.base_url}/rdagent/runs", json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    run_id = data.get("run_id") or data.get("id")
    if not run_id:
        raise SystemExit(f"Run creation response did not include run_id/id: {data}")
    return str(run_id)


def _fetch_json(session: requests.Session, url: str) -> Any:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def _poll_run(session: requests.Session, args: argparse.Namespace, run_id: str) -> str:
    deadline = time.time() + args.timeout_seconds
    last_status: str | None = None

    while time.time() < deadline:
        payload = _fetch_json(session, f"{args.base_url}/rdagent/runs/{run_id}")
        status = payload.get("status", "unknown")
        if status != last_status:
            print(f"Status transition: {last_status} -> {status}")
            last_status = status
        if status in TERMINAL_STATUSES:
            return status
        time.sleep(args.poll_interval)

    raise SystemExit(f"Polling timed out after {args.timeout_seconds} seconds for run {run_id}.")


def _summarize_factors(factors: list[dict[str, Any]]) -> bool:
    any_zero_metrics = False
    print(f"Factor count: {len(factors)}")
    for factor in factors:
        name = factor.get("name") or factor.get("factor_name") or "unnamed"
        ic_mean = _coerce_float(factor.get("ic_mean"))
        icir = _coerce_float(factor.get("icir"))
        sharpe = _coerce_float(factor.get("sharpe"))
        expression = factor.get("expression", "")
        print(
            f"Factor: {name} | IC: {ic_mean:.4f} | ICIR: {icir:.4f} | "
            f"Sharpe: {sharpe:.4f} | Expr: {expression}"
        )
        if ic_mean == 0.0 and icir == 0.0 and sharpe == 0.0:
            any_zero_metrics = True
    print(f"Any factors with all zero metrics: {any_zero_metrics}")
    return any_zero_metrics


def main() -> int:
    args = _parse_args()
    session = requests.Session()
    _authenticate(session, args)

    run_id = args.run_id or _create_run(session, args)
    print(f"Run ID: {run_id}")

    final_status = _poll_run(session, args, run_id)
    factors = _fetch_json(session, f"{args.base_url}/rdagent/runs/{run_id}/factors")
    if not isinstance(factors, list):
        raise SystemExit(f"Unexpected factors payload: {factors}")

    print(f"Final status: {final_status}")
    any_zero_metrics = _summarize_factors(factors)

    if final_status != "completed":
        return 1
    if args.fail_on_zero_metrics and any_zero_metrics:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
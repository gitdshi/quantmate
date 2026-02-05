# Backtesting API

Base path: `/api/backtest`

Overview

- Backtests are executed asynchronously via background workers (RQ). Submit jobs and poll for status/results.

Endpoints

- `POST /api/backtest`
  - Description: Submit a single backtest job
  - Auth: Bearer token required
  - Body (example):
    ```json
    {
      "strategy_id": 1,
      "strategy_class": "TripleMAStrategy",
      "symbol": "000001.SZ",
      "start_date": "2020-01-01",
      "end_date": "2023-12-31",
      "initial_capital": 100000.0,
      "parameters": { "fast_window": 5, "slow_window": 20 }
    }
    ```
  - Response: `{ "job_id": "<uuid>", "status": "queued" }`

- `POST /api/backtest/batch`
  - Description: Submit multiple symbols in one job (batch)
  - Body: `{ "symbols": ["000001.SZ", "000002.SZ"], ... }`

- `GET /api/backtest/{job_id}`
  - Description: Get job status and result metadata
  - Response: `{ "job_id": "..", "status": "started|finished|failed", "progress": 0-100, "result_url": "/api/backtest/results/{job_id}" }`

- `GET /api/backtest/history`
  - Description: List historical backtests for the user

- `POST /api/backtest/{job_id}/cancel`
  - Description: Cancel a running backtest job

Notes

- Results are persisted in job storage (Redis + persistent layer). Large result payloads are provided via result URLs or stored in DB.
- For synchronous testing or local dev, a non-RQ path may exist to execute short backtests inline (not recommended for production).

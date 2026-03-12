# TraderMate Test Suite

This directory contains automated tests for the TraderMate backend.

## Structure

```
tests/
├── conftest.py           # Shared pytest fixtures and configuration
├── datasync/            # DataSync service unit tests
│   ├── test_init_progress_resume.py
│   └── test_tushare_ingest_ratelimit.py
├── integration/         # Integration tests (to be expanded)
│   └── __init__.py
├── unit/               # Unit tests for specific modules
│   ├── dao/
│   │   └── test_user_dao.py
│   └── services/
│       └── test_strategy_service.py
└── test_config_security.py  # Security configuration tests
```

## Running Tests

```bash
# Run all tests
pytest

# Run with specific marker
pytest -m unit
pytest -m integration
pytest -m datasync

# Run with coverage report
pytest --cov=app --cov-report=html

# Run a specific test file
pytest tests/datasync/test_tushare_ingest_ratelimit.py -v

# Run tests matching a keyword
pytest -k "ratelimit" -v
```

## Current Status

- **Total tests**: 11 (as of 2026-03-09)
- **Pass rate**: 100% for datasync tests (31 tests in dedicated suite)
- **Framework**: pytest 9.0+ with coverage support

## Notes

- Tests require a `.env` file with test database credentials. See `.env.example`.
- Integration tests require a running MySQL and Redis instance (use `docker-compose up -d`).
- For local development, use `pytest -v` to see detailed output.

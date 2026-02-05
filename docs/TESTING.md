# Testing Overview

Core testing documentation and quick references for running the automated test suite.

- Unit & Integration: Vitest (see `tradermate-portal` folder for test files)
- E2E: Playwright (see `docs/frontend/E2E_README.md` for scenarios and commands)
- Test summaries: `docs/frontend/TEST_SUMMARY.md`

Quick commands:

```bash
# Install deps and Playwright browsers
npm install
npx playwright install

# Run unit tests
npm run test:run

# Run coverage
npm run test:coverage

# Run E2E tests
npm run test:e2e
```

For full testing guidance and examples, open `docs/frontend/TEST_SUMMARY.md` and `docs/frontend/E2E_README.md`.

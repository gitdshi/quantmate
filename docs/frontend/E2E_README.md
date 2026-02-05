# E2E Test Suite for TraderMate

## Overview
End-to-end tests to validate complete user flows and interactions.

## Test Scenarios

### 1. Authentication Flow
- User registration
- User login
- Session persistence
- Logout

### 2. Strategy Management Flow
- Create new strategy
- View strategy details
- Edit existing strategy
- Delete strategy
- Browse built-in strategies

### 3. Backtest Flow
- Submit backtest
- Monitor job status
- View results
- Cancel running backtest

### 4. Market Data Flow
- Search symbols
- View market overview
- Check technical indicators
- View historical data

### 5. Portfolio Flow
- View open positions
- Close a position
- View trade history

### 6. Analytics Flow
- View portfolio analytics
- Check risk metrics
- Compare backtest results

## Running E2E Tests

### Prerequisites
```bash
npm install -D playwright @playwright/test
npx playwright install
```

### Run Tests
```bash
# Run all E2E tests
npm run test:e2e

# Run in headed mode
npm run test:e2e:headed

# Run specific test
npx playwright test auth.spec.ts

# Run with UI
npx playwright test --ui

# Generate report
npx playwright show-report
```

## Test Structure

```
e2e/
  ├── auth.spec.ts          # Authentication tests
  ├── strategies.spec.ts    # Strategy management tests
  ├── backtest.spec.ts      # Backtest workflow tests
  ├── market-data.spec.ts   # Market data tests
  ├── portfolio.spec.ts     # Portfolio tests
  └── analytics.spec.ts     # Analytics tests
```

## Test Data
Tests use mock data and a test database. Ensure the backend is running in test mode:

```bash
# Backend test mode
TESTING=true python -m app.main
```

## CI/CD Integration
E2E tests run automatically in the CI pipeline:
- On every PR
- Before deployment to production
- Scheduled daily runs

## Debugging
```bash
# Debug mode with Playwright Inspector
PWDEBUG=1 npx playwright test

# Save trace for debugging
npx playwright test --trace on
```

## Best Practices
1. Use data-testid attributes for reliable selectors
2. Clean up test data after each test
3. Use page object models for complex flows
4. Mock external APIs when possible
5. Keep tests isolated and independent

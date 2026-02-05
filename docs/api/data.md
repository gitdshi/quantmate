# Market Data API

Base path: `/api/data`

Endpoints

- `GET /api/data/symbols`
  - Description: List available symbols (supports search and pagination)
  - Query params: `?q=000001&page=1&limit=100`
  - Response: `{ items: [{ symbol, name, exchange }], total: number }`

- `GET /api/data/history`
  - Description: Retrieve OHLCV historical data
  - Query params: `?symbol=000001.SZ&start=2020-01-01&end=2023-12-31&interval=daily`
  - Response: Array of OHLCV rows `{ date, open, high, low, close, volume }`

- `GET /api/data/indicators`
  - Description: Compute technical indicators for a symbol (MA, EMA, RSI, etc.)
  - Query params: `?symbol=000001.SZ&period=30&type=ma`
  - Response: Indicator series suitable for chart overlays

- `GET /api/data/overview`
  - Description: Market overview and indices snapshot

- `GET /api/data/sectors`
  - Description: Sector aggregation and sector-level metrics

Notes

- Data source: Tushare DB or configured market data DB. Ensure ingestion is up-to-date for accurate backtests.
- Pagination and caching headers are used for large history responses.

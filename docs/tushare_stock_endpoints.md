Tushare (stock) endpoints — inventory and DB mapping

Scope
- Coverage: Tushare Pro endpoints relevant to 沪深股票 (A-shares) and related market/company/financial data.
- Goal: map each API to an existing or proposed database table in `mysql/init/init.sql`.

1) Basic data
- `stock_basic` -> `tushare_stock_stock_basic` (structured columns: ts_code, symbol, name, area, industry, market, exchange, list_status, list_date, delist_date, is_hs)
- `company` -> `tushare_stock_company` (structured columns + `business_scope` TEXT)
- `trade_cal` -> `tushare_stock_trade_cal` (exchange, cal_date, is_open)
- `hs_const` -> `tushare_stock_hs_const` (ts_code, trade_date, in_date/out_date)
- `namechange` -> `tushare_stock_name_change` (history)
- `new_share` -> `tushare_stock_new_share` (ipo/issue info)

2) Market / price data
- `daily` -> `tushare_stock_daily` (ts_code, trade_date, open, high, low, close, pre_close, pct_change, vol, amount)
- `daily_basic` -> `tushare_stock_daily_basic` (turnover, pe, pb, mv fields)
- `adj_factor` -> `tushare_stock_adj_factor` (factor by date)
- `moneyflow` -> `tushare_stock_moneyflow` (net flows, buy/sell by size)
- `suspend` -> `tushare_stock_suspend` (suspend/resume records)
- `limit_list` -> (propose `tushare_stock_limit_list`) (limit-up/down events)
- `lt_minute` / `min` -> `tushare_stock_minute` (minute bars)
- `tick` -> `tushare_stock_tick` (tick trades)
- `top_list` / `top_inst` -> (propose `tushare_stock_top_list`) (large-trade summaries)

3) Corporate actions & holders
- `dividend` -> `tushare_stock_dividend` (cash/stock/bonus)
- `stk_rewards` -> (propose `tushare_stock_stk_rewards`) (share incentives)
- `top10_holders` / `top10_floatholders` -> `tushare_stock_top10_holders`
- `holder_changes` -> (propose `tushare_stock_holder_changes`) (shareholder count / net changes)
- `pledge_stat` / `pledge_detail` -> (propose `tushare_stock_pledge`) (pledged shares)

4) Financial statements / fundamentals
- `income` / `balance` / `cashflow` -> `tushare_stock_financial_statement` (JSON) OR normalized tables if required; currently stored in `data` JSON with `statement_type` tag
- `fina_indicator` -> `tushare_stock_financial_meta` (JSON)
- `fina_audit` -> `tushare_stock_financial_meta` (api_name = 'fina_audit')
- `fina_mainbz` (主营业务构成) -> `tushare_stock_financial_meta`
- `express` (快报) / `forecast` (业绩预告) / `profit` (业绩快报) -> `tushare_stock_financial_meta`

5) Financing / margin / repo / funding
- `margin` / `margin_detail` / `margin_target` -> (propose `tushare_stock_margin*` tables)
- `repo` / `rpt_funding` -> (propose `tushare_stock_repo`)

6) Block trades / large orders / short sell
- `block_trade` / `block_deal` -> `tushare_stock_block_trade`
- `short_sell` -> `tushare_stock_short_sell`

7) Limits / large-order summaries
- `limit_list` -> `tushare_stock_limit_list`
- `top_list` / `top_inst` -> `tushare_stock_top_list` / `tushare_stock_top_inst`

8) Shareholder / pledge / rewards
- `stk_rewards` -> `tushare_stock_stk_rewards`
- `holder_changes` -> `tushare_stock_holder_changes`
- `pledge_stat` / `pledge_detail` -> `tushare_stock_pledge`

6) Industry / concept / classification / index
- `index_basic` / `index_daily` -> (propose `tushare_stock_index_*` tables)
- `index_member` -> (index constituents table)
- `concept` / `concept_detail` / `concept_class` -> (propose `tushare_stock_concept*` tables)
- `classification` / `industry` -> (tushare_stock_classification) (industry taxonomy table)

Storage decisions made and next refinements
- Where endpoints are stable and small (stock_basic, company, trade_cal), store as normalized columns.
- Time-series data (daily/minute/tick/adj/moneyflow) use composite PKs `(ts_code, date/time)` for idempotent upserts.
- Variable or evolving disclosures (financial statements, some meta endpoints) are stored as JSON in `tushare_stock_financial_statement` and `tushare_stock_financial_meta` to avoid breaking schema on field additions; we'll expand to typed columns later for frequently-queried metrics (ROE, EPS, operating revenue, etc.).

What I added to `mysql/init/init.sql` today
- `tushare_stock_limit_list`, `tushare_stock_top_list`, `tushare_stock_top_inst`, `tushare_stock_stk_rewards`, `tushare_stock_holder_changes`, `tushare_stock_pledge`, `tushare_stock_margin`, `tushare_stock_margin_detail`, `tushare_stock_block_trade`, `tushare_stock_short_sell`, `tushare_stock_index_basic`, `tushare_stock_index_member`, `tushare_stock_index_daily`, `tushare_stock_concept`, `tushare_stock_concept_detail`, `tushare_stock_classification`, `tushare_stock_repo`, `tushare_stock_ingest_audit`.

Next automated steps (I'll run unless you stop me):
- Crawl each endpoint doc page and extract exact field lists (will take multiple requests).
- For each financial endpoint (`income`, `balance`, `cashflow`, `fina_indicator`) propose expanded typed columns for the most-used fields and update `init.sql` accordingly.
-- Generate a Python ingestion template that uses `tushare` client to fetch, transform and upsert into these tables, plus an ingestion runner using `tushare_stock_ingest_audit` to track runs.

7) Other market behavior data
- `margin` / `short_sell` / `block_trade` / `block_deal` -> (propose tables)
- `stk_rewards` -> incentives

Storage strategy and rationale
- Time-series (daily/minute/tick/adj/moneyflow): structured relational tables with composite PK `(ts_code, date/time)` for dedup and fast range queries; indexes on `(ts_code, date)` and `(date)`.
- Stable metadata (stock_basic, company, trade_cal): normalized tables keyed by `ts_code` or `(exchange, cal_date)`.
- Variable/dense financial disclosures (income/balance/cashflow/indicators): stored as JSON in `tushare_stock_financial_statement` and `tushare_stock_financial_meta` for forward compatibility; create additional normalized tables only after identifying key recurring fields to optimize query patterns.
- Raw API responses: keep `tushare_stock_raw_response` for debugging/backfill and replay.

Next actions (if you confirm):
- Crawl each endpoint doc page and extract the exact field list and types, then refine table columns for endpoints currently stored as JSON (e.g., expand `income` to explicit numeric columns).
- Generate Python ingestion templates (using `tushare` python client) to fetch and upsert data into each table.

If you'd like, I can start crawling each endpoint page to extract exact fields and update `mysql/init/init.sql` to replace JSON storage with typed columns where appropriate.

-- Issue #10: Weekly/Monthly/Index Data Sync
-- Creates stock_weekly, stock_monthly tables in tushare DB
-- index_daily and adj_factor already exist

-- Weekly K-line (same structure as stock_daily)
CREATE TABLE IF NOT EXISTS stock_weekly (
    ts_code       VARCHAR(32)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_weekly_ts ON stock_weekly(ts_code);
CREATE INDEX idx_weekly_date ON stock_weekly(trade_date);

-- Monthly K-line (same structure as stock_daily)
CREATE TABLE IF NOT EXISTS stock_monthly (
    ts_code       VARCHAR(32)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_monthly_ts ON stock_monthly(ts_code);
CREATE INDEX idx_monthly_date ON stock_monthly(trade_date);

-- Index weekly K-line
CREATE TABLE IF NOT EXISTS index_weekly (
    index_code    VARCHAR(32)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (index_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Register new data source items
INSERT INTO data_source_items (source, item_key, item_name, enabled, description) VALUES
('tushare', 'stock_weekly',  '周线行情',      1, 'A股周K线'),
('tushare', 'stock_monthly', '月线行情',      1, 'A股月K线'),
('tushare', 'index_weekly',  '指数周线',      1, '指数周K线'),
('tushare', 'index_daily',   '指数日线',      1, '指数日K线')
ON DUPLICATE KEY UPDATE item_name = VALUES(item_name);

-- Extend data_sync_status step_name ENUM to include new steps
ALTER TABLE data_sync_status
    MODIFY COLUMN step_name ENUM(
        'akshare_index',
        'tushare_stock_basic',
        'tushare_stock_daily',
        'tushare_adj_factor',
        'tushare_dividend',
        'tushare_top10_holders',
        'vnpy_sync',
        'tushare_stock_weekly',
        'tushare_stock_monthly',
        'tushare_index_daily',
        'tushare_index_weekly'
    ) NOT NULL;

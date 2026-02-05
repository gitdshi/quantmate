-- =============================================================================
-- TraderMate MySQL Initialization Script
-- Creates two separate databases:
--   1. tushare - for Tushare data ingestion (raw market data)
--   2. vnpy    - for vnpy trading platform (backtesting, strategies)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DATABASE 1: tushare - Raw Tushare Data Ingestion
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS tushare CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE tushare;

-- Stock basic information (tushare: stock_basic)
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code VARCHAR(32) NOT NULL,
    symbol VARCHAR(16),
    name VARCHAR(255),
    area VARCHAR(64),
    industry VARCHAR(128),
    fullname VARCHAR(255),
    enname VARCHAR(255),
    market VARCHAR(32),
    exchange VARCHAR(16),
    list_status VARCHAR(16),
    list_date DATE,
    delist_date DATE,
    is_hs VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_stock_basic_symbol ON stock_basic(symbol);
CREATE INDEX idx_stock_basic_exchange ON stock_basic(exchange);

-- Company information (tushare: company)
CREATE TABLE IF NOT EXISTS stock_company (
    ts_code VARCHAR(32) NOT NULL,
    chairman VARCHAR(128),
    manager VARCHAR(128),
    secretary VARCHAR(128),
    reg_capital VARCHAR(64),
    setup_date DATE,
    province VARCHAR(64),
    city VARCHAR(64),
    website VARCHAR(255),
    email VARCHAR(128),
    employees INT,
    business_scope TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Trading calendar (tushare: trade_cal)
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange VARCHAR(16) NOT NULL,
    cal_date DATE NOT NULL,
    is_open TINYINT NOT NULL DEFAULT 0,
    pretrade_date DATE,
    PRIMARY KEY (exchange, cal_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- HS (沪深股通) constituents (tushare: hs_const)
CREATE TABLE IF NOT EXISTS hs_const (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    in_date DATE,
    out_date DATE,
    market VARCHAR(32),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hs_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Stock name change history (tushare: namechange)
CREATE TABLE IF NOT EXISTS stock_name_change (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    INDEX idx_namechange_ts (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- New share / IPO list (tushare: new_share)
CREATE TABLE IF NOT EXISTS new_share (
    ts_code VARCHAR(32) NOT NULL,
    name VARCHAR(255),
    ipo_date DATE,
    issue_date DATE,
    issue_price DECIMAL(12,2),
    amount BIGINT,
    market VARCHAR(32),
    PRIMARY KEY (ts_code, ipo_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================
-- Market / price / time-series data
-- =====================================

-- Daily OHLC data (tushare: daily)
CREATE TABLE IF NOT EXISTS stock_daily (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,2),
    high DECIMAL(16,2),
    low DECIMAL(16,2),
    close DECIMAL(16,2),
    pre_close DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change DECIMAL(10,2),
    vol BIGINT,
    amount DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_daily_ts ON stock_daily(ts_code);
CREATE INDEX idx_daily_date ON stock_daily(trade_date);
CREATE INDEX idx_daily_ts_date ON stock_daily(ts_code, trade_date);

-- Adjustment factor (tushare: adj_factor)
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DECIMAL(24,12),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Daily derived indicators (tushare: daily_basic)
CREATE TABLE IF NOT EXISTS daily_basic (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    turnover_rate DECIMAL(10,2),
    turnover_rate_f DECIMAL(10,2),
    volume_ratio DECIMAL(10,2),
    pe DECIMAL(12,2),
    pe_ttm DECIMAL(12,2),
    pb DECIMAL(12,2),
    ps DECIMAL(12,2),
    ps_ttm DECIMAL(12,2),
    total_mv DECIMAL(20,2),
    circ_mv DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Money flow / capital movement (tushare: moneyflow)
CREATE TABLE IF NOT EXISTS stock_moneyflow (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    net_mf DECIMAL(20,2),
    buy_small DECIMAL(20,2),
    sell_small DECIMAL(20,2),
    buy_medium DECIMAL(20,2),
    sell_medium DECIMAL(20,2),
    buy_large DECIMAL(20,2),
    sell_large DECIMAL(20,2),
    buy_huge DECIMAL(20,2),
    sell_huge DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Minute-level bar data (tushare: index/minute / stock/lt_minute)
CREATE TABLE IF NOT EXISTS stock_minute (
    ts_code VARCHAR(32) NOT NULL,
    trade_time DATETIME NOT NULL,
    open DECIMAL(16,2),
    high DECIMAL(16,2),
    low DECIMAL(16,2),
    close DECIMAL(16,2),
    vol BIGINT,
    amount DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_minute_ts_time ON stock_minute(ts_code, trade_time);

-- Tick-level (tushare: tick)
CREATE TABLE IF NOT EXISTS stock_tick (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_time DATETIME NOT NULL,
    price DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    volume BIGINT,
    amount DECIMAL(20,2),
    type VARCHAR(16),
    INDEX idx_tick_ts_time (ts_code, trade_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Suspension / resumption (tushare: suspend)
CREATE TABLE IF NOT EXISTS stock_suspend (
    ts_code VARCHAR(32) NOT NULL,
    suspend_date DATE,
    resume_date DATE,
    reason TEXT,
    PRIMARY KEY (ts_code, suspend_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Corporate actions: dividends, allotments, splits (tushare: dividend)
CREATE TABLE IF NOT EXISTS stock_dividend (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    ann_date DATE,
    imp_ann_date DATE,
    record_date DATE,
    ex_date DATE,
    pay_date DATE,
    div_cash DECIMAL(20,2),
    div_stock DECIMAL(20,2),
    bonus_ratio DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_div_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Top-10 holders / top-10 floating holders
CREATE TABLE IF NOT EXISTS top10_holders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    end_date DATE NOT NULL,
    holder_name VARCHAR(255),
    hold_amount DECIMAL(20,2),
    hold_ratio DECIMAL(8,2),
    INDEX idx_top10_ts_end (ts_code, end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Financial statements stored as JSON payloads
CREATE TABLE IF NOT EXISTS financial_statement (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    statement_type VARCHAR(32) NOT NULL,
    ann_date DATE,
    end_date DATE,
    report_date DATE,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fin_ts_end (ts_code, end_date),
    INDEX idx_fin_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Financial indicators / audit / forecasts (generic storage)
CREATE TABLE IF NOT EXISTS financial_meta (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    api_name VARCHAR(64) NOT NULL,
    ann_date DATE,
    end_date DATE,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fm_ts_api (ts_code, api_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Raw responses capture (for debugging/backfill)
CREATE TABLE IF NOT EXISTS raw_response (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    interface_name VARCHAR(128) NOT NULL,
    params JSON,
    ts_code VARCHAR(32),
    data JSON,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_raw_iface (interface_name),
    INDEX idx_raw_ts (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Limit-up / limit-down list
CREATE TABLE IF NOT EXISTS stock_limit_list (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    limit_type VARCHAR(32),
    limit_reason TEXT,
    INDEX idx_limit_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Top institutional holdings
CREATE TABLE IF NOT EXISTS stock_top_list (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    change_amount DECIMAL(20,2),
    change_rate DECIMAL(10,2),
    reason TEXT,
    INDEX idx_toplist_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Top institutional holdings by investor
CREATE TABLE IF NOT EXISTS stock_top_inst (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    end_date DATE,
    inst_name VARCHAR(255),
    hold_amount DECIMAL(20,2),
    hold_ratio DECIMAL(8,2),
    INDEX idx_topinst_ts_end (ts_code, end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Stock incentives / rewards
CREATE TABLE IF NOT EXISTS stock_stk_rewards (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    reward_date DATE,
    holder VARCHAR(255),
    change_amount DECIMAL(20,2),
    change_ratio DECIMAL(8,2),
    note TEXT,
    INDEX idx_rewards_ts_date (ts_code, reward_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Holder changes
CREATE TABLE IF NOT EXISTS holder_changes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    change_date DATE,
    holder_name VARCHAR(255),
    before_amount DECIMAL(20,2),
    after_amount DECIMAL(20,2),
    change_amount DECIMAL(20,2),
    INDEX idx_holderchg_ts_date (ts_code, change_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Pledge statistics
CREATE TABLE IF NOT EXISTS stock_pledge (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    pledge_date DATE,
    pledge_amount DECIMAL(20,2),
    pledge_ratio DECIMAL(8,2),
    pledge_holder VARCHAR(255),
    detail JSON,
    INDEX idx_pledge_ts_date (ts_code, pledge_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Margin financing / securities lending
CREATE TABLE IF NOT EXISTS stock_margin (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    financing_balance DECIMAL(20,2),
    financing_buy DECIMAL(20,2),
    financing_repay DECIMAL(20,2),
    securities_lend_balance DECIMAL(20,2),
    INDEX idx_margin_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_margin_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    detail JSON,
    INDEX idx_margindet_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Block trades / large orders
CREATE TABLE IF NOT EXISTS block_trade (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    trade_time DATETIME,
    price DECIMAL(16,2),
    volume BIGINT,
    amount DECIMAL(20,2),
    side VARCHAR(16),
    INDEX idx_block_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Short sell / borrow related data
CREATE TABLE IF NOT EXISTS short_sell (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    short_volume BIGINT,
    short_amount DECIMAL(20,2),
    INDEX idx_short_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Index basics and constituents
CREATE TABLE IF NOT EXISTS index_basic (
    index_code VARCHAR(32) NOT NULL PRIMARY KEY,
    name VARCHAR(255),
    market VARCHAR(32),
    publisher VARCHAR(128),
    category VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS index_member (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    index_code VARCHAR(32) NOT NULL,
    ts_code VARCHAR(32) NOT NULL,
    in_date DATE,
    out_date DATE,
    weight DECIMAL(12,8),
    INDEX idx_index_member_code (index_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS index_daily (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,2),
    high DECIMAL(16,2),
    low DECIMAL(16,2),
    close DECIMAL(16,2),
    vol BIGINT,
    amount DECIMAL(20,2),
    PRIMARY KEY (index_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Concept / classification tables
CREATE TABLE IF NOT EXISTS stock_concept (
    concept_code VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255),
    description TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS concept_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    concept_code VARCHAR(64) NOT NULL,
    ts_code VARCHAR(32) NOT NULL,
    in_date DATE,
    out_date DATE,
    INDEX idx_concept_ts (concept_code, ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_classification (
    class_code VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255),
    parent_code VARCHAR(64)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Repo / funding market data
CREATE TABLE IF NOT EXISTS repo (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    repo_date DATE,
    instrument VARCHAR(64),
    rate DECIMAL(12,2),
    amount DECIMAL(20,2),
    INDEX idx_repo_date (repo_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Ingestion audit table
CREATE TABLE IF NOT EXISTS ingest_audit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    api_name VARCHAR(128) NOT NULL,
    params JSON,
    status VARCHAR(32),
    fetched_rows INT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    INDEX idx_ingest_api (api_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Daily sync log to record per-day sync results
CREATE TABLE IF NOT EXISTS sync_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sync_date DATE NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    rows_synced INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    UNIQUE KEY uq_sync_date_endpoint (sync_date, endpoint),
    INDEX idx_sync_date (sync_date),
    INDEX idx_sync_endpoint (endpoint)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- -----------------------------------------------------------------------------
-- DATABASE 2: vnpy - VeighNa Trading Platform Data
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS vnpy CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE vnpy;

-- Bar data table (OHLCV for backtesting and live trading)
CREATE TABLE IF NOT EXISTS dbbardata (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(45) NOT NULL,
    exchange VARCHAR(45) NOT NULL,
    `datetime` DATETIME NOT NULL,
    `interval` VARCHAR(45) NOT NULL,
    volume DOUBLE,
    turnover DOUBLE,
    open_interest DOUBLE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    UNIQUE KEY idx_bar_unique (symbol, exchange, `interval`, `datetime`),
    INDEX idx_bar_symbol_exchange (symbol, exchange),
    INDEX idx_bar_datetime (`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Bar overview table (summary of available bar data)
CREATE TABLE IF NOT EXISTS dbbaroverview (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(45) NOT NULL,
    exchange VARCHAR(45) NOT NULL,
    `interval` VARCHAR(45) NOT NULL,
    count INT,
    start DATETIME,
    end DATETIME,
    UNIQUE KEY idx_overview_unique (symbol, exchange, `interval`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tick data table
CREATE TABLE IF NOT EXISTS dbtickdata (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(45) NOT NULL,
    exchange VARCHAR(45) NOT NULL,
    `datetime` DATETIME NOT NULL,
    name VARCHAR(45),
    volume DOUBLE,
    turnover DOUBLE,
    open_interest DOUBLE,
    last_price DOUBLE,
    last_volume DOUBLE,
    limit_up DOUBLE,
    limit_down DOUBLE,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    pre_close DOUBLE,
    bid_price_1 DOUBLE,
    bid_price_2 DOUBLE,
    bid_price_3 DOUBLE,
    bid_price_4 DOUBLE,
    bid_price_5 DOUBLE,
    ask_price_1 DOUBLE,
    ask_price_2 DOUBLE,
    ask_price_3 DOUBLE,
    ask_price_4 DOUBLE,
    ask_price_5 DOUBLE,
    bid_volume_1 DOUBLE,
    bid_volume_2 DOUBLE,
    bid_volume_3 DOUBLE,
    bid_volume_4 DOUBLE,
    bid_volume_5 DOUBLE,
    ask_volume_1 DOUBLE,
    ask_volume_2 DOUBLE,
    ask_volume_3 DOUBLE,
    ask_volume_4 DOUBLE,
    ask_volume_5 DOUBLE,
    `localtime` DATETIME,
    UNIQUE KEY idx_tick_unique (symbol, exchange, `datetime`),
    INDEX idx_tick_symbol_exchange (symbol, exchange),
    INDEX idx_tick_datetime (`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tick overview table
CREATE TABLE IF NOT EXISTS dbtickoverview (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(45) NOT NULL,
    exchange VARCHAR(45) NOT NULL,
    count INT,
    start DATETIME,
    end DATETIME,
    UNIQUE KEY idx_tick_overview_unique (symbol, exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Sync status table (track what data has been synced from tushare)
CREATE TABLE IF NOT EXISTS sync_status (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(45) NOT NULL,
    exchange VARCHAR(45) NOT NULL,
    `interval` VARCHAR(45) NOT NULL,
    last_sync_date DATE,
    last_sync_count INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_sync_status (symbol, exchange, `interval`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- -----------------------------------------------------------------------------
-- Grant permissions (adjust user as needed)
-- -----------------------------------------------------------------------------

-- Create user with access to both databases if not exists
-- Note: Adjust password in production!
CREATE USER IF NOT EXISTS 'tradermate'@'%' IDENTIFIED BY 'tradermate123';
GRANT ALL PRIVILEGES ON tushare.* TO 'tradermate'@'%';
GRANT ALL PRIVILEGES ON vnpy.* TO 'tradermate'@'%';
FLUSH PRIVILEGES;

-- Also grant root user access (for docker-compose connection)
GRANT ALL PRIVILEGES ON tushare.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON vnpy.* TO 'root'@'%';
FLUSH PRIVILEGES;

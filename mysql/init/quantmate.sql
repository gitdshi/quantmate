-- =============================================================================
-- QuantMate Backend API Database (Merged Init)
-- Database: quantmate - stores user accounts, strategies, backtest results,
--           portfolios, alerts, system config, AI, and more
-- Includes all migrations 000-016
-- =============================================================================

CREATE DATABASE IF NOT EXISTS quantmate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE quantmate;

-- =============================================================================
-- SECTION 1: Core Tables (baseline)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Users table - stores user accounts and authentication
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) UNIQUE,
    hashed_password VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    must_change_password BOOLEAN DEFAULT FALSE NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='User accounts';

-- -----------------------------------------------------------------------------
-- Strategies table - stores user-created trading strategies
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    class_name VARCHAR(100) NOT NULL,
    description TEXT,
    parameters TEXT,
    code LONGTEXT NOT NULL,
    version INT NOT NULL DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_strategy (user_id, name),
    INDEX idx_user_id (user_id),
    INDEX idx_class_name (class_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='User trading strategies';

-- -----------------------------------------------------------------------------
-- Backtest history table - stores backtest execution history and results
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS backtest_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL UNIQUE,
    bulk_job_id VARCHAR(36),
    strategy_id INT,
    strategy_class VARCHAR(100),
    strategy_version INT,
    vt_symbol VARCHAR(50),
    start_date DATE,
    end_date DATE,
    parameters JSON,
    status VARCHAR(20) NOT NULL,
    result JSON,
    error TEXT,
    created_at DATETIME NOT NULL,
    completed_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE SET NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_job_id (job_id),
    INDEX idx_bulk_job_id (bulk_job_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Backtest execution history';

-- -----------------------------------------------------------------------------
-- Bulk backtest table - tracks multi-symbol bulk backtest jobs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bulk_backtest (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL UNIQUE,
    strategy_id INT,
    strategy_class VARCHAR(100),
    strategy_version INT,
    symbols JSON NOT NULL,
    start_date DATE,
    end_date DATE,
    parameters JSON,
    initial_capital DOUBLE DEFAULT 100000,
    rate DOUBLE DEFAULT 0.0001,
    slippage DOUBLE DEFAULT 0,
    benchmark VARCHAR(50) DEFAULT '399300.SZ',
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    total_symbols INT DEFAULT 0,
    completed_count INT DEFAULT 0,
    best_return DOUBLE,
    best_symbol VARCHAR(50),
    created_at DATETIME NOT NULL,
    completed_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE SET NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_job_id (job_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Bulk backtest jobs tracking';

-- -----------------------------------------------------------------------------
-- Watchlists table (migration 005 version - description/sort_order replaces symbols JSON)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS watchlists (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    name        VARCHAR(100) NOT NULL,
    description VARCHAR(500) DEFAULT NULL,
    sort_order  INT          NOT NULL DEFAULT 0,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_wl_user (user_id),
    CONSTRAINT fk_wl_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS watchlist_items (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    watchlist_id INT          NOT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    added_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes        VARCHAR(500) DEFAULT NULL,
    INDEX idx_wli_wl (watchlist_id),
    UNIQUE KEY uq_wli_symbol (watchlist_id, symbol),
    CONSTRAINT fk_wli_wl FOREIGN KEY (watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Optimization results table - stores parameter optimization results
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS optimization_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL UNIQUE,
    strategy_class VARCHAR(100),
    vt_symbol VARCHAR(50),
    start_date DATE,
    end_date DATE,
    parameter_grid JSON,
    status VARCHAR(20) NOT NULL,
    best_params JSON,
    best_result JSON,
    all_results JSON,
    error TEXT,
    created_at DATETIME NOT NULL,
    completed_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_user_id (user_id),
    INDEX idx_job_id (job_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Strategy optimization results';

-- Strategy history - stores historical snapshots of strategy code
CREATE TABLE IF NOT EXISTS strategy_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    strategy_id INT NOT NULL,
    strategy_name VARCHAR(200),
    class_name VARCHAR(200),
    description TEXT,
    version INT,
    parameters TEXT,
    code LONGTEXT,
    created_at DATETIME NOT NULL,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE,
    INDEX idx_strategy_id (strategy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='History snapshots for DB strategy code';

-- -----------------------------------------------------------------------------
-- Data sync status table (refactored: dynamic multi-source, migration 018)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_sync_status (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sync_date DATE NOT NULL,
    source VARCHAR(50) NOT NULL,
    interface_key VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    rows_synced INT DEFAULT 0,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_date_source_interface (sync_date, source, interface_key),
    INDEX idx_status (status),
    INDEX idx_sync_date (sync_date),
    INDEX idx_source_interface (source, interface_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Per-interface per-trading-day sync status tracking';

-- =============================================================================
-- SECTION 2: Migration Tables (000-016)
-- =============================================================================

-- Migration 000: Schema migration tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(14) NOT NULL PRIMARY KEY COMMENT 'Migration version (YYYYMMDDHHMMSS)',
    name VARCHAR(255) NOT NULL COMMENT 'Migration script name',
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    checksum VARCHAR(64) COMMENT 'SHA-256 of the migration file content'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tracks applied database migrations';

INSERT IGNORE INTO schema_migrations (version, name) VALUES
    ('00000000000000', 'baseline_init_quantmate'),
    ('000', '000_create_migration_table.sql'),
    ('001', '001_create_audit_logs.sql'),
    ('002', '002_create_user_profiles.sql'),
    ('003', '003_create_kyc_submissions.sql'),
    ('004', '004_create_data_source_items.sql'),
    ('005', '005_create_watchlists.sql'),
    ('006', '006_create_portfolio_tables.sql'),
    ('007', '007_create_trade_logs.sql'),
    ('008', '008_add_weekly_monthly_index_tables.sql'),
    ('009', '009_create_mfa_apikey_sessions.sql'),
    ('010', '010_create_trading_tables.sql'),
    ('011', '011_create_alerts_reports_tables.sql'),
    ('012', '012_create_system_config_optimization_indicator.sql'),
    ('013', '013_create_tushare_extended_tables.sql'),
    ('014', '014_create_akshare_minute_preset_tables.sql'),
    ('015', '015_create_p3_feature_tables.sql'),
    ('016', '016_create_multi_market_tables.sql'),
    ('017', '017_create_paper_deployments.sql'),
    ('018', '018_datasync_multi_source_refactor.sql');

-- Migration 001: Audit logs
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    user_id INT,
    username VARCHAR(50),
    operation_type VARCHAR(50) NOT NULL COMMENT 'e.g. AUTH_LOGIN, STRATEGY_CREATE, DATA_ACCESS',
    resource_type VARCHAR(50) COMMENT 'e.g. user, strategy, backtest',
    resource_id VARCHAR(100) COMMENT 'ID of the affected resource',
    details JSON COMMENT 'Operation-specific details',
    ip_address VARCHAR(45),
    user_agent VARCHAR(500),
    http_method VARCHAR(10),
    http_path VARCHAR(500),
    http_status INT,
    INDEX idx_timestamp (timestamp),
    INDEX idx_user_id (user_id),
    INDEX idx_operation_type (operation_type),
    INDEX idx_resource_type (resource_type),
    INDEX idx_user_timestamp (user_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Immutable audit log for all user operations';

-- Migration 002: User profiles
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id       INT          NOT NULL PRIMARY KEY,
    display_name  VARCHAR(100) DEFAULT NULL,
    avatar_url    VARCHAR(500) DEFAULT NULL,
    phone         VARCHAR(30)  DEFAULT NULL,
    timezone      VARCHAR(50)  DEFAULT 'Asia/Shanghai',
    language      VARCHAR(10)  DEFAULT 'zh-CN',
    bio           TEXT         DEFAULT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_profile_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 003: KYC verification
CREATE TABLE IF NOT EXISTS kyc_submissions (
    id            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    status        ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
    real_name     VARCHAR(100) NOT NULL,
    id_number     VARCHAR(100) NOT NULL COMMENT 'Encrypted',
    id_type       VARCHAR(20)  NOT NULL DEFAULT 'mainland_id' COMMENT 'mainland_id|passport|hk_pass',
    id_front_path VARCHAR(500) NOT NULL COMMENT 'Encrypted file path',
    id_back_path  VARCHAR(500) NOT NULL COMMENT 'Encrypted file path',
    reviewer_id   INT          DEFAULT NULL,
    review_notes  TEXT         DEFAULT NULL,
    reviewed_at   DATETIME     DEFAULT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_kyc_user (user_id),
    INDEX idx_kyc_status (status),
    CONSTRAINT fk_kyc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 004+018: Data source items configuration (with multi-source columns)
CREATE TABLE IF NOT EXISTS data_source_items (
    id                  INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source              VARCHAR(50)  NOT NULL COMMENT 'tushare, akshare, etc.',
    item_key            VARCHAR(100) NOT NULL,
    item_name           VARCHAR(200) NOT NULL,
    enabled             TINYINT(1)   NOT NULL DEFAULT 1,
    description         TEXT         DEFAULT NULL,
    requires_permission VARCHAR(50)  DEFAULT NULL COMMENT 'Permission level required',
    target_database     VARCHAR(50)  NOT NULL DEFAULT '' COMMENT 'Target DB: tushare, akshare',
    target_table        VARCHAR(100) NOT NULL DEFAULT '' COMMENT 'Target table name',
    table_created       TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1 if table has been created',
    sync_priority       INT          NOT NULL DEFAULT 100 COMMENT 'Lower = higher priority',
    updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_source_item (source, item_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Consolidated seed data for data_source_items (from migrations 004, 008, 013, 014, 018)
INSERT INTO data_source_items (source, item_key, item_name, enabled, description, requires_permission, target_database, target_table, table_created, sync_priority) VALUES
-- Core tushare items
('tushare', 'stock_basic',     '股票基本信息',  1, 'A股基本资料',                   NULL,      'tushare', 'stock_basic',     1, 10),
('tushare', 'stock_daily',     '日线行情',      1, 'A股日K线',                      NULL,      'tushare', 'stock_daily',     1, 20),
('tushare', 'adj_factor',      '复权因子',      1, '前复权因子',                     NULL,      'tushare', 'adj_factor',      1, 30),
('tushare', 'trade_cal',       '交易日历',      1, '交易所交易日历',                  NULL,      'akshare', 'trade_cal',       1, 5),
('tushare', 'stock_dividend',  '分红送股',      0, '分红送转信息',                   'premium', 'tushare', 'stock_dividend',  1, 50),
-- Weekly/Monthly/Index items
('tushare', 'stock_weekly',    '周线行情',      1, 'A股周K线',                      NULL,      'tushare', 'stock_weekly',    1, 25),
('tushare', 'stock_monthly',   '月线行情',      1, 'A股月K线',                      NULL,      'tushare', 'stock_monthly',   1, 26),
('tushare', 'index_weekly',    '指数周线',      1, '指数周K线',                      NULL,      'tushare', 'index_weekly',    1, 28),
('tushare', 'index_daily',     '指数日线',      1, '指数日K线',                      NULL,      'tushare', 'index_daily',     1, 27),
-- Extended tushare items
('tushare', 'money_flow',      '资金流向',      1, '个股资金流向数据(大中小单)',       '0',       'tushare', 'moneyflow',       1, 60),
('tushare', 'stk_limit',       '涨跌停统计',    1, '涨跌停数据(封单/强度)',           '0',       'tushare', 'stk_limit',       1, 70),
('tushare', 'margin_detail',   '融资融券',      1, '融资融券余额明细',                '0',       'tushare', 'margin',          1, 80),
('tushare', 'block_trade',     '大宗交易',      1, '大宗交易数据',                   '0',       'tushare', 'block_trade',     1, 90),
('tushare', 'stock_company',   '公司基本面',    1, '上市公司基本信息',                '0',       'tushare', 'stock_company',   1, 15),
('tushare', 'fina_indicator',  '财务指标',      1, '主要财务指标数据',                '0',       'tushare', 'fina_indicator',  1, 55),
('tushare', 'dividend',        '分红送股',      0, '分红送股数据(需高级权限)',         '1',       'tushare', 'stock_dividend',  1, 50),
('tushare', 'income',          '利润表',        0, '利润表数据(需高级权限)',           '1',       'tushare', 'income',          1, 56),
('tushare', 'top10_holders',   '十大股东',      0, '十大股东数据(需高级权限)',         '1',       'tushare', 'top10_holders',   1, 57),
-- AkShare items
('akshare', 'stock_zh_index',  '指数行情',      0, 'A股指数实时行情',                NULL,      'akshare', 'stock_zh_index_spot', 1, 40),
('akshare', 'stock_zh_index_spot', '指数实时行情', 1, 'A股指数实时报价',              '0',       'akshare', 'stock_zh_index_spot', 1, 40),
('akshare', 'fund_etf_daily',  'ETF日线',       1, 'ETF基金日K线数据',               '0',       'akshare', 'fund_etf_daily',  1, 45),
-- AkShare index_daily (for index OHLCV)
('akshare', 'index_daily',     '指数日线',      1, 'AkShare指数日K线(沪深300等)',     NULL,      'akshare', 'index_daily',     1, 41)
ON DUPLICATE KEY UPDATE
    item_name = VALUES(item_name),
    target_database = VALUES(target_database),
    target_table = VALUES(target_table),
    table_created = VALUES(table_created),
    sync_priority = VALUES(sync_priority);

-- Migration 006: Portfolio tables
CREATE TABLE IF NOT EXISTS portfolios (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    name        VARCHAR(100) NOT NULL DEFAULT 'Default',
    mode        ENUM('paper','live') NOT NULL DEFAULT 'paper',
    initial_cash DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    cash        DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_port_user (user_id),
    CONSTRAINT fk_port_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS portfolio_positions (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    portfolio_id INT          NOT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    quantity     INT          NOT NULL DEFAULT 0,
    avg_cost     DECIMAL(10,4) NOT NULL DEFAULT 0,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_port_sym (portfolio_id, symbol),
    CONSTRAINT fk_pos_port FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS portfolio_transactions (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    portfolio_id INT          NOT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    direction    ENUM('buy','sell') NOT NULL,
    quantity     INT          NOT NULL,
    price        DECIMAL(10,4) NOT NULL,
    fee          DECIMAL(10,4) NOT NULL DEFAULT 0,
    strategy_id  INT          DEFAULT NULL,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tx_port (portfolio_id),
    INDEX idx_tx_date (created_at),
    CONSTRAINT fk_tx_port FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    portfolio_id INT          NOT NULL,
    date         DATE         NOT NULL,
    nav          DECIMAL(16,4) NOT NULL,
    total_value  DECIMAL(16,4) NOT NULL,
    cash         DECIMAL(16,2) NOT NULL,
    positions_json JSON       DEFAULT NULL,
    returns_1d   DECIMAL(10,6) DEFAULT NULL,
    returns_5d   DECIMAL(10,6) DEFAULT NULL,
    returns_20d  DECIMAL(10,6) DEFAULT NULL,
    returns_ytd  DECIMAL(10,6) DEFAULT NULL,
    UNIQUE KEY uq_snap_date (portfolio_id, date),
    CONSTRAINT fk_snap_port FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 007: Trade audit logs
CREATE TABLE IF NOT EXISTS trade_logs (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    timestamp   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type  VARCHAR(50)  NOT NULL COMMENT 'signal|risk_check|order_submit|fill|settlement',
    symbol      VARCHAR(20)  NOT NULL,
    direction   VARCHAR(10)  DEFAULT NULL COMMENT 'buy|sell',
    quantity    INT          DEFAULT NULL,
    price       DECIMAL(10,4) DEFAULT NULL,
    strategy_id INT          DEFAULT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'created',
    notes       TEXT         DEFAULT NULL,
    INDEX idx_tl_time (timestamp),
    INDEX idx_tl_symbol (symbol),
    INDEX idx_tl_strategy (strategy_id),
    INDEX idx_tl_event (event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 009: MFA, API Key Management, Session Management
CREATE TABLE IF NOT EXISTS mfa_settings (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    mfa_type    ENUM('totp','email') NOT NULL DEFAULT 'totp',
    secret_encrypted VARCHAR(512) NOT NULL,
    is_enabled  TINYINT(1)   NOT NULL DEFAULT 0,
    recovery_codes_hash TEXT DEFAULT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_mfa_user (user_id),
    CONSTRAINT fk_mfa_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS api_keys (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    key_id       VARCHAR(64)  NOT NULL,
    secret_hash  VARCHAR(255) NOT NULL,
    name         VARCHAR(100) NOT NULL,
    permissions  JSON         DEFAULT NULL,
    expires_at   DATETIME     DEFAULT NULL,
    ip_whitelist JSON         DEFAULT NULL,
    rate_limit   INT          DEFAULT 60,
    is_active    TINYINT(1)   NOT NULL DEFAULT 1,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at DATETIME     DEFAULT NULL,
    UNIQUE KEY uq_key_id (key_id),
    INDEX idx_apikey_user (user_id),
    CONSTRAINT fk_apikey_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_sessions (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    token_hash   VARCHAR(255) NOT NULL,
    device_info  VARCHAR(255) DEFAULT NULL,
    ip_address   VARCHAR(45)  DEFAULT NULL,
    login_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_active_at DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at   DATETIME     NOT NULL,
    INDEX idx_sess_user (user_id),
    INDEX idx_sess_token (token_hash),
    CONSTRAINT fk_sess_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 010: Paper Trading, Order Management, Broker Config, Risk Rules
CREATE TABLE IF NOT EXISTS orders (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    portfolio_id INT          DEFAULT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    direction    ENUM('buy','sell') NOT NULL,
    order_type   ENUM('market','limit','stop','stop_limit') NOT NULL DEFAULT 'market',
    quantity     INT          NOT NULL,
    price        DECIMAL(10,4) DEFAULT NULL,
    stop_price   DECIMAL(10,4) DEFAULT NULL,
    status       ENUM('created','submitted','partial','filled','cancelled','rejected','expired') NOT NULL DEFAULT 'created',
    filled_quantity INT       NOT NULL DEFAULT 0,
    avg_fill_price  DECIMAL(10,4) DEFAULT NULL,
    fee          DECIMAL(10,4) NOT NULL DEFAULT 0,
    strategy_id  INT          DEFAULT NULL,
    mode         ENUM('paper','live') NOT NULL DEFAULT 'paper',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ord_user (user_id),
    INDEX idx_ord_status (status),
    INDEX idx_ord_symbol (symbol),
    INDEX idx_ord_date (created_at),
    CONSTRAINT fk_ord_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS trades (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id        INT          NOT NULL,
    filled_quantity INT          NOT NULL,
    filled_price    DECIMAL(10,4) NOT NULL,
    fee             DECIMAL(10,4) NOT NULL DEFAULT 0,
    filled_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_trade_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS risk_rules (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    name        VARCHAR(100) NOT NULL,
    rule_type   ENUM('position_limit','drawdown','concentration','frequency','custom') NOT NULL,
    condition_expr VARCHAR(500) DEFAULT NULL,
    threshold   DECIMAL(10,4) NOT NULL,
    action      ENUM('block','reduce','warn') NOT NULL DEFAULT 'warn',
    is_active   TINYINT(1)   NOT NULL DEFAULT 1,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_rr_user (user_id),
    CONSTRAINT fk_rr_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS broker_configs (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    broker_type     VARCHAR(50)  NOT NULL,
    name            VARCHAR(100) NOT NULL,
    config_json_encrypted TEXT   NOT NULL,
    is_active       TINYINT(1)   NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_bc_user (user_id),
    CONSTRAINT fk_bc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 011: Alert Engine, Notification Channels, Reports
CREATE TABLE IF NOT EXISTS alert_rules (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    name         VARCHAR(100) NOT NULL,
    metric       VARCHAR(100) NOT NULL,
    comparator   ENUM('gt','gte','lt','lte','eq','neq') NOT NULL,
    threshold    DECIMAL(16,4) NOT NULL,
    time_window  INT          DEFAULT NULL,
    level        ENUM('info','warning','severe') NOT NULL DEFAULT 'warning',
    is_active    TINYINT(1)   NOT NULL DEFAULT 1,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ar_user (user_id),
    CONSTRAINT fk_ar_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS alert_history (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    rule_id      INT          DEFAULT NULL,
    user_id      INT          NOT NULL,
    triggered_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level        ENUM('info','warning','severe') NOT NULL,
    message      TEXT         NOT NULL,
    status       ENUM('unread','read','acknowledged') NOT NULL DEFAULT 'unread',
    INDEX idx_ah_user (user_id),
    INDEX idx_ah_date (triggered_at),
    CONSTRAINT fk_ah_rule FOREIGN KEY (rule_id) REFERENCES alert_rules(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS notification_channels (
    id            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    channel_type  ENUM('email','wechat','dingtalk','telegram','slack','webhook') NOT NULL,
    config_json   JSON         NOT NULL,
    is_active     TINYINT(1)   NOT NULL DEFAULT 1,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_nc_user (user_id),
    CONSTRAINT fk_nc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS reports (
    id            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    report_type   ENUM('daily','weekly','monthly','custom') NOT NULL,
    period_start  DATE         NOT NULL,
    period_end    DATE         NOT NULL,
    content_json  JSON         DEFAULT NULL,
    pdf_path      VARCHAR(500) DEFAULT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rpt_user (user_id),
    INDEX idx_rpt_date (period_start, period_end),
    CONSTRAINT fk_rpt_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 012: System Config, Parameter Optimization, Indicator Library
CREATE TABLE IF NOT EXISTS system_configs (
    id                 INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    config_key         VARCHAR(100) NOT NULL,
    config_value       TEXT         NOT NULL,
    category           VARCHAR(50)  NOT NULL DEFAULT 'general',
    description        VARCHAR(500) DEFAULT NULL,
    is_user_overridable TINYINT(1)  NOT NULL DEFAULT 0,
    updated_by         INT          DEFAULT NULL,
    updated_at         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_config_key (config_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS data_source_configs (
    id                INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source_key        VARCHAR(50)  NOT NULL,
    display_name      VARCHAR(100) NOT NULL DEFAULT '',
    api_token_encrypted TEXT       DEFAULT NULL,
    config_json       JSON         DEFAULT NULL,
    rate_limit        INT          NOT NULL DEFAULT 60,
    enabled           TINYINT(1)   NOT NULL DEFAULT 1,
    requires_token    TINYINT(1)   DEFAULT 0,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_source_key (source_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed data source configs
INSERT INTO data_source_configs (source_key, display_name, enabled, rate_limit, requires_token) VALUES
('tushare', 'Tushare Pro', 1, 50, 1),
('akshare', 'AkShare', 1, 30, 0)
ON DUPLICATE KEY UPDATE display_name = VALUES(display_name);

CREATE TABLE IF NOT EXISTS optimization_tasks (
    id             INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id        INT          NOT NULL,
    strategy_id    INT          NOT NULL,
    search_method  ENUM('grid','random','bayesian') NOT NULL DEFAULT 'random',
    param_ranges   JSON         NOT NULL,
    objective      VARCHAR(50)  NOT NULL DEFAULT 'sharpe_ratio',
    max_iterations INT          NOT NULL DEFAULT 100,
    status         ENUM('pending','running','completed','failed','cancelled') NOT NULL DEFAULT 'pending',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at   DATETIME     DEFAULT NULL,
    INDEX idx_ot_user (user_id),
    CONSTRAINT fk_ot_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS optimization_task_results (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id     INT          NOT NULL,
    params      JSON         NOT NULL,
    metrics     JSON         NOT NULL,
    rank_num    INT          DEFAULT NULL,
    CONSTRAINT fk_otr_task FOREIGN KEY (task_id) REFERENCES optimization_tasks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS indicator_configs (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(50)  NOT NULL,
    category     ENUM('trend','oscillator','volume','volatility','custom') NOT NULL,
    params_schema JSON        DEFAULT NULL,
    calc_function TEXT        DEFAULT NULL,
    user_id      INT          DEFAULT NULL,
    is_builtin   TINYINT(1)   NOT NULL DEFAULT 0,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ind_name_user (name, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Built-in indicator seed data
INSERT IGNORE INTO indicator_configs (name, category, params_schema, is_builtin) VALUES
('SMA', 'trend', '{"period": {"type": "int", "default": 20, "min": 2, "max": 500}}', 1),
('EMA', 'trend', '{"period": {"type": "int", "default": 20, "min": 2, "max": 500}}', 1),
('BOLL', 'trend', '{"period": {"type": "int", "default": 20}, "std_dev": {"type": "float", "default": 2.0}}', 1),
('SAR', 'trend', '{"af": {"type": "float", "default": 0.02}, "max_af": {"type": "float", "default": 0.2}}', 1),
('ADX', 'trend', '{"period": {"type": "int", "default": 14}}', 1),
('RSI', 'oscillator', '{"period": {"type": "int", "default": 14, "min": 2, "max": 100}}', 1),
('MACD', 'oscillator', '{"fast": {"type": "int", "default": 12}, "slow": {"type": "int", "default": 26}, "signal": {"type": "int", "default": 9}}', 1),
('KDJ', 'oscillator', '{"n": {"type": "int", "default": 9}, "m1": {"type": "int", "default": 3}, "m2": {"type": "int", "default": 3}}', 1),
('CCI', 'oscillator', '{"period": {"type": "int", "default": 14}}', 1),
('WR', 'oscillator', '{"period": {"type": "int", "default": 14}}', 1),
('ROC', 'oscillator', '{"period": {"type": "int", "default": 12}}', 1),
('OBV', 'volume', '{}', 1),
('VWAP', 'volume', '{}', 1),
('MFI', 'volume', '{"period": {"type": "int", "default": 14}}', 1),
('ATR', 'volatility', '{"period": {"type": "int", "default": 14}}', 1),
('HV', 'volatility', '{"period": {"type": "int", "default": 20}}', 1);

-- Migration 014 (quantmate part): Strategy parameter presets, Position sizing
CREATE TABLE IF NOT EXISTS parameter_presets (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    strategy_id INT NOT NULL,
    user_id     INT NOT NULL,
    name        VARCHAR(100) NOT NULL,
    description TEXT DEFAULT NULL,
    params      JSON NOT NULL,
    is_default  BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_strategy_user (strategy_id, user_id),
    UNIQUE KEY uq_strategy_user_name (strategy_id, user_id, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS position_sizing_configs (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    user_id     INT NOT NULL,
    name        VARCHAR(100) NOT NULL,
    method      VARCHAR(30) NOT NULL COMMENT 'fixed_amount/fixed_pct/kelly/equal_risk/risk_parity',
    params      JSON NOT NULL COMMENT 'Method-specific parameters',
    max_position_pct DECIMAL(5,2) DEFAULT 20.00 COMMENT 'Max single position %',
    max_total_pct    DECIMAL(5,2) DEFAULT 80.00 COMMENT 'Max total position %',
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Migration 015: P3 Feature Tables (AI, Factor Lab, Templates, Teams)
CREATE TABLE IF NOT EXISTS ai_conversations (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    user_id     INT NOT NULL,
    session_id  VARCHAR(64) NOT NULL COMMENT 'UUID grouping a conversation',
    title       VARCHAR(200) DEFAULT NULL,
    model_used  VARCHAR(50) DEFAULT NULL,
    total_tokens INT DEFAULT 0,
    status      VARCHAR(20) DEFAULT 'active' COMMENT 'active/archived',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_session (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ai_messages (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    conversation_id INT NOT NULL,
    role        VARCHAR(20) NOT NULL COMMENT 'user/assistant/system',
    content     TEXT NOT NULL,
    tokens      INT DEFAULT 0,
    metadata    JSON DEFAULT NULL COMMENT 'tool_calls, citations, etc.',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_conversation (conversation_id),
    CONSTRAINT fk_msg_conversation FOREIGN KEY (conversation_id) REFERENCES ai_conversations(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ai_model_configs (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    model_name  VARCHAR(50) NOT NULL UNIQUE,
    provider    VARCHAR(30) NOT NULL COMMENT 'openai/anthropic/local/deepseek',
    endpoint    VARCHAR(500) DEFAULT NULL,
    api_key_ref VARCHAR(100) DEFAULT NULL COMMENT 'Reference to secrets manager key',
    temperature DECIMAL(3,2) DEFAULT 0.70,
    max_tokens  INT DEFAULT 4096,
    enabled     BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS factor_definitions (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    user_id     INT NOT NULL,
    name        VARCHAR(100) NOT NULL,
    category    VARCHAR(50) DEFAULT NULL COMMENT 'momentum/value/quality/volatility/custom',
    expression  TEXT NOT NULL COMMENT 'Factor formula or code',
    description TEXT DEFAULT NULL,
    params      JSON DEFAULT NULL COMMENT 'Configurable parameters',
    status      VARCHAR(20) DEFAULT 'draft' COMMENT 'draft/backtesting/validated/published',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS factor_evaluations (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    factor_id   INT NOT NULL,
    start_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    ic_mean     DECIMAL(8,6) DEFAULT NULL COMMENT 'Information Coefficient mean',
    ic_ir       DECIMAL(8,6) DEFAULT NULL COMMENT 'IC Information Ratio',
    turnover    DECIMAL(8,6) DEFAULT NULL,
    long_ret    DECIMAL(10,6) DEFAULT NULL COMMENT 'Long portfolio return',
    short_ret   DECIMAL(10,6) DEFAULT NULL COMMENT 'Short portfolio return',
    long_short_ret DECIMAL(10,6) DEFAULT NULL,
    metrics     JSON DEFAULT NULL COMMENT 'Full evaluation metrics',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_factor (factor_id),
    CONSTRAINT fk_eval_factor FOREIGN KEY (factor_id) REFERENCES factor_definitions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS strategy_templates (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    author_id   INT NOT NULL,
    name        VARCHAR(100) NOT NULL,
    category    VARCHAR(50) DEFAULT NULL COMMENT 'trend/mean_revert/arbitrage/ml/multi_factor',
    template_type ENUM('standalone','component','composite') NOT NULL DEFAULT 'standalone'
                  COMMENT 'standalone = VNPy CTA, component = pipeline layer, composite = pipeline blueprint',
    layer       ENUM('universe','trading','risk') DEFAULT NULL
                  COMMENT 'Applicable only when template_type = component',
    sub_type    VARCHAR(50) DEFAULT NULL
                  COMMENT 'Finer subclass label for component templates',
    composite_config JSON DEFAULT NULL
                  COMMENT 'Composite-only: bindings blueprint referencing sub_type values',
    description TEXT DEFAULT NULL,
    code        MEDIUMTEXT NOT NULL,
    params_schema JSON DEFAULT NULL COMMENT 'JSON Schema for parameters',
    default_params JSON DEFAULT NULL,
    version     VARCHAR(20) DEFAULT '1.0.0',
    visibility  VARCHAR(20) DEFAULT 'private' COMMENT 'private/team/public',
    downloads   INT DEFAULT 0,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_author (author_id),
    INDEX idx_visibility (visibility),
    INDEX idx_category (category),
    INDEX idx_template_type (template_type),
    INDEX idx_layer (layer)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS strategy_shares (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    strategy_id INT NOT NULL,
    shared_by   INT NOT NULL,
    shared_with_user_id INT DEFAULT NULL,
    shared_with_team_id INT DEFAULT NULL,
    permission  VARCHAR(20) DEFAULT 'view' COMMENT 'view/clone/edit',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_strategy (strategy_id),
    INDEX idx_shared_user (shared_with_user_id),
    INDEX idx_shared_team (shared_with_team_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS team_workspaces (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    description TEXT DEFAULT NULL,
    owner_id    INT NOT NULL,
    avatar_url  VARCHAR(500) DEFAULT NULL,
    max_members INT DEFAULT 10,
    status      VARCHAR(20) DEFAULT 'active' COMMENT 'active/archived',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_owner (owner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS workspace_members (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    workspace_id INT NOT NULL,
    user_id     INT NOT NULL,
    role        VARCHAR(20) DEFAULT 'member' COMMENT 'owner/admin/member/viewer',
    joined_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_workspace_user (workspace_id, user_id),
    INDEX idx_user (user_id),
    CONSTRAINT fk_member_workspace FOREIGN KEY (workspace_id) REFERENCES team_workspaces(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS strategy_comments (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    template_id INT NOT NULL,
    user_id     INT NOT NULL,
    content     TEXT NOT NULL,
    parent_id   INT DEFAULT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_template (template_id),
    INDEX idx_parent (parent_id),
    CONSTRAINT fk_comment_template FOREIGN KEY (template_id) REFERENCES strategy_templates(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS strategy_ratings (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    template_id INT NOT NULL,
    user_id     INT NOT NULL,
    rating      TINYINT NOT NULL COMMENT '1-5',
    review      TEXT DEFAULT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_template_user (template_id, user_id),
    INDEX idx_template (template_id),
    CONSTRAINT fk_rating_template FOREIGN KEY (template_id) REFERENCES strategy_templates(id) ON DELETE CASCADE,
    CONSTRAINT chk_rating CHECK (rating BETWEEN 1 AND 5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Migration 016 (quantmate part): Multi-market exchange reference
CREATE TABLE IF NOT EXISTS market_exchanges (
    code          VARCHAR(20)     NOT NULL    COMMENT 'SSE, SZSE, HKEX, NYSE, NASDAQ',
    name          VARCHAR(100)    NOT NULL,
    country       VARCHAR(5)      NOT NULL    COMMENT 'CN, HK, US',
    timezone      VARCHAR(50)     NOT NULL    COMMENT 'e.g. Asia/Shanghai',
    currency      VARCHAR(5)      NOT NULL    COMMENT 'CNY, HKD, USD',
    open_time     TIME            DEFAULT NULL,
    close_time    TIME            DEFAULT NULL,
    enabled       TINYINT(1)      DEFAULT 1,
    updated_at    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO market_exchanges (code, name, country, timezone, currency, open_time, close_time) VALUES
('SSE',    'Shanghai Stock Exchange',    'CN', 'Asia/Shanghai',    'CNY', '09:30:00', '15:00:00'),
('SZSE',   'Shenzhen Stock Exchange',    'CN', 'Asia/Shanghai',    'CNY', '09:30:00', '15:00:00'),
('BSE',    'Beijing Stock Exchange',     'CN', 'Asia/Shanghai',    'CNY', '09:30:00', '15:00:00'),
('HKEX',   'Hong Kong Stock Exchange',   'HK', 'Asia/Hong_Kong',   'HKD', '09:30:00', '16:00:00'),
('NYSE',   'New York Stock Exchange',    'US', 'America/New_York', 'USD', '09:30:00', '16:00:00'),
('NASDAQ', 'NASDAQ',                     'US', 'America/New_York', 'USD', '09:30:00', '16:00:00');

-- =============================================================================
-- SECTION 3: Indexes for query optimization
-- =============================================================================
CREATE INDEX idx_backtest_user_date ON backtest_history(user_id, created_at DESC);
CREATE INDEX idx_strategies_user_active ON strategies(user_id, is_active);

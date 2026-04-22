-- =============================================================================
-- QuantMate Backend API Database (Merged Init)
-- Database: quantmate - stores user accounts, strategies, backtest results,
--           portfolios, alerts, system config, AI, and more
-- Includes folded migration state through 038
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
-- Migration 017: Paper trading deployments table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `paper_deployments` (
    `id`              INT AUTO_INCREMENT PRIMARY KEY,
    `user_id`         INT NOT NULL,
    `strategy_id`     INT NOT NULL,
    `strategy_name`   VARCHAR(255) NOT NULL,
    `vt_symbol`       VARCHAR(50) NOT NULL,
    `parameters`      JSON DEFAULT NULL,
    `status`          ENUM('running', 'stopped', 'error') NOT NULL DEFAULT 'running',
    `started_at`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `stopped_at`      TIMESTAMP NULL DEFAULT NULL,
    `created_at`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_paper_deploy_user`     (`user_id`),
    INDEX `idx_paper_deploy_strategy` (`strategy_id`),
    INDEX `idx_paper_deploy_status`   (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
-- SECTION 2: Migration Tables (000-038)
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
    ('018', '018_datasync_multi_source_refactor.sql'),
    ('019', '019_factor_screening.sql'),
    ('020', '020_strategy_factor_bridge.sql'),
    ('021', '021_create_paper_accounts.sql'),
    ('022', '022_create_composite_strategy_tables.sql'),
    ('023', '023_create_composite_backtests_table.sql'),
    ('024', '024_seed_strategy_templates.sql'),
    ('025', '025_template_source_tracking.sql'),
    ('026', '026_create_rbac_tables.sql'),
    ('027', '027_align_rbac_role_permissions_with_spec.sql'),
    ('028', '028_fix_watchlist_sort_order_and_kyc_submissions.sql'),
    ('029', '029_p0_workflow_traceability.sql'),
    ('030', '030_tushare_full_catalog.sql'),
    ('031', '031_fix_tushare_catalog_guardrails.sql'),
    ('032', '032_rdagent_tables.sql'),
    ('033', '033_align_tushare_extended_schema.sql'),
    ('034', '034_normalize_datasync_catalog_aliases.sql'),
    ('035', '035_fix_tushare_suspend_and_audit.sql'),
    ('036', '036_normalize_tushare_permissions.sql'),
    ('037', '037_permission_points_as_int.sql'),
    ('038', '038_refresh_tushare_catalog_from_csv.sql');

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
('tushare', 'stock_basic',     '股票基本信息',  1, 'A股基本资料',                   NULL,      'tushare', 'stock_basic',     0, 10),
('tushare', 'stock_company',   '公司基本面',    1, '上市公司基本信息',                '0',       'tushare', 'stock_company',   1, 15),
('tushare', 'new_share',       'IPO新股列表',   0, '新股上市列表数据',                NULL,      'tushare', 'new_share',      1, 104),
('tushare', 'stock_daily',     '日线行情',      1, 'A股日K线',                      NULL,      'tushare', 'stock_daily',     1, 20),
('tushare', 'adj_factor',      '复权因子',      1, '前复权因子',                     NULL,      'tushare', 'adj_factor',      0, 30),
('tushare', 'trade_cal',       '交易日历',      1, '交易所交易日历',                  NULL,      'tushare', 'trade_cal',       0, 5),
('tushare', 'suspend_d',       '停复牌当日信息',0, '停复牌当日状态数据',              NULL,      'tushare', 'suspend_d',       1, 23),
('tushare', 'report_rc',       '盈利预测数据',  0, '券商盈利预测数据',                NULL,      'tushare', 'report_rc',      1, 315),
-- Weekly/Monthly/Index items
('tushare', 'stock_weekly',    '周线行情',      1, 'A股周K线',                      NULL,      'tushare', 'stock_weekly',    0, 25),
('tushare', 'stock_monthly',   '月线行情',      1, 'A股月K线',                      NULL,      'tushare', 'stock_monthly',   0, 26),
('tushare', 'index_weekly',    '指数周线',      1, '指数周K线',                      NULL,      'tushare', 'index_weekly',    0, 28),
('tushare', 'index_daily',     '指数日线',      1, '指数日K线',                      NULL,      'tushare', 'index_daily',     0, 27),
-- Extended tushare items
('tushare', 'moneyflow',       '资金流向',      1, '个股资金流向数据',                '0',       'tushare', 'moneyflow',       0, 25),
('tushare', 'stk_limit',       '涨跌停统计',    1, '涨跌停数据(封单/强度)',           '0',       'tushare', 'stk_limit',       0, 70),
('tushare', 'margin_detail',   '融资融券',      1, '融资融券余额明细',                '0',       'tushare', 'margin_detail',   0, 80),
('tushare', 'block_trade',     '大宗交易',      1, '大宗交易数据',                   '0',       'tushare', 'block_trade',     0, 90),
('tushare', 'fina_indicator',  '财务指标',      1, '主要财务指标数据',                '0',       'tushare', 'fina_indicator',  0, 55),
('tushare', 'dividend',        '分红送股',      0, '分红送股数据(需高级权限)',         '1',       'tushare', 'dividend',        0, 50),
('tushare', 'income',          '利润表',        0, '利润表数据(需高级权限)',           '1',       'tushare', 'income',          0, 56),
('tushare', 'top10_holders',   '十大股东',      0, '十大股东数据(需高级权限)',         '1',       'tushare', 'top10_holders',   0, 57),
('tushare', 'us_basic',        '美股列表',      0, '美股基础信息',                    NULL,      'tushare', 'us_basic',        1, 800),
('tushare', 'us_daily',        '美股日线',      0, '美股日线行情',                    NULL,      'tushare', 'us_daily',        1, 801),
('tushare', 'shibor_lpr',      'LPR贷款基础利率',0,'LPR贷款基础利率',                NULL,      'tushare', 'shibor_lpr',      1, 840),
-- AkShare items
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

INSERT INTO system_configs (config_key, config_value, category, description, is_user_overridable) VALUES
('datasync.sync_hour', '2', 'datasync', 'Daily sync schedule hour in 24-hour local time.', 0),
('datasync.sync_minute', '0', 'datasync', 'Daily sync schedule minute in local time.', 0),
('datasync.timezone', 'Asia/Shanghai', 'datasync', 'Timezone used by scheduler windows and sync dashboards.', 0),
('datasync.batch_size', '100', 'datasync', 'Default batch size for interface-level sync fetches.', 0),
('datasync.max_retries', '3', 'datasync', 'Maximum retry count for sync engine retries.', 0),
('datasync.sync_parallel_workers', '4', 'datasync', 'Worker count for regular daily sync dispatch.', 0),
('datasync.backfill_workers', '10', 'datasync', 'Worker count for backfill retry execution.', 0),
('datasync.backfill_idle_interval_hours', '4', 'datasync', 'Sleep hours between empty backfill loop passes.', 0),
('datasync.backfill_lock_retry_seconds', '60', 'datasync', 'Retry interval when another backfill worker owns the DB lock.', 0),
('datasync.backfill_lock_stale_hours', '6', 'datasync', 'Running status stale threshold for backfill lock recovery.', 0),
('datasync.sync_status_running_stale_hours', '6', 'datasync', 'Threshold used to reopen stuck running sync records.', 0),
('datasync.sync_init.batch_size', '500', 'datasync', 'Trade-date batch size used when filling missing sync status rows.', 0),
('datasync.dashboard.cache_ttl_seconds', '30', 'datasync', 'Seconds to cache sync dashboard snapshots.', 0),
('datasync.backfill_job_timeout_seconds', '3600', 'datasync', 'RQ timeout for queued backfill jobs.', 0),
('api.manual_datasync_job_timeout_seconds', '1800', 'datasync', 'RQ timeout for manually triggered daily sync jobs.', 0),
('datasync.akshare.calls_per_min.default', '30', 'datasync', 'Default rate limit for AkShare sync APIs without endpoint-specific overrides.', 0),
('datasync.akshare.max_retries', '3', 'datasync', 'Maximum retry count for AkShare fetch failures.', 0),
('datasync.akshare.backoff_base_seconds', '5', 'datasync', 'Base backoff interval used after AkShare throttling or transient failures.', 0),
('datasync.source_concurrency.tushare', '3', 'datasync', 'Maximum concurrent Tushare daily sync calls.', 0),
('datasync.backfill_source_concurrency.tushare', '3', 'datasync', 'Maximum concurrent Tushare backfill calls.', 0),
('datasync.tushare.token_points', '0', 'datasync', 'Capability hint used to decide which registry-backed Tushare APIs are sync-supported.', 0),
('datasync.tushare.granted_api_names', '', 'datasync', 'Comma-separated list of token-granted Tushare API names.', 0),
('backtest.default_capital', '100000.0', 'backtest', 'Default capital used by single backtest forms and tasks.', 0),
('backtest.default_rate', '0.0001', 'backtest', 'Default commission rate used by backtests.', 0),
('backtest.default_slippage', '0.0', 'backtest', 'Default slippage applied in backtests.', 0),
('backtest.default_size', '1', 'backtest', 'Default lot size used by backtests.', 0),
('backtest.default_pricetick', '0.01', 'backtest', 'Default price tick used by backtests.', 0),
('backtest.default_benchmark', '399300.SZ', 'backtest', 'Default benchmark symbol used by backtests and optimizations.', 0),
('backtest.job_timeout_seconds', '3600', 'backtest', 'RQ timeout for single backtest jobs.', 0),
('backtest.bulk_job_timeout_seconds', '7200', 'backtest', 'RQ timeout for bulk backtest jobs.', 0),
('backtest.optimization_job_timeout_seconds', '14400', 'backtest', 'RQ timeout for optimization jobs dispatched from backtest flows.', 0),
('backtest.result_ttl_seconds', '604800', 'backtest', 'Seconds to keep stored backtest results and artifacts.', 0),
('backtest.optimization.task_job_timeout_seconds', '14400', 'backtest', 'RQ timeout for optimization task jobs.', 0),
('backtest.optimization.task_result_ttl_seconds', '259200', 'backtest', 'Seconds to keep optimization task results.', 0),
('backtest.optimization.max_workers', '4', 'backtest', 'Maximum worker count used by optimization tasks.', 0),
('backtest.optimization.default_rate', '0.0003', 'backtest', 'Default commission rate used by optimization tasks.', 0),
('backtest.optimization.default_slippage', '0.0001', 'backtest', 'Default slippage used by optimization tasks.', 0),
('trading.default_slippage', '0.001', 'trading', 'Default live-trading slippage assumption for order matching.', 0),
('market.calendar.trade_days_cache_ttl_seconds', '300', 'market', 'Seconds to cache trade-day responses.', 0),
('market.calendar.events_cache_ttl_seconds', '300', 'market', 'Seconds to cache market event responses.', 0),
('market.calendar.max_events_per_type', '30', 'market', 'Maximum number of events returned for each event category.', 0),
('market.sentiment.cache_ttl_seconds', '60', 'market', 'Seconds to cache market sentiment snapshots.', 0),
('realtime_quote.bulk_cache_ttl_seconds', '60', 'market', 'Seconds to reuse cached bulk quote responses.', 0),
('realtime_quote.akshare_timeout_seconds', '15.0', 'market', 'Timeout in seconds for AkShare realtime quote requests.', 0),
('realtime_quote.tencent_timeout_seconds', '8.0', 'market', 'Timeout in seconds for Tencent quote requests.', 0),
('realtime_quote.tencent_retries', '2', 'market', 'Retry count for Tencent quote timeouts or connection errors.', 0),
('realtime_quote.tencent_backoff_seconds', '1.0', 'market', 'Base backoff in seconds between Tencent quote retries.', 0),
('worker.default_queue_names', 'backtest,optimization,default,low', 'worker', 'Comma-separated queue names used when a worker starts without explicit queue arguments.', 0),
('worker.queue_timeout.high', '600', 'worker', 'Default RQ timeout for the high priority queue.', 0),
('worker.queue_timeout.default', '1800', 'worker', 'Default RQ timeout for the standard queue.', 0),
('worker.queue_timeout.low', '3600', 'worker', 'Default RQ timeout for the low priority queue.', 0),
('worker.queue_timeout.backtest', '3600', 'worker', 'Default RQ timeout for the backtest queue.', 0),
('worker.queue_timeout.optimization', '7200', 'worker', 'Default RQ timeout for the optimization queue.', 0),
('worker.queue_timeout.rdagent', '14400', 'worker', 'Default RQ timeout for the RD-Agent queue.', 0),
('jobs.storage_ttl_seconds', '604800', 'jobs', 'Seconds to keep persisted job metadata in Redis.', 0),
('rdagent.request_timeout_seconds', '14400.0', 'rdagent', 'Timeout in seconds for RD-Agent sidecar HTTP requests.', 0),
('cli.redis_socket_timeout_seconds', '2.0', 'jobs', 'Socket timeout in seconds for CLI Redis health checks.', 0)
ON DUPLICATE KEY UPDATE
    config_value = VALUES(config_value),
    category = VALUES(category),
    description = VALUES(description),
    is_user_overridable = VALUES(is_user_overridable);

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


-- =============================================================================
-- SECTION 4: Folded Migrations (019-038)
-- =============================================================================

-- Migration 019: Factor screening tables + factor_evaluations.ic_std column
-- Supports: Factor mining, batch screening, correlation deduplication

-- ─── 1. Add ic_std to factor_evaluations (quantmate DB) ─────────────

SET @dbname = 'quantmate';
SET @tablename = 'factor_evaluations';

-- ic_std column
SET @colname = 'ic_std';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` DECIMAL(8,6) DEFAULT NULL COMMENT \'IC standard deviation\' AFTER `ic_mean`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;


-- ─── 2. Factor screening results (quantmate DB) ─────────────────────

CREATE TABLE IF NOT EXISTS `quantmate`.`factor_screening_results` (
  `id`            INT AUTO_INCREMENT PRIMARY KEY,
  `user_id`       INT NOT NULL,
  `run_label`     VARCHAR(200) NOT NULL COMMENT 'User-defined label for this screening run',
  `config`        JSON DEFAULT NULL COMMENT 'Screening configuration (thresholds, date range, etc.)',
  `result_count`  INT DEFAULT 0,
  `status`        VARCHAR(20) DEFAULT 'completed',
  `created_at`    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user` (`user_id`),
  INDEX `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Factor screening run metadata';


CREATE TABLE IF NOT EXISTS `quantmate`.`factor_screening_details` (
  `id`              INT AUTO_INCREMENT PRIMARY KEY,
  `run_id`          INT NOT NULL,
  `rank_order`      INT NOT NULL COMMENT 'Rank within screening run (1 = best)',
  `factor_name`     VARCHAR(200) NOT NULL,
  `factor_set`      VARCHAR(30) DEFAULT 'custom' COMMENT 'Alpha158, Alpha360, custom',
  `expression`      TEXT DEFAULT NULL COMMENT 'Expression (for custom factors)',
  `ic_mean`         DECIMAL(10,6) DEFAULT NULL,
  `ic_std`          DECIMAL(10,6) DEFAULT NULL,
  `ic_ir`           DECIMAL(10,4) DEFAULT NULL,
  `turnover`        DECIMAL(10,4) DEFAULT NULL,
  `long_ret`        DECIMAL(10,6) DEFAULT NULL,
  `short_ret`       DECIMAL(10,6) DEFAULT NULL,
  `long_short_ret`  DECIMAL(10,6) DEFAULT NULL,
  INDEX `idx_run` (`run_id`),
  INDEX `idx_rank` (`run_id`, `rank_order`),
  CONSTRAINT `fk_screening_run` FOREIGN KEY (`run_id`)
    REFERENCES `quantmate`.`factor_screening_results`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Individual factor results within a screening run';

-- Migration 020: Strategy ↔ Factor bridge table
-- Links strategies to their constituent factors (for multi-factor strategies)

CREATE TABLE IF NOT EXISTS `quantmate`.`strategy_factors` (
  `id`            INT AUTO_INCREMENT PRIMARY KEY,
  `strategy_id`   INT NOT NULL,
  `factor_id`     INT DEFAULT NULL COMMENT 'FK to factor_definitions (NULL if using raw factor_name)',
  `factor_name`   VARCHAR(200) NOT NULL COMMENT 'Factor name (from definitions or Qlib built-in)',
  `factor_set`    VARCHAR(30) DEFAULT 'custom' COMMENT 'Alpha158/Alpha360/custom',
  `weight`        DECIMAL(8,4) DEFAULT 1.0 COMMENT 'Factor weight in composite signal',
  `direction`     TINYINT DEFAULT 1 COMMENT '1=long higher values, -1=short higher values',
  `created_at`    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_strategy` (`strategy_id`),
  INDEX `idx_factor` (`factor_id`),
  CONSTRAINT `fk_sf_strategy` FOREIGN KEY (`strategy_id`)
    REFERENCES `quantmate`.`strategies`(`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_sf_factor` FOREIGN KEY (`factor_id`)
    REFERENCES `quantmate`.`factor_definitions`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Strategy-Factor relationship for multi-factor strategies';

-- Paper Trading: Paper Accounts, Account Snapshots, Paper Signals
-- and extensions to existing tables for paper trading support.

-- Paper accounts — independent virtual capital accounts for simulation
CREATE TABLE IF NOT EXISTS `quantmate`.`paper_accounts` (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    name            VARCHAR(100) NOT NULL,
    initial_capital DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    balance         DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    frozen          DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    market_value    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    total_pnl       DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    currency        ENUM('CNY','HKD','USD') NOT NULL DEFAULT 'CNY',
    market          ENUM('CN','HK','US') NOT NULL DEFAULT 'CN',
    status          ENUM('active','closed') NOT NULL DEFAULT 'active',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_pa_user   (user_id),
    INDEX idx_pa_status (status),
    CONSTRAINT fk_pa_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Daily equity snapshots for paper accounts (used for equity curve)
CREATE TABLE IF NOT EXISTS `quantmate`.`paper_account_snapshots` (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    account_id      INT          NOT NULL,
    snapshot_date   DATE         NOT NULL,
    balance         DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    market_value    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    total_equity    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    daily_pnl       DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_snap_acct_date (account_id, snapshot_date),
    CONSTRAINT fk_snap_acct FOREIGN KEY (account_id) REFERENCES `quantmate`.`paper_accounts`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Paper signals — strategy signal notifications for semi-auto mode
CREATE TABLE IF NOT EXISTS `quantmate`.`paper_signals` (
    id               INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id          INT          NOT NULL,
    paper_account_id INT          NOT NULL,
    deployment_id    INT          NOT NULL,
    symbol           VARCHAR(20)  NOT NULL,
    direction        ENUM('buy','sell') NOT NULL,
    quantity         INT          NOT NULL,
    suggested_price  DECIMAL(10,4) DEFAULT NULL,
    reason           TEXT         DEFAULT NULL,
    status           ENUM('pending','confirmed','rejected','expired') NOT NULL DEFAULT 'pending',
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_at     DATETIME     DEFAULT NULL,
    INDEX idx_ps_user   (user_id),
    INDEX idx_ps_acct   (paper_account_id),
    INDEX idx_ps_status (status),
    CONSTRAINT fk_ps_user FOREIGN KEY (user_id)          REFERENCES `quantmate`.`users`(id)             ON DELETE CASCADE,
    CONSTRAINT fk_ps_acct FOREIGN KEY (paper_account_id) REFERENCES `quantmate`.`paper_accounts`(id)    ON DELETE CASCADE,
    CONSTRAINT fk_ps_depl FOREIGN KEY (deployment_id)    REFERENCES `quantmate`.`paper_deployments`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Extend existing orders table with paper_account_id and buy_date for T+1
SET @has_col_orders_paper_account_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'orders' AND column_name = 'paper_account_id'
);
SET @sql_add_orders_paper_account_id := IF(
    @has_col_orders_paper_account_id = 0,
    'ALTER TABLE `quantmate`.`orders` ADD COLUMN `paper_account_id` INT DEFAULT NULL AFTER `mode`',
    'SELECT 1'
);
PREPARE stmt_add_orders_paper_account_id FROM @sql_add_orders_paper_account_id;
EXECUTE stmt_add_orders_paper_account_id;
DEALLOCATE PREPARE stmt_add_orders_paper_account_id;

SET @has_col_orders_buy_date := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'orders' AND column_name = 'buy_date'
);
SET @sql_add_orders_buy_date := IF(
    @has_col_orders_buy_date = 0,
    'ALTER TABLE `quantmate`.`orders` ADD COLUMN `buy_date` DATE DEFAULT NULL AFTER `paper_account_id`',
    'SELECT 1'
);
PREPARE stmt_add_orders_buy_date FROM @sql_add_orders_buy_date;
EXECUTE stmt_add_orders_buy_date;
DEALLOCATE PREPARE stmt_add_orders_buy_date;

-- Extend paper_deployments with paper_account_id and execution_mode
SET @has_col_deploy_paper_account_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'paper_deployments' AND column_name = 'paper_account_id'
);
SET @sql_add_deploy_paper_account_id := IF(
    @has_col_deploy_paper_account_id = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `paper_account_id` INT DEFAULT NULL AFTER `user_id`',
    'SELECT 1'
);
PREPARE stmt_add_deploy_paper_account_id FROM @sql_add_deploy_paper_account_id;
EXECUTE stmt_add_deploy_paper_account_id;
DEALLOCATE PREPARE stmt_add_deploy_paper_account_id;

SET @has_col_deploy_execution_mode := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'paper_deployments' AND column_name = 'execution_mode'
);
SET @sql_add_deploy_execution_mode := IF(
    @has_col_deploy_execution_mode = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `execution_mode` ENUM(''auto'',''semi_auto'') NOT NULL DEFAULT ''auto'' AFTER `status`',
    'SELECT 1'
);
PREPARE stmt_add_deploy_execution_mode FROM @sql_add_deploy_execution_mode;
EXECUTE stmt_add_deploy_execution_mode;
DEALLOCATE PREPARE stmt_add_deploy_execution_mode;

-- Composite Strategy System: strategy_components, composite_strategies, composite_component_bindings
-- Implements the three-layer architecture: Universe → Trading → Risk

-- Strategy components — individual reusable building blocks for composite strategies
CREATE TABLE IF NOT EXISTS `quantmate`.`strategy_components` (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    name            VARCHAR(100) NOT NULL,
    layer           ENUM('universe','trading','risk') NOT NULL,
    sub_type        VARCHAR(50)  NOT NULL COMMENT 'Sub-type: factor/technical/trend/grid/mean_revert/stop_loss/position_sizing/var_constraint/...',
    description     TEXT,
    code            MEDIUMTEXT   DEFAULT NULL COMMENT 'Executable Python source (mainly for trading layer)',
    config          JSON         DEFAULT NULL COMMENT 'Declarative config (factor DSL / rule params / filter criteria)',
    parameters      JSON         DEFAULT NULL,
    version         INT          NOT NULL DEFAULT 1,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_sc_user_layer (user_id, layer),
    INDEX idx_sc_sub_type (sub_type),
    CONSTRAINT fk_sc_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Composite strategies — composed from multiple strategy components
CREATE TABLE IF NOT EXISTS `quantmate`.`composite_strategies` (
    id                INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id           INT          NOT NULL,
    name              VARCHAR(100) NOT NULL,
    description       TEXT,
    portfolio_config  JSON         DEFAULT NULL COMMENT 'Weight allocation / rebalance config',
    market_constraints JSON        DEFAULT NULL COMMENT 'T+1 / price limit / lot size constraints',
    execution_mode    ENUM('backtest','paper','live') NOT NULL DEFAULT 'backtest',
    is_active         BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_cs_user (user_id),
    CONSTRAINT fk_cs_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Composite-component bindings — many-to-many with ordering and weights
CREATE TABLE IF NOT EXISTS `quantmate`.`composite_component_bindings` (
    id                     INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
    composite_strategy_id  INT            NOT NULL,
    component_id           INT            NOT NULL,
    layer                  ENUM('universe','trading','risk') NOT NULL,
    ordinal                INT            NOT NULL DEFAULT 0   COMMENT 'Order within same layer (priority / chain sequence)',
    weight                 DECIMAL(5,4)   NOT NULL DEFAULT 1.0 COMMENT 'Weight for multi-component voting / merging',
    config_override        JSON           DEFAULT NULL          COMMENT 'Per-binding parameter overrides',
    FOREIGN KEY (composite_strategy_id) REFERENCES `quantmate`.`composite_strategies`(id) ON DELETE CASCADE,
    FOREIGN KEY (component_id) REFERENCES `quantmate`.`strategy_components`(id) ON DELETE RESTRICT,
    INDEX idx_ccb_composite_layer (composite_strategy_id, layer),
    UNIQUE KEY uq_ccb_composite_component (composite_strategy_id, component_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Composite backtest results table
-- Stores results of composite strategy backtests (daily-frequency, multi-symbol)

CREATE TABLE IF NOT EXISTS `quantmate`.`composite_backtests` (
  id                    INT AUTO_INCREMENT PRIMARY KEY,
  job_id                VARCHAR(64) NOT NULL UNIQUE,
  user_id               INT NOT NULL,
  composite_strategy_id INT NOT NULL,
  start_date            DATE NOT NULL,
  end_date              DATE NOT NULL,
  initial_capital       DECIMAL(15,2) DEFAULT 1000000.00,
  benchmark             VARCHAR(30) DEFAULT '000300.SH',
  status                ENUM('queued','running','completed','failed') DEFAULT 'queued',
  result                JSON COMMENT 'Performance metrics + equity curve + trade log',
  attribution           JSON COMMENT 'Layer attribution analysis',
  error_message         TEXT,
  started_at            TIMESTAMP NULL,
  completed_at          TIMESTAMP NULL,
  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user (user_id),
  INDEX idx_composite (composite_strategy_id),
  INDEX idx_status (status),
  FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE,
  FOREIGN KEY (composite_strategy_id) REFERENCES `quantmate`.`composite_strategies`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 024: Add template_type system + seed 32 strategy templates
-- Adds template_type/layer/sub_type/composite_config columns to strategy_templates
-- and seeds 5 standalone + 23 component + 4 composite templates.
-- NOTE: Run scripts/seed_template_code.py after this migration to populate code fields.

SET NAMES 'utf8mb4';
SET CHARACTER SET utf8mb4;

-- ─────────────────────────────────────────────────────────
-- 2.1  ALTER TABLE — add new columns
-- ─────────────────────────────────────────────────────────

SET @ddl = (
  SELECT CONCAT(
    'ALTER TABLE `quantmate`.`strategy_templates`',
    IF(
      EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND column_name = 'template_type'
      ),
      '',
      ' ADD COLUMN template_type ENUM(''standalone'',''component'',''composite'') NOT NULL DEFAULT ''standalone'' COMMENT ''standalone = VNPy CTA, component = pipeline layer, composite = pipeline blueprint'' AFTER category'
    ),
    IF(
      EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND column_name = 'layer'
      ),
      '',
      IF(
        EXISTS(
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND column_name = 'template_type'
        ),
        ', ADD COLUMN layer ENUM(''universe'',''trading'',''risk'') DEFAULT NULL COMMENT ''Applicable only when template_type = component'' AFTER template_type',
        ', ADD COLUMN layer ENUM(''universe'',''trading'',''risk'') DEFAULT NULL COMMENT ''Applicable only when template_type = component'' AFTER category'
      )
    ),
    IF(
      EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND column_name = 'sub_type'
      ),
      '',
      IF(
        EXISTS(
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND column_name = 'layer'
        ),
        ', ADD COLUMN sub_type VARCHAR(50) DEFAULT NULL COMMENT ''Finer subclass label for component templates'' AFTER layer',
        ', ADD COLUMN sub_type VARCHAR(50) DEFAULT NULL COMMENT ''Finer subclass label for component templates'' AFTER category'
      )
    ),
    IF(
      EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND column_name = 'composite_config'
      ),
      '',
      IF(
        EXISTS(
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND column_name = 'sub_type'
        ),
        ', ADD COLUMN composite_config JSON DEFAULT NULL COMMENT ''Composite-only: bindings blueprint referencing sub_type values'' AFTER sub_type',
        ', ADD COLUMN composite_config JSON DEFAULT NULL COMMENT ''Composite-only: bindings blueprint referencing sub_type values'' AFTER category'
      )
    )
  )
);
SET @ddl = IF(@ddl = 'ALTER TABLE `quantmate`.`strategy_templates`', 'SELECT 1', @ddl);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_template_type = (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.statistics
      WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND index_name = 'idx_template_type'
    ),
    'SELECT 1',
    'ALTER TABLE `quantmate`.`strategy_templates` ADD INDEX idx_template_type (template_type)'
  )
);
PREPARE stmt FROM @idx_template_type;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_layer = (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.statistics
      WHERE table_schema = 'quantmate' AND table_name = 'strategy_templates' AND index_name = 'idx_layer'
    ),
    'SELECT 1',
    'ALTER TABLE `quantmate`.`strategy_templates` ADD INDEX idx_layer (layer)'
  )
);
PREPARE stmt FROM @idx_layer;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ─────────────────────────────────────────────────────────
-- 2.2  Seed 5 standalone templates (VNPy CtaTemplate)
-- ─────────────────────────────────────────────────────────

INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, 'MACD交叉策略', 'cta', 'standalone', NULL, NULL,
   'MACD histogram flip + zero-line cross entry/exit',
   '"""MACD-based CTA strategy for testing.

Simple MACD strategy:
- Uses ArrayManager.macd to compute MACD, signal and hist
- Long entry: macd > signal and macd_hist > 0
- Long exit: macd < signal
- Short entry: macd < signal and macd_hist < 0
- Short exit: macd > signal

This is intended for local testing/backtests only.
"""

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)
from vnpy.trader.constant import Direction, Offset


class MACDStrategy(CtaTemplate):
    """A small MACD crossover strategy for testing/backtests."""

    author = "QuantMate"

    # strategy parameters
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    fixed_size: int = 1

    parameters = ["fast_period", "slow_period", "signal_period", "fixed_size"]
    variables = ["macd", "macd_signal", "macd_hist"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        # state
        self.macd = 0.0
        self.macd_signal = 0.0
        self.macd_hist = 0.0

    def on_init(self):
        self.write_log("MACDStrategy initializing")
        self.load_bar(50)

    def on_start(self):
        self.write_log("MACDStrategy started")

    def on_stop(self):
        self.write_log("MACDStrategy stopped")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        # cancel outstanding orders first
        self.cancel_all()

        # update array manager
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # compute MACD via ArrayManager utility (returns macd, signal, hist)
        macd_v, signal_v, hist_v = self.am.macd(self.fast_period, self.slow_period, self.signal_period)
        self.macd = macd_v
        self.macd_signal = signal_v
        self.macd_hist = hist_v

        # Trading logic (long-only; use available capital to size positions)
        # No position -> consider long entry only
        if self.pos == 0:
            if self.macd > self.macd_signal and self.macd_hist > 0:
                # Calculate number of contracts to buy using full capital
                try:
                    size_per_contract = int(self.get_size() or 1)
                except Exception:
                    size_per_contract = 1

                try:
                    engine_capital = float(getattr(self.cta_engine, "capital", 0) or 0)
                except Exception:
                    engine_capital = 0.0

                # Guard against zero price or capital
                price = float(bar.close_price or 0.0)
                volume = 1
                if price > 0 and size_per_contract > 0 and engine_capital > 0:
                    # number of contracts = floor(capital / (price * size_per_contract))
                    volume = max(1, int(engine_capital / (price * size_per_contract)))

                # Place a marketable buy using a small price adjustment
                self.buy(price * 1.01, volume)
                self.write_log(f"Long entry signal: buying {volume} @ {price:.4f}")

        # Have long -> exit when macd crosses below signal
        elif self.pos > 0:
            if self.macd < self.macd_signal:
                self.sell(bar.close_price * 0.99, abs(self.pos))
                self.write_log("Exit long (macd < signal)")

        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def on_trade(self, trade: TradeData):
        # Log trade with clearer intent (entry vs exit, long vs short)
        try:
            if trade.direction == Direction.LONG and getattr(trade, "offset", None) == Offset.OPEN:
                kind = "Long entry"
            elif trade.direction == Direction.SHORT and getattr(trade, "offset", None) == Offset.CLOSE:
                kind = "Exit long"
            elif trade.direction == Direction.SHORT and getattr(trade, "offset", None) == Offset.OPEN:
                kind = "Short entry"
            elif trade.direction == Direction.LONG and getattr(trade, "offset", None) == Offset.CLOSE:
                kind = "Exit short"
            else:
                kind = f"{trade.direction} {getattr(trade, ''offset'', '''')}"
        except Exception:
            kind = str(trade.direction)

        self.write_log(f"Trade executed: {kind} {trade.volume} @ {trade.price}")

    def on_stop_order(self, stop_order: StopOrder):
        pass
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":12},"slow_period":{"type":"integer","default":26},"signal_period":{"type":"integer","default":9},"fixed_size":{"type":"integer","default":1}}}',
   '{"fast_period":12,"slow_period":26,"signal_period":9,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '三均线趋势策略', 'cta', 'standalone', NULL, NULL,
   'Triple moving-average trend-following strategy',
   '"""
三均线策略 (Triple Moving Average Strategy)
集成固定止损和移动止损

策略逻辑：
1. 使用三条不同周期的移动平均线（快线、中线、慢线）
2. 开多条件：快线 > 中线 > 慢线（多头排列）
3. 开空条件：快线 < 中线 < 慢线（空头排列）
4. 平多条件：快线下穿中线 或 触发止损
5. 平空条件：快线上穿中线 或 触发止损
6. 集成基于标准差的固定止损和移动止损

Author: QuantMate
"""

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    Direction,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)

from app.strategies.stop_loss import StopLossManager


class TripleMAStrategy(CtaTemplate):
    """
    三均线策略

    参数说明：
    - fast_window: 快线周期（默认5）
    - mid_window: 中线周期（默认10）
    - slow_window: 慢线周期（默认20）
    - fixed_size: 固定交易手数
    - stop_loss_window: 计算标准差的回看周期
    - fixed_stop_multiplier: 固定止损标准差倍数
    - trailing_stop_multiplier: 移动止损标准差倍数
    """

    author = "QuantMate"

    # 策略参数
    fast_window: int = 5  # 快线周期
    mid_window: int = 10  # 中线周期
    slow_window: int = 20  # 慢线周期
    fixed_size: int = 1  # 固定交易手数

    # 止损参数
    stop_loss_window: int = 10  # 计算标准差的回看周期
    fixed_stop_multiplier: float = 1.0  # 固定止损：1倍标准差
    trailing_stop_multiplier: float = 2.0  # 移动止损：2倍标准差
    use_stop_loss: bool = True  # 是否启用止损

    # 策略变量
    fast_ma: float = 0  # 快线值
    mid_ma: float = 0  # 中线值
    slow_ma: float = 0  # 慢线值

    ma_trend: int = 0  # 均线趋势：1=多头排列，-1=空头排列，0=无趋势

    # 止损状态变量
    entry_price: float = 0  # 入场价格
    fixed_stop: float = 0  # 固定止损价
    trailing_stop: float = 0  # 移动止损价

    parameters = [
        "fast_window",
        "mid_window",
        "slow_window",
        "fixed_size",
        "stop_loss_window",
        "fixed_stop_multiplier",
        "trailing_stop_multiplier",
        "use_stop_loss",
    ]

    variables = ["fast_ma", "mid_ma", "slow_ma", "ma_trend", "entry_price", "fixed_stop", "trailing_stop"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """初始化策略"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        # 初始化止损管理器
        self.stop_loss_manager = StopLossManager(
            fixed_std_multiplier=self.fixed_stop_multiplier,
            trailing_std_multiplier=self.trailing_stop_multiplier,
            lookback_period=self.stop_loss_window,
            use_fixed_stop=self.use_stop_loss,
            use_trailing_stop=self.use_stop_loss,
        )

    def on_init(self):
        """策略初始化"""
        self.write_log("三均线策略初始化")

        # 加载历史数据
        self.load_bar(max(self.slow_window, self.stop_loss_window) + 10)

    def on_start(self):
        """策略启动"""
        self.write_log("三均线策略启动")

    def on_stop(self):
        """策略停止"""
        self.write_log("三均线策略停止")

    def on_tick(self, tick: TickData):
        """Tick数据更新"""
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        """K线数据更新"""
        # 取消所有挂单
        self.cancel_all()

        # 更新K线到数组管理器
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # 计算三条均线
        self.fast_ma = self.am.sma(self.fast_window)
        self.mid_ma = self.am.sma(self.mid_window)
        self.slow_ma = self.am.sma(self.slow_window)

        # 判断均线趋势（仅用于多头信号，策略为多头优先）
        if self.fast_ma > self.mid_ma > self.slow_ma:
            self.ma_trend = 1  # 多头排列
        else:
            self.ma_trend = 0  # 无明显多头趋势

        # 获取最近收盘价用于止损计算
        recent_closes = list(self.am.close[-self.stop_loss_window :])
        vt_symbol = f"{bar.symbol}.{bar.exchange.value}"

        # 如果有持仓，更新移动止损
        if self.pos != 0 and self.use_stop_loss:
            self.stop_loss_manager.update_trailing_stop(vt_symbol, bar.close_price, recent_closes)
            state = self.stop_loss_manager.get_state(vt_symbol)

            if state:
                self.fixed_stop = state.fixed_stop_price
                self.trailing_stop = state.trailing_stop_price

                # 检查是否触发止损
                if self.stop_loss_manager.should_stop_loss(vt_symbol, bar.close_price):
                    reason = self.stop_loss_manager.get_stop_reason(vt_symbol, bar.close_price)
                    active_stop = state.get_active_stop_price()

                    if self.pos > 0:
                        self.write_log(
                            f"多头止损触发 ({reason}): 当前价={bar.close_price:.2f}, 止损价={active_stop:.2f}"
                        )
                        self.sell(bar.close_price * 0.99, abs(self.pos))
                    elif self.pos < 0:
                        self.write_log(
                            f"空头止损触发 ({reason}): 当前价={bar.close_price:.2f}, 止损价={active_stop:.2f}"
                        )
                        self.cover(bar.close_price * 1.01, abs(self.pos))

                    return

        # 无持仓时的开仓逻辑
        if self.pos == 0:
            # 清除止损状态
            self.stop_loss_manager.remove_position(vt_symbol)
            self.entry_price = 0
            self.fixed_stop = 0
            self.trailing_stop = 0

            # 多头排列，开多（策略为多头方向，仅建多仓）
            if self.ma_trend == 1:
                self.buy(bar.close_price * 1.01, self.fixed_size)
                self.write_log(
                    f"多头开仓信号: 快线={self.fast_ma:.2f} > 中线={self.mid_ma:.2f} > 慢线={self.slow_ma:.2f}"
                )

        # 持有多头时
        elif self.pos > 0:
            # 快线下穿中线，平多
            if self.fast_ma < self.mid_ma:
                self.sell(bar.close_price * 0.99, abs(self.pos))
                self.write_log("多头平仓信号: 快线下穿中线")

        # 不支持空头仓位（策略为多头-only）

        # 更新UI
        self.put_event()

    def on_order(self, order: OrderData):
        """委托回报"""
        pass

    def on_trade(self, trade: TradeData):
        """成交回报"""
        vt_symbol = f"{trade.symbol}.{trade.exchange.value}"
        recent_closes = list(self.am.close[-self.stop_loss_window :])

        # 开仓成交时设置止损
        if self.use_stop_loss and len(recent_closes) >= 2:
            if trade.direction == Direction.LONG:
                # 开多仓
                self.entry_price = trade.price
                state = self.stop_loss_manager.set_entry(vt_symbol, trade.price, recent_closes, is_long=True)
                self.fixed_stop = state.fixed_stop_price
                self.trailing_stop = state.trailing_stop_price

                self.write_log(
                    f"开多仓成交: 价格={trade.price:.2f}, "
                    f"固定止损={self.fixed_stop:.2f}, "
                    f"移动止损={self.trailing_stop:.2f}"
                )

            elif trade.direction == Direction.SHORT:
                # 开空仓
                self.entry_price = trade.price
                state = self.stop_loss_manager.set_entry(vt_symbol, trade.price, recent_closes, is_long=False)
                self.fixed_stop = state.fixed_stop_price
                self.trailing_stop = state.trailing_stop_price

                self.write_log(
                    f"开空仓成交: 价格={trade.price:.2f}, "
                    f"固定止损={self.fixed_stop:.2f}, "
                    f"移动止损={self.trailing_stop:.2f}"
                )

        # 平仓成交时清除止损
        if (trade.direction == Direction.LONG and trade.offset.value != "OPEN") or (
            trade.direction == Direction.SHORT and trade.offset.value != "OPEN"
        ):
            self.stop_loss_manager.remove_position(vt_symbol)
            self.write_log(f"平仓成交: 价格={trade.price:.2f}")

    def on_stop_order(self, stop_order: StopOrder):
        """停止单回报"""
        pass
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":5},"mid_period":{"type":"integer","default":10},"slow_period":{"type":"integer","default":20},"fixed_size":{"type":"integer","default":1}}}',
   '{"fast_period":5,"mid_period":10,"slow_period":20,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '海龟交易策略', 'cta', 'standalone', NULL, NULL,
   'Turtle-trading Donchian breakout with ATR stops',
   'from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    Direction,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)

# 导入通用止损模块
from app.strategies.stop_loss import StopLossManager


class TurtleTradingStrategy(CtaTemplate):
    """Turtle Trading strategy implemented for vn.py CTA framework.

    Features:
    - 20-day Donchian breakout entry (configurable)
    - 10-day Donchian exit (configurable)
    - ATR-based volatility measure for stops and pyramiding
    - Up to 4 units pyramiding
    - 集成固定止损和移动止损策略
    """

    author = "QuantMate"

    entry_window: int = 20
    exit_window: int = 10
    atr_window: int = 20
    fixed_size: int = 1

    # 止损参数
    stop_loss_window: int = 10  # 计算标准差的回看周期
    fixed_stop_multiplier: float = 2.0  # 固定止损：2倍标准差
    trailing_stop_multiplier: float = 1.0  # 移动止损：1倍标准差
    use_std_stop_loss: bool = True  # 是否使用基于标准差的止损

    entry_up: float = 0
    entry_down: float = 0
    exit_up: float = 0
    exit_down: float = 0
    atr_value: float = 0
    long_entry: float = 0
    long_stop: float = 0

    # 止损状态变量
    std_fixed_stop: float = 0  # 基于标准差的固定止损价
    std_trailing_stop: float = 0  # 基于标准差的移动止损价

    parameters = [
        "entry_window",
        "exit_window",
        "atr_window",
        "fixed_size",
        "stop_loss_window",
        "fixed_stop_multiplier",
        "trailing_stop_multiplier",
        "use_std_stop_loss",
    ]
    variables = ["entry_up", "entry_down", "exit_up", "exit_down", "atr_value", "std_fixed_stop", "std_trailing_stop"]

    def on_init(self) -> None:
        """Initialize strategy: set up bar generator and array manager."""
        self.write_log("TurtleTradingStrategy initialized")

        self.bg: BarGenerator = BarGenerator(self.on_bar)
        self.am: ArrayManager = ArrayManager()

        # 初始化止损管理器
        self.stop_loss_manager = StopLossManager(
            fixed_std_multiplier=self.fixed_stop_multiplier,
            trailing_std_multiplier=self.trailing_stop_multiplier,
            lookback_period=self.stop_loss_window,
            use_fixed_stop=self.use_std_stop_loss,
            use_trailing_stop=self.use_std_stop_loss,
        )

        # load historical bars for indicators
        self.load_bar(max(self.entry_window, self.atr_window, self.stop_loss_window) + 5)

    def on_start(self) -> None:
        self.write_log("TurtleTradingStrategy started")

    def on_stop(self) -> None:
        self.write_log("TurtleTradingStrategy stopped")

    def on_tick(self, tick: TickData) -> None:
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData) -> None:
        # Called when a new bar is ready
        self.cancel_all()

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # Only calculate entry channel when no position
        if not self.pos:
            self.entry_up, self.entry_down = self.am.donchian(self.entry_window)

        self.exit_up, self.exit_down = self.am.donchian(self.exit_window)

        # 获取最近N天收盘价用于计算标准差止损
        recent_closes = list(self.am.close[-self.stop_loss_window :])
        vt_symbol = f"{bar.symbol}.{bar.exchange.value}"

        if not self.pos:
            self.atr_value = self.am.atr(self.atr_window)

            # reset trackers
            self.long_entry = 0
            self.long_stop = 0
            self.std_fixed_stop = 0
            self.std_trailing_stop = 0

            # 清除止损状态
            self.stop_loss_manager.remove_position(vt_symbol)

            # send entry orders at breakout levels (marketable stop orders)
            self.send_buy_orders(self.entry_up)

        elif self.pos > 0:
            # 更新移动止损
            if self.use_std_stop_loss:
                self.stop_loss_manager.update_trailing_stop(vt_symbol, bar.close_price, recent_closes)
                state = self.stop_loss_manager.get_state(vt_symbol)
                if state:
                    self.std_fixed_stop = state.fixed_stop_price
                    self.std_trailing_stop = state.trailing_stop_price

                    # 检查是否触发止损
                    if self.stop_loss_manager.should_stop_loss(vt_symbol, bar.close_price):
                        reason = self.stop_loss_manager.get_stop_reason(vt_symbol, bar.close_price)
                        self.write_log(f"触发止损: {reason}, 止损价={state.get_active_stop_price():.2f}")
                        self.sell(bar.close_price * 0.99, abs(self.pos), False)
                        return

            # if long, maintain pyramiding and set protective exit
            self.send_buy_orders(self.entry_up)

            # 综合ATR止损和标准差止损，取较高者
            if self.use_std_stop_loss and self.std_trailing_stop > 0:
                sell_price: float = max(self.long_stop, self.exit_down, self.std_trailing_stop)
            else:
                sell_price: float = max(self.long_stop, self.exit_down)
            # use stop_order True to indicate stop style
            self.sell(sell_price, abs(self.pos), True)

        # no short positions supported

        self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        # Update stops and last entry price on fills
        vt_symbol = f"{trade.symbol}.{trade.exchange.value}"

        # 获取最近收盘价用于计算止损
        recent_closes = list(self.am.close[-self.stop_loss_window :])

        if trade.direction == Direction.LONG:
            self.long_entry = trade.price
            self.long_stop = self.long_entry - 2 * self.atr_value

            # 设置基于标准差的止损
            if self.use_std_stop_loss and len(recent_closes) >= 2:
                state = self.stop_loss_manager.set_entry(vt_symbol, trade.price, recent_closes, is_long=True)
                self.std_fixed_stop = state.fixed_stop_price
                self.std_trailing_stop = state.trailing_stop_price
                self.write_log(
                    f"开多仓: 入场价={trade.price:.2f}, "
                    f"固定止损={self.std_fixed_stop:.2f}, "
                    f"移动止损={self.std_trailing_stop:.2f}"
                )
        # short fills ignored (strategy is long-only)

    def on_order(self, order: OrderData) -> None:
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        pass

    def send_buy_orders(self, price: float) -> None:
        """Place up to 4 pyramiding buy orders using ATR offsets."""
        t: float = self.pos / self.fixed_size

        if t < 1:
            self.buy(price, self.fixed_size, True)

        if t < 2:
            self.buy(price + self.atr_value * 0.5, self.fixed_size, True)

        if t < 3:
            self.buy(price + self.atr_value, self.fixed_size, True)

        if t < 4:
            self.buy(price + self.atr_value * 1.5, self.fixed_size, True)

    def send_short_orders(self, price: float) -> None:
        """Place up to 4 pyramiding short orders using ATR offsets."""
        t: float = self.pos / self.fixed_size
        # short orders removed for long-only strategy
        return
',
   '{"type":"object","properties":{"entry_window":{"type":"integer","default":20},"exit_window":{"type":"integer","default":10},"atr_window":{"type":"integer","default":20},"fixed_size":{"type":"integer","default":1}}}',
   '{"entry_window":20,"exit_window":10,"atr_window":20,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '布林带突破策略', 'cta', 'standalone', NULL, NULL,
   'Bollinger Band breakout with bandwidth confirmation',
   '"""Bollinger Breakout — standalone VNPy CTA strategy.

Enters when price closes outside Bollinger Bands with expanding
bandwidth and exits on mean reversion back to the middle band.
"""

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)


class BollingerBreakoutStrategy(CtaTemplate):
    """Bollinger Band breakout with bandwidth confirmation."""

    author = "QuantMate"

    # parameters
    bb_period: int = 20
    bb_std: float = 2.0
    bandwidth_threshold: float = 0.04
    fixed_size: int = 1

    parameters = ["bb_period", "bb_std", "bandwidth_threshold", "fixed_size"]
    variables = ["bb_upper", "bb_lower", "bb_mid", "bandwidth"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        self.bb_upper = 0.0
        self.bb_lower = 0.0
        self.bb_mid = 0.0
        self.bandwidth = 0.0

    def on_init(self):
        self.write_log("BollingerBreakoutStrategy initializing")
        self.load_bar(self.bb_period + 20)

    def on_start(self):
        self.write_log("BollingerBreakoutStrategy started")

    def on_stop(self):
        self.write_log("BollingerBreakoutStrategy stopped")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.cancel_all()

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.bb_upper, self.bb_mid, self.bb_lower = self.am.boll(
            self.bb_period, self.bb_std
        )
        if self.bb_mid > 0:
            self.bandwidth = (self.bb_upper - self.bb_lower) / self.bb_mid
        else:
            self.bandwidth = 0

        close = bar.close_price
        wide_enough = self.bandwidth >= self.bandwidth_threshold

        if self.pos == 0:
            if close > self.bb_upper and wide_enough:
                self.buy(close * 1.01, self.fixed_size)
                self.write_log(
                    f"Long breakout: close={close:.2f} > upper={self.bb_upper:.2f}"
                )
            elif close < self.bb_lower and wide_enough:
                self.short(close * 0.99, self.fixed_size)
                self.write_log(
                    f"Short breakout: close={close:.2f} < lower={self.bb_lower:.2f}"
                )
        elif self.pos > 0:
            if close <= self.bb_mid:
                self.sell(close * 0.99, abs(self.pos))
                self.write_log("Exit long — reverted to mid band")
        elif self.pos < 0:
            if close >= self.bb_mid:
                self.cover(close * 1.01, abs(self.pos))
                self.write_log("Exit short — reverted to mid band")

        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def on_trade(self, trade: TradeData):
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        pass
',
   '{"type":"object","properties":{"bb_period":{"type":"integer","default":20},"bb_std":{"type":"number","default":2.0},"bandwidth_threshold":{"type":"number","default":0.04},"fixed_size":{"type":"integer","default":1}}}',
   '{"bb_period":20,"bb_std":2.0,"bandwidth_threshold":0.04,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, 'ATR通道策略', 'cta', 'standalone', NULL, NULL,
   'ATR channel breakout / reversion strategy',
   '"""ATR Channel — standalone VNPy CTA strategy.

Uses ATR‑based channels around a moving average for trend‑following
entries and volatility‑scaled exits.
"""

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)


class ATRChannelStrategy(CtaTemplate):
    """ATR channel breakout / reversion strategy."""

    author = "QuantMate"

    # parameters
    ma_period: int = 20
    atr_period: int = 14
    atr_multiplier: float = 2.0
    fixed_size: int = 1

    parameters = ["ma_period", "atr_period", "atr_multiplier", "fixed_size"]
    variables = ["ma_value", "atr_value", "upper_band", "lower_band"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        self.ma_value = 0.0
        self.atr_value = 0.0
        self.upper_band = 0.0
        self.lower_band = 0.0

    def on_init(self):
        self.write_log("ATRChannelStrategy initializing")
        self.load_bar(max(self.ma_period, self.atr_period) + 20)

    def on_start(self):
        self.write_log("ATRChannelStrategy started")

    def on_stop(self):
        self.write_log("ATRChannelStrategy stopped")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.cancel_all()

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.ma_value = self.am.sma(self.ma_period)
        self.atr_value = self.am.atr(self.atr_period)
        self.upper_band = self.ma_value + self.atr_multiplier * self.atr_value
        self.lower_band = self.ma_value - self.atr_multiplier * self.atr_value

        close = bar.close_price

        if self.pos == 0:
            if close > self.upper_band:
                self.buy(close * 1.01, self.fixed_size)
                self.write_log(
                    f"Long: close={close:.2f} above ATR upper={self.upper_band:.2f}"
                )
            elif close < self.lower_band:
                self.short(close * 0.99, self.fixed_size)
                self.write_log(
                    f"Short: close={close:.2f} below ATR lower={self.lower_band:.2f}"
                )
        elif self.pos > 0:
            # exit when price falls back below MA
            if close < self.ma_value:
                self.sell(close * 0.99, abs(self.pos))
                self.write_log("Exit long — price below MA")
        elif self.pos < 0:
            if close > self.ma_value:
                self.cover(close * 1.01, abs(self.pos))
                self.write_log("Exit short — price above MA")

        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def on_trade(self, trade: TradeData):
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        pass
',
   '{"type":"object","properties":{"ma_period":{"type":"integer","default":20},"atr_period":{"type":"integer","default":14},"atr_multiplier":{"type":"number","default":2.0},"fixed_size":{"type":"integer","default":1}}}',
   '{"ma_period":20,"atr_period":14,"atr_multiplier":2.0,"fixed_size":1}',
   '1.0.0', 'public');

-- ─────────────────────────────────────────────────────────
-- 2.3  Seed 23 component templates
-- ─────────────────────────────────────────────────────────

-- Universe components (6)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '市值过滤', 'cta', 'component', 'universe', 'market_cap_filter',
   'Filter by market cap range',
   '"""Market Cap Filter — universe component.

Filters the tradable universe by market capitalisation range.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols whose market cap falls within [min, max]."""
    cfg = config or {}
    min_cap = cfg.get("min_market_cap", 5_000_000_000)
    max_cap = cfg.get("max_market_cap", 1_000_000_000_000)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        cap = bar.get("market_cap", 0)
        if min_cap <= cap <= max_cap:
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"min_market_cap":{"type":"number","default":5000000000},"max_market_cap":{"type":"number","default":1000000000000}}}',
   '{"min_market_cap":5000000000,"max_market_cap":1000000000000}',
   '1.0.0', 'public'),

  (1, '流动性过滤', 'cta', 'component', 'universe', 'liquidity_filter',
   'Filter by average volume and turnover rate',
   '"""Liquidity Filter — universe component.

Filters based on average daily volume and turnover rate.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols meeting minimum liquidity thresholds."""
    cfg = config or {}
    min_volume = cfg.get("min_avg_volume", 1_000_000)
    min_turnover = cfg.get("min_turnover_rate", 0.005)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        vol = bar.get("avg_volume_20d", 0)
        turnover = bar.get("turnover_rate", 0)
        if vol >= min_volume and turnover >= min_turnover:
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"min_avg_volume":{"type":"number","default":1000000},"min_turnover_rate":{"type":"number","default":0.005}}}',
   '{"min_avg_volume":1000000,"min_turnover_rate":0.005}',
   '1.0.0', 'public'),

  (1, '行业轮动选股', 'cta', 'component', 'universe', 'sector_rotation',
   'Select top-momentum sectors',
   '"""Sector Rotation — universe component.

Selects stocks from the top‑performing industry sectors based on
rolling relative‑strength momentum.
"""

from typing import Any, Dict, List
from collections import defaultdict


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols belonging to the top‑N momentum sectors."""
    cfg = config or {}
    top_n = cfg.get("top_sectors", 3)
    momentum_key = cfg.get("momentum_key", "sector_momentum_20d")

    # group symbols by sector
    sectors: Dict[str, List[str]] = defaultdict(list)
    sector_scores: Dict[str, float] = {}
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        sector = bar.get("sector", "Unknown")
        sectors[sector].append(symbol)
        # take the max momentum as sector score
        score = bar.get(momentum_key, 0.0)
        sector_scores[sector] = max(sector_scores.get(sector, float("-inf")), score)

    # pick top sectors
    ranked = sorted(sector_scores, key=sector_scores.get, reverse=True)  # type: ignore[arg-type]
    top_sectors = set(ranked[:top_n])

    result: List[str] = []
    for sector in top_sectors:
        result.extend(sectors[sector])
    return result
',
   '{"type":"object","properties":{"top_sectors":{"type":"integer","default":3},"momentum_key":{"type":"string","default":"sector_momentum_20d"}}}',
   '{"top_sectors":3,"momentum_key":"sector_momentum_20d"}',
   '1.0.0', 'public'),

  (1, '指数成分股', 'cta', 'component', 'universe', 'index_constituents',
   'Filter to major index constituents (CSI 300/500)',
   '"""Index Constituents — universe component.

Selects universe from major index constituent lists
(e.g. CSI 300, CSI 500, S&P 500).
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols that belong to the configured index."""
    cfg = config or {}
    index_name = cfg.get("index", "csi300")
    index_key = f"is_{index_name}"

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        if bar.get(index_key, False):
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"index":{"type":"string","default":"csi300"}}}',
   '{"index":"csi300"}',
   '1.0.0', 'public'),

  (1, '基本面筛选', 'alpha', 'component', 'universe', 'fundamental_screen',
   'PE/PB/ROE/revenue growth screen',
   '"""Fundamental Screen — universe component.

Screens stocks by PE, PB, ROE, revenue growth and other
fundamental metrics.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols passing all fundamental filters."""
    cfg = config or {}
    max_pe = cfg.get("max_pe", 40.0)
    max_pb = cfg.get("max_pb", 8.0)
    min_roe = cfg.get("min_roe", 0.08)
    min_revenue_growth = cfg.get("min_revenue_growth", 0.0)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        pe = bar.get("pe_ratio", float("inf"))
        pb = bar.get("pb_ratio", float("inf"))
        roe = bar.get("roe", 0.0)
        rev_g = bar.get("revenue_growth_yoy", 0.0)
        if pe <= max_pe and pb <= max_pb and roe >= min_roe and rev_g >= min_revenue_growth:
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"max_pe":{"type":"number","default":40},"max_pb":{"type":"number","default":8},"min_roe":{"type":"number","default":0.08},"min_revenue_growth":{"type":"number","default":0}}}',
   '{"max_pe":40,"max_pb":8,"min_roe":0.08,"min_revenue_growth":0}',
   '1.0.0', 'public'),

  (1, 'ST/停牌过滤', 'cta', 'component', 'universe', 'st_halt_filter',
   'Exclude ST, suspended and newly-listed stocks',
   '"""ST / Halt Filter — universe component.

Excludes ST‑flagged, suspended, and newly‑listed stocks.
Essential for A‑share compliance.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols that are NOT ST, suspended, or too new."""
    cfg = config or {}
    min_list_days = cfg.get("min_list_days", 60)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        if bar.get("is_st", False):
            continue
        if bar.get("is_suspended", False):
            continue
        if bar.get("list_days", 0) < min_list_days:
            continue
        result.append(symbol)
    return result
',
   '{"type":"object","properties":{"min_list_days":{"type":"integer","default":60}}}',
   '{"min_list_days":60}',
   '1.0.0', 'public');

-- Trading components (11)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '双均线交叉信号', 'cta', 'component', 'trading', 'dual_ma_signal',
   'Fast/slow MA crossover signals',
   '"""Dual MA Signal — trading component.

Generates buy/sell signals based on fast/slow moving average crossovers.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return a list of signal dicts {symbol, direction, strength, reason}."""
    cfg = config or {}
    fast_period = cfg.get("fast_period", 5)
    slow_period = cfg.get("slow_period", 20)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        fast_ma = bar.get(f"ma_{fast_period}", 0)
        slow_ma = bar.get(f"ma_{slow_period}", 0)
        close = bar.get("close", 0)

        if fast_ma == 0 or slow_ma == 0 or close == 0:
            continue

        if fast_ma > slow_ma and close > fast_ma:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min((fast_ma - slow_ma) / slow_ma * 10, 1.0),
                    "reason": f"MA{fast_period} crossed above MA{slow_period}",
                }
            )
        elif fast_ma < slow_ma and close < fast_ma:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min((slow_ma - fast_ma) / slow_ma * 10, 1.0),
                    "reason": f"MA{fast_period} crossed below MA{slow_period}",
                }
            )
    return signals
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":5},"slow_period":{"type":"integer","default":20}}}',
   '{"fast_period":5,"slow_period":20}',
   '1.0.0', 'public'),

  (1, '唐奇安突破信号', 'cta', 'component', 'trading', 'donchian_breakout',
   'Donchian channel breakout entry/exit',
   '"""Donchian Breakout — trading component.

Generates signals when price breaks above/below the Donchian channel.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return breakout signals based on Donchian channels."""
    cfg = config or {}
    entry_period = cfg.get("entry_period", 20)
    exit_period = cfg.get("exit_period", 10)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        high = bar.get("close", 0)
        upper = bar.get(f"donchian_upper_{entry_period}", 0)
        lower = bar.get(f"donchian_lower_{entry_period}", 0)
        exit_lower = bar.get(f"donchian_lower_{exit_period}", 0)

        held = symbol in positions

        if high >= upper and upper > 0 and not held:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": 0.8,
                    "reason": f"Breakout above {entry_period}‑day high",
                }
            )
        elif high <= exit_lower and exit_lower > 0 and held:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "close",
                    "strength": 0.9,
                    "reason": f"Broke below {exit_period}‑day low — exit",
                }
            )
    return signals
',
   '{"type":"object","properties":{"entry_period":{"type":"integer","default":20},"exit_period":{"type":"integer","default":10}}}',
   '{"entry_period":20,"exit_period":10}',
   '1.0.0', 'public'),

  (1, 'MACD信号', 'cta', 'component', 'trading', 'macd_signal',
   'MACD histogram flip + zero-line cross',
   '"""MACD Signal — trading component.

Generates signals from MACD histogram flips and zero‑line crosses.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return MACD‑based signals."""
    cfg = config or {}
    fast = cfg.get("fast_period", 12)
    slow = cfg.get("slow_period", 26)
    signal_period = cfg.get("signal_period", 9)
    _ = (fast, slow, signal_period)  # used to select the right pre-computed field

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        macd_val = bar.get("macd", 0)
        signal_val = bar.get("macd_signal", 0)
        hist = bar.get("macd_hist", 0)
        prev_hist = bar.get("macd_hist_prev", 0)

        # histogram flip
        if prev_hist <= 0 < hist:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(hist) * 5, 1.0),
                    "reason": "MACD histogram flipped positive",
                }
            )
        elif prev_hist >= 0 > hist:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(hist) * 5, 1.0),
                    "reason": "MACD histogram flipped negative",
                }
            )
        # zero‑line cross
        elif macd_val > 0 and signal_val < 0:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": 0.6,
                    "reason": "MACD crossed zero line upward",
                }
            )
    return signals
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":12},"slow_period":{"type":"integer","default":26},"signal_period":{"type":"integer","default":9}}}',
   '{"fast_period":12,"slow_period":26,"signal_period":9}',
   '1.0.0', 'public'),

  (1, '布林带回归信号', 'cta', 'component', 'trading', 'bollinger_reversion',
   'Mean-reversion at Bollinger extremes',
   '"""Bollinger Reversion — trading component.

Mean‑reversion signals triggered when price touches or exceeds
Bollinger Bands.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return reversion signals at Bollinger extremes."""
    cfg = config or {}
    bb_period = cfg.get("bb_period", 20)
    bb_std = cfg.get("bb_std", 2.0)
    _ = bb_period  # field pre‑computed

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        upper = bar.get("bb_upper", 0)
        lower = bar.get("bb_lower", 0)
        mid = bar.get("bb_mid", 0)

        if close == 0 or mid == 0:
            continue

        pct_b = (close - lower) / (upper - lower) if upper != lower else 0.5

        if close <= lower:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(1.0, (1 - pct_b)),
                    "reason": f"Price touched lower BB ({bb_std}σ)",
                }
            )
        elif close >= upper:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(1.0, pct_b),
                    "reason": f"Price touched upper BB ({bb_std}σ)",
                }
            )
    return signals
',
   '{"type":"object","properties":{"bb_period":{"type":"integer","default":20},"bb_std":{"type":"number","default":2.0}}}',
   '{"bb_period":20,"bb_std":2.0}',
   '1.0.0', 'public'),

  (1, '多因子Alpha信号', 'alpha', 'component', 'trading', 'multi_factor_alpha',
   'Value + momentum + quality composite z-score',
   '"""Multi‑Factor Alpha — trading component.

Combines value, momentum, and quality z‑scores into a composite
alpha signal.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return alpha signals sorted by composite z‑score."""
    cfg = config or {}
    w_value = cfg.get("weight_value", 0.4)
    w_momentum = cfg.get("weight_momentum", 0.3)
    w_quality = cfg.get("weight_quality", 0.3)
    top_k = cfg.get("top_k", 10)
    threshold = cfg.get("alpha_threshold", 0.5)

    scored: List[tuple[str, float]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        z_val = bar.get("z_value", 0)
        z_mom = bar.get("z_momentum", 0)
        z_qual = bar.get("z_quality", 0)
        composite = w_value * z_val + w_momentum * z_mom + w_quality * z_qual
        scored.append((symbol, composite))

    scored.sort(key=lambda x: x[1], reverse=True)

    signals: List[Dict[str, Any]] = []
    for symbol, score in scored[:top_k]:
        if score < threshold:
            break
        signals.append(
            {
                "symbol": symbol,
                "direction": "long",
                "strength": min(score / 3.0, 1.0),
                "reason": f"Multi‑factor alpha z={score:.2f}",
            }
        )
    return signals
',
   '{"type":"object","properties":{"weight_value":{"type":"number","default":0.4},"weight_momentum":{"type":"number","default":0.3},"weight_quality":{"type":"number","default":0.3},"top_k":{"type":"integer","default":10},"alpha_threshold":{"type":"number","default":0.5}}}',
   '{"weight_value":0.4,"weight_momentum":0.3,"weight_quality":0.3,"top_k":10,"alpha_threshold":0.5}',
   '1.0.0', 'public'),

  (1, '动量信号', 'alpha', 'component', 'trading', 'momentum_signal',
   'Cross-sectional momentum long/short',
   '"""Momentum Signal — trading component.

Cross‑sectional momentum: go long stocks with strongest N‑day
returns, short the weakest.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return momentum‑ranked long/short signals."""
    cfg = config or {}
    lookback = cfg.get("momentum_days", 20)
    top_pct = cfg.get("long_pct", 0.1)
    bottom_pct = cfg.get("short_pct", 0.1)

    returns: List[tuple[str, float]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        ret = bar.get(f"return_{lookback}d", 0)
        returns.append((symbol, ret))

    returns.sort(key=lambda x: x[1], reverse=True)
    n = len(returns)
    long_n = max(1, int(n * top_pct))
    short_n = max(1, int(n * bottom_pct))

    signals: List[Dict[str, Any]] = []
    for symbol, ret in returns[:long_n]:
        signals.append(
            {
                "symbol": symbol,
                "direction": "long",
                "strength": min(abs(ret) * 5, 1.0),
                "reason": f"{lookback}d momentum top decile ({ret:+.2%})",
            }
        )
    for symbol, ret in returns[-short_n:]:
        signals.append(
            {
                "symbol": symbol,
                "direction": "short",
                "strength": min(abs(ret) * 5, 1.0),
                "reason": f"{lookback}d momentum bottom decile ({ret:+.2%})",
            }
        )
    return signals
',
   '{"type":"object","properties":{"momentum_days":{"type":"integer","default":20},"long_pct":{"type":"number","default":0.1},"short_pct":{"type":"number","default":0.1}}}',
   '{"momentum_days":20,"long_pct":0.1,"short_pct":0.1}',
   '1.0.0', 'public'),

  (1, '均值回归Alpha', 'alpha', 'component', 'trading', 'mean_reversion_alpha',
   'Z-score reversion at extended deviations',
   '"""Mean Reversion Alpha — trading component.

Identifies over‑extended price deviations from a rolling mean and
generates reversion entry signals.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return mean‑reversion signals for over‑extended stocks."""
    cfg = config or {}
    lookback = cfg.get("lookback", 20)
    entry_z = cfg.get("entry_z_threshold", 2.0)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        mean = bar.get(f"ma_{lookback}", 0)
        std = bar.get(f"std_{lookback}", 0)

        if std == 0 or mean == 0:
            continue

        z = (close - mean) / std

        if z <= -entry_z:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(z) / 4.0, 1.0),
                    "reason": f"Price {z:.1f}σ below mean — reversion long",
                }
            )
        elif z >= entry_z:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(z) / 4.0, 1.0),
                    "reason": f"Price {z:.1f}σ above mean — reversion short",
                }
            )
    return signals
',
   '{"type":"object","properties":{"lookback":{"type":"integer","default":20},"entry_z_threshold":{"type":"number","default":2.0}}}',
   '{"lookback":20,"entry_z_threshold":2.0}',
   '1.0.0', 'public'),

  (1, '固定网格信号', 'grid', 'component', 'trading', 'fixed_grid',
   'Fixed-percentage grid entry levels',
   '"""Fixed Grid — trading component.

Places buy/sell signals at fixed price intervals around a
configurable base price.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return grid‑level entry/exit signals."""
    cfg = config or {}
    grid_pct = cfg.get("grid_pct", 0.02)
    max_layers = cfg.get("max_layers", 5)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        base = bar.get("grid_base_price", close)

        if close == 0 or base == 0:
            continue

        deviation = (close - base) / base
        layer = int(abs(deviation) / grid_pct)

        if layer == 0 or layer > max_layers:
            continue

        direction = "long" if deviation < 0 else "short"
        signals.append(
            {
                "symbol": symbol,
                "direction": direction,
                "strength": min(layer / max_layers, 1.0),
                "reason": f"Grid layer {layer} ({deviation:+.1%} from base)",
            }
        )
    return signals
',
   '{"type":"object","properties":{"grid_pct":{"type":"number","default":0.02},"max_layers":{"type":"integer","default":5}}}',
   '{"grid_pct":0.02,"max_layers":5}',
   '1.0.0', 'public'),

  (1, '动态网格信号', 'grid', 'component', 'trading', 'dynamic_grid',
   'ATR-adaptive grid spacing',
   '"""Dynamic Grid — trading component.

Like fixed grid but adapts spacing based on ATR (Average True Range).
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return ATR‑adaptive grid signals."""
    cfg = config or {}
    atr_multiplier = cfg.get("atr_multiplier", 1.0)
    max_layers = cfg.get("max_layers", 5)
    atr_period = cfg.get("atr_period", 14)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        base = bar.get("grid_base_price", close)
        atr = bar.get(f"atr_{atr_period}", 0)

        if close == 0 or base == 0 or atr == 0:
            continue

        grid_size = atr * atr_multiplier
        deviation = close - base
        layer = int(abs(deviation) / grid_size)

        if layer == 0 or layer > max_layers:
            continue

        direction = "long" if deviation < 0 else "short"
        signals.append(
            {
                "symbol": symbol,
                "direction": direction,
                "strength": min(layer / max_layers, 1.0),
                "reason": f"Dynamic grid L{layer} (ATR={atr:.2f}, Δ={deviation:+.2f})",
            }
        )
    return signals
',
   '{"type":"object","properties":{"atr_multiplier":{"type":"number","default":1.0},"max_layers":{"type":"integer","default":5},"atr_period":{"type":"integer","default":14}}}',
   '{"atr_multiplier":1.0,"max_layers":5,"atr_period":14}',
   '1.0.0', 'public'),

  (1, '配对交易信号', 'arbitrage', 'component', 'trading', 'pair_trading_signal',
   'Co-integrated pair spread-reversion',
   '"""Pair Trading Signal — trading component.

Identifies co‑integrated pairs and generates spread‑reversion signals.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return pair spread‑reversion signals."""
    cfg = config or {}
    entry_z = cfg.get("entry_z", 2.0)
    exit_z = cfg.get("exit_z", 0.5)
    pairs = cfg.get("pairs", [])

    signals: List[Dict[str, Any]] = []
    for pair in pairs:
        leg_a = pair.get("leg_a", "")
        leg_b = pair.get("leg_b", "")
        if leg_a not in universe or leg_b not in universe:
            continue

        bar_a = market_data.get(leg_a, {})
        bar_b = market_data.get(leg_b, {})
        spread_z = bar_a.get(f"pair_z_{leg_b}", 0)

        if abs(spread_z) >= entry_z:
            # spread too wide — expect reversion
            if spread_z > 0:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "short",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — short A",
                    }
                )
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "long",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — long B",
                    }
                )
            else:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "long",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — long A",
                    }
                )
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "short",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — short B",
                    }
                )
        elif abs(spread_z) <= exit_z:
            held_a = leg_a in positions
            held_b = leg_b in positions
            if held_a:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "close",
                        "strength": 0.9,
                        "reason": f"Pair spread converged z={spread_z:.1f} — close A",
                    }
                )
            if held_b:
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "close",
                        "strength": 0.9,
                        "reason": f"Pair spread converged z={spread_z:.1f} — close B",
                    }
                )
    return signals
',
   '{"type":"object","properties":{"entry_z":{"type":"number","default":2.0},"exit_z":{"type":"number","default":0.5},"pairs":{"type":"array","items":{"type":"object","properties":{"leg_a":{"type":"string"},"leg_b":{"type":"string"}}}}}}',
   '{"entry_z":2.0,"exit_z":0.5,"pairs":[]}',
   '1.0.0', 'public'),

  (1, 'ETF套利信号', 'arbitrage', 'component', 'trading', 'etf_arbitrage',
   'ETF premium/discount arbitrage',
   '"""ETF Arbitrage — trading component.

Exploits premium/discount between an ETF and its underlying basket.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return ETF–basket arbitrage signals."""
    cfg = config or {}
    premium_threshold = cfg.get("premium_threshold", 0.005)
    discount_threshold = cfg.get("discount_threshold", -0.005)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        premium = bar.get("etf_premium", 0)

        if premium == 0:
            continue

        if premium >= premium_threshold:
            # ETF over‑priced vs basket — short ETF, long basket
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(premium) / 0.02, 1.0),
                    "reason": f"ETF premium {premium:+.2%} — arbitrage short",
                }
            )
        elif premium <= discount_threshold:
            # ETF under‑priced — long ETF, short basket
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(premium) / 0.02, 1.0),
                    "reason": f"ETF discount {premium:+.2%} — arbitrage long",
                }
            )
    return signals
',
   '{"type":"object","properties":{"premium_threshold":{"type":"number","default":0.005},"discount_threshold":{"type":"number","default":-0.005}}}',
   '{"premium_threshold":0.005,"discount_threshold":-0.005}',
   '1.0.0', 'public');

-- Risk components (6)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '等权配置', 'cta', 'component', 'risk', 'equal_weight',
   'Equal-weight capital allocation',
   '"""Equal Weight — risk component.

Allocates equal capital weight to every signal that passes through.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return sized orders with equal weight allocation."""
    cfg = config or {}
    max_positions = cfg.get("max_positions", 10)

    # filter to actionable signals only
    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    if not actionable:
        return []

    weight = 1.0 / len(actionable)
    alloc = cash * weight

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        volume = int(alloc / price / 100) * 100  # round to board lot
        if volume <= 0:
            continue
        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_positions":{"type":"integer","default":10}}}',
   '{"max_positions":10}',
   '1.0.0', 'public'),

  (1, '波动率平价', 'alpha', 'component', 'risk', 'volatility_parity',
   'Inverse-volatility position sizing',
   '"""Volatility Parity — risk component.

Sizes positions inversely proportional to each asset''s recent
volatility so that each contributes equal risk.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders sized by inverse volatility."""
    cfg = config or {}
    max_positions = cfg.get("max_positions", 10)
    vol_key = cfg.get("vol_key", "volatility_20d")
    target_vol = cfg.get("target_portfolio_vol", 0.15)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]
    if not actionable:
        return []

    # compute inverse-vol weights
    inv_vols: List[float] = []
    for sig in actionable:
        vol = sig.get(vol_key, 0.3)
        inv_vols.append(1.0 / max(vol, 0.01))
    total_inv = sum(inv_vols) or 1.0

    orders: List[Dict[str, Any]] = []
    for sig, inv_v in zip(actionable, inv_vols):
        weight = inv_v / total_inv
        alloc = cash * weight
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue
        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_positions":{"type":"integer","default":10},"target_portfolio_vol":{"type":"number","default":0.15}}}',
   '{"max_positions":10,"target_portfolio_vol":0.15}',
   '1.0.0', 'public'),

  (1, '固定止损', 'cta', 'component', 'risk', 'fixed_stop_loss',
   'Fixed percentage stop-loss with risk-per-trade sizing',
   '"""Fixed Stop Loss — risk component.

Rejects signals that have already moved beyond the stop threshold
and attaches stop‑loss prices to surviving orders.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders with fixed stop‑loss prices attached."""
    cfg = config or {}
    stop_pct = cfg.get("stop_pct", 0.05)
    max_positions = cfg.get("max_positions", 20)
    risk_per_trade = cfg.get("risk_per_trade_pct", 0.02)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        if sig["direction"] == "long":
            stop = price * (1 - stop_pct)
        else:
            stop = price * (1 + stop_pct)

        risk_per_share = abs(price - stop)
        if risk_per_share == 0:
            continue
        max_loss = cash * risk_per_trade
        volume = int(max_loss / risk_per_share / 100) * 100
        if volume <= 0:
            continue

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "stop_price": round(stop, 2),
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"stop_pct":{"type":"number","default":0.05},"risk_per_trade_pct":{"type":"number","default":0.02},"max_positions":{"type":"integer","default":20}}}',
   '{"stop_pct":0.05,"risk_per_trade_pct":0.02,"max_positions":20}',
   '1.0.0', 'public'),

  (1, '追踪止损', 'cta', 'component', 'risk', 'trailing_stop',
   'Trailing stop-loss that ratchets with price',
   '"""Trailing Stop — risk component.

Attaches trailing stop‑loss orders that ratchet with price movement.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders with trailing-stop metadata."""
    cfg = config or {}
    trail_pct = cfg.get("trail_pct", 0.03)
    max_positions = cfg.get("max_positions", 20)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        alloc = cash * alloc_pct
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue

        if sig["direction"] == "long":
            trail_stop = price * (1 - trail_pct)
        else:
            trail_stop = price * (1 + trail_pct)

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "trail_stop": round(trail_stop, 2),
                "trail_pct": trail_pct,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"trail_pct":{"type":"number","default":0.03},"max_positions":{"type":"integer","default":20},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"trail_pct":0.03,"max_positions":20,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public'),

  (1, '回撤控制', 'cta', 'component', 'risk', 'drawdown_control',
   'Throttle new entries when portfolio drawdown exceeds threshold',
   '"""Drawdown Control — risk component.

Reduces or blocks new entries when portfolio drawdown exceeds
configurable thresholds.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders throttled by current drawdown level."""
    cfg = config or {}
    max_dd = cfg.get("max_drawdown", 0.15)
    reduce_dd = cfg.get("reduce_at_drawdown", 0.10)
    scale_factor = cfg.get("reduce_scale", 0.5)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    # compute current drawdown
    peak = cfg.get("portfolio_peak", cash)
    current_value = cash + sum(
        positions.get(s, {}).get("volume", 0) * prices.get(s, 0)
        for s in positions
        if isinstance(positions.get(s), dict)
    )
    dd = (peak - current_value) / peak if peak > 0 else 0

    if dd >= max_dd:
        # drawdown too deep — reject all new entries
        return []

    scale = scale_factor if dd >= reduce_dd else 1.0

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        alloc = cash * alloc_pct * scale
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue
        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_drawdown":{"type":"number","default":0.15},"reduce_at_drawdown":{"type":"number","default":0.10},"reduce_scale":{"type":"number","default":0.5},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"max_drawdown":0.15,"reduce_at_drawdown":0.10,"reduce_scale":0.5,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public'),

  (1, '持仓限制', 'cta', 'component', 'risk', 'position_limits',
   'Per-symbol and portfolio position limit enforcement',
   '"""Position Limits — risk component.

Enforces per‑symbol and portfolio‑level position limits.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders capped by position-limit constraints."""
    cfg = config or {}
    max_single_pct = cfg.get("max_single_position_pct", 0.10)
    max_total_positions = cfg.get("max_total_positions", 20)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    current_count = len(positions)
    remaining_slots = max(0, max_total_positions - current_count)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:remaining_slots]

    portfolio_value = cash + sum(
        positions.get(s, {}).get("volume", 0) * prices.get(s, 0)
        for s in positions
        if isinstance(positions.get(s), dict)
    )
    max_single = portfolio_value * max_single_pct

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        alloc = min(cash * alloc_pct, max_single)

        # subtract existing exposure
        existing = positions.get(symbol, {})
        if isinstance(existing, dict):
            existing_value = existing.get("volume", 0) * price
            alloc = min(alloc, max_single - existing_value)

        if alloc <= 0:
            continue
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_single_position_pct":{"type":"number","default":0.10},"max_total_positions":{"type":"integer","default":20},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"max_single_position_pct":0.10,"max_total_positions":20,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public');

-- ─────────────────────────────────────────────────────────
-- 2.4  Seed 4 composite templates
-- ─────────────────────────────────────────────────────────

INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, composite_config, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, 'CTA趋势跟踪组合', 'cta', 'composite', NULL, NULL,
   '{"bindings":{"universe":["market_cap_filter","liquidity_filter","st_halt_filter"],"trading":["dual_ma_signal"],"risk":["fixed_stop_loss","drawdown_control"]}}',
   'Classic CTA trend-following composite: cap+liquidity+ST filter → dual MA signals → fixed stop + drawdown control',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, 'Alpha多因子组合', 'alpha', 'composite', NULL, NULL,
   '{"bindings":{"universe":["index_constituents","fundamental_screen","st_halt_filter"],"trading":["multi_factor_alpha"],"risk":["volatility_parity","position_limits"]}}',
   'Multi-factor alpha composite: index+fundamentals → alpha z-score → vol-parity sizing + position caps',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, '网格震荡组合', 'grid', 'composite', NULL, NULL,
   '{"bindings":{"universe":["liquidity_filter","st_halt_filter"],"trading":["dynamic_grid"],"risk":["equal_weight","trailing_stop"]}}',
   'Grid-trading composite: liquidity screen → dynamic ATR grid → equal weight + trailing stop',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, '统计套利组合', 'arbitrage', 'composite', NULL, NULL,
   '{"bindings":{"universe":["liquidity_filter","index_constituents"],"trading":["pair_trading_signal"],"risk":["equal_weight","drawdown_control"]}}',
   'Statistical arbitrage composite: liquidity+index filter → pair spread signals → equal weight + drawdown control',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public');

-- Migration 025: Add source tracking columns to strategy_templates
-- Enables distinguishing marketplace-cloned templates from user-created ones

ALTER TABLE `quantmate`.`strategy_templates`
  ADD COLUMN source_template_id INT DEFAULT NULL AFTER author_id,
  ADD COLUMN source ENUM('marketplace','personal') NOT NULL DEFAULT 'personal' AFTER source_template_id;

CREATE INDEX idx_source_template ON `quantmate`.`strategy_templates`(source_template_id);
CREATE INDEX idx_source ON `quantmate`.`strategy_templates`(source);

-- Migration 026: RBAC roles, permissions, and user-role assignments

CREATE TABLE IF NOT EXISTS `quantmate`.`roles` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(50) NOT NULL UNIQUE,
  `description` VARCHAR(255) DEFAULT NULL,
  `is_system` BOOLEAN NOT NULL DEFAULT TRUE,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `quantmate`.`permissions` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `resource` VARCHAR(50) NOT NULL,
  `action` VARCHAR(20) NOT NULL,
  `description` VARCHAR(255) DEFAULT NULL,
  `is_system` BOOLEAN NOT NULL DEFAULT TRUE,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uk_resource_action` (`resource`, `action`)
);

CREATE TABLE IF NOT EXISTS `quantmate`.`role_permissions` (
  `role_id` INT NOT NULL,
  `permission_id` INT NOT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`role_id`, `permission_id`),
  CONSTRAINT `fk_role_permissions_role` FOREIGN KEY (`role_id`) REFERENCES `quantmate`.`roles` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_role_permissions_permission` FOREIGN KEY (`permission_id`) REFERENCES `quantmate`.`permissions` (`id`) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS `quantmate`.`user_roles` (
  `user_id` INT NOT NULL,
  `role_id` INT NOT NULL,
  `assigned_by` INT DEFAULT NULL,
  `assigned_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `is_active` BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (`user_id`, `role_id`),
  KEY `idx_user_roles_user_id` (`user_id`),
  KEY `idx_user_roles_role_id` (`role_id`),
  CONSTRAINT `fk_user_roles_role` FOREIGN KEY (`role_id`) REFERENCES `quantmate`.`roles` (`id`) ON DELETE CASCADE
);

INSERT IGNORE INTO `quantmate`.`roles` (`name`, `description`, `is_system`) VALUES
  ('admin', 'Full system administrator', TRUE),
  ('trader', 'Trading operator with execution permissions', TRUE),
  ('researcher', 'Research-focused role for strategy analysis', TRUE),
  ('viewer', 'Read-only access', TRUE);

INSERT IGNORE INTO `quantmate`.`permissions` (`resource`, `action`, `description`, `is_system`) VALUES
  ('strategies', 'read', 'Read strategy records', TRUE),
  ('strategies', 'write', 'Create or update strategies', TRUE),
  ('strategies', 'manage', 'Manage all strategies', TRUE),
  ('backtests', 'read', 'Read backtest results', TRUE),
  ('backtests', 'write', 'Create or cancel backtests', TRUE),
  ('backtests', 'manage', 'Manage all backtests', TRUE),
  ('data', 'read', 'Read market and research data', TRUE),
  ('data', 'write', 'Manage data jobs and sources', TRUE),
  ('data', 'manage', 'Manage all data permissions', TRUE),
  ('portfolios', 'read', 'Read portfolio data', TRUE),
  ('portfolios', 'write', 'Manage portfolio operations', TRUE),
  ('portfolios', 'manage', 'Manage all portfolios', TRUE),
  ('alerts', 'read', 'Read alert rules and history', TRUE),
  ('alerts', 'write', 'Create or update alert rules', TRUE),
  ('alerts', 'manage', 'Manage notification channels', TRUE),
  ('trading', 'read', 'Read trading state', TRUE),
  ('trading', 'write', 'Create or cancel orders', TRUE),
  ('trading', 'manage', 'Manage all trading operations', TRUE),
  ('reports', 'read', 'Read reports', TRUE),
  ('reports', 'write', 'Create reports', TRUE),
  ('reports', 'manage', 'Manage all reports', TRUE),
  ('system', 'read', 'Read system status', TRUE),
  ('system', 'write', 'Update system settings', TRUE),
  ('system', 'manage', 'Full system management', TRUE),
  ('account', 'read', 'Read user account data', TRUE),
  ('account', 'write', 'Update user account data', TRUE),
  ('account', 'manage', 'Manage users, roles, and permissions', TRUE),
  ('templates', 'read', 'Read templates', TRUE),
  ('templates', 'write', 'Create or update templates', TRUE),
  ('templates', 'manage', 'Manage template publishing', TRUE),
  ('teams', 'read', 'Read team workspaces', TRUE),
  ('teams', 'write', 'Create or update teams', TRUE),
  ('teams', 'manage', 'Manage all teams', TRUE);

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'admin';

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'trader'
  AND (
    (p.resource IN ('strategies', 'backtests', 'data', 'portfolios', 'alerts', 'trading', 'reports', 'system', 'account', 'templates', 'teams') AND p.action = 'read')
    OR (p.resource IN ('strategies', 'backtests', 'portfolios', 'alerts', 'trading', 'templates', 'teams') AND p.action = 'write')
    OR (p.resource = 'alerts' AND p.action = 'manage')
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'researcher'
  AND (
    (p.resource IN ('strategies', 'backtests', 'data', 'portfolios', 'alerts', 'trading', 'reports', 'system', 'account', 'templates', 'teams') AND p.action = 'read')
    OR (p.resource IN ('strategies', 'backtests', 'data', 'reports', 'templates') AND p.action = 'write')
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'viewer'
  AND p.action = 'read';

INSERT INTO `quantmate`.`user_roles` (`user_id`, `role_id`, `assigned_by`, `is_active`)
SELECT u.id, r.id, NULL, TRUE
FROM `quantmate`.`users` u
JOIN `quantmate`.`roles` r ON r.name = 'admin'
WHERE u.username = 'admin'
ON DUPLICATE KEY UPDATE
  `assigned_by` = VALUES(`assigned_by`),
  `is_active` = VALUES(`is_active`);

-- Migration 027: Align system role permissions with RBAC spec v1

DELETE rp
FROM `quantmate`.`role_permissions` rp
JOIN `quantmate`.`roles` r ON r.id = rp.role_id
WHERE r.name IN ('trader', 'researcher', 'viewer');

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'trader'
  AND (
    (p.resource = 'strategies' AND p.action IN ('read', 'write'))
    OR (p.resource = 'backtests' AND p.action IN ('read', 'write'))
    OR (p.resource = 'trading' AND p.action IN ('read', 'write'))
    OR (p.resource = 'portfolios' AND p.action IN ('read', 'write'))
    OR (p.resource = 'reports' AND p.action IN ('read', 'write'))
    OR (p.resource = 'data' AND p.action = 'read')
    OR (p.resource = 'alerts' AND p.action = 'read')
    OR (p.resource = 'account' AND p.action IN ('read', 'write'))
    OR (p.resource = 'templates' AND p.action IN ('read', 'write'))
    OR (p.resource = 'teams' AND p.action IN ('read', 'write'))
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'researcher'
  AND (
    (p.resource = 'strategies' AND p.action IN ('read', 'write'))
    OR (p.resource = 'backtests' AND p.action IN ('read', 'write'))
    OR (p.resource = 'portfolios' AND p.action = 'read')
    OR (p.resource = 'reports' AND p.action = 'read')
    OR (p.resource = 'data' AND p.action = 'read')
    OR (p.resource = 'alerts' AND p.action = 'read')
    OR (p.resource = 'account' AND p.action IN ('read', 'write'))
    OR (p.resource = 'templates' AND p.action IN ('read', 'write'))
    OR (p.resource = 'teams' AND p.action = 'read')
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'viewer'
  AND (
    (p.resource = 'reports' AND p.action = 'read')
    OR (p.resource = 'data' AND p.action = 'read')
    OR (p.resource = 'alerts' AND p.action = 'read')
    OR (p.resource = 'account' AND p.action = 'read')
  );

-- P0 workflow traceability fields for backtests and paper deployments.

SET @has_backtest_source_col := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'source'
);
SET @add_backtest_source_sql := IF(
    @has_backtest_source_col = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `source` VARCHAR(50) DEFAULT NULL AFTER `strategy_version`',
    'SELECT 1'
);
PREPARE stmt_add_backtest_source FROM @add_backtest_source_sql;
EXECUTE stmt_add_backtest_source;
DEALLOCATE PREPARE stmt_add_backtest_source;

SET @has_pd_source_bt := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'source_backtest_job_id'
);
SET @add_pd_source_bt_sql := IF(
    @has_pd_source_bt = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `source_backtest_job_id` VARCHAR(36) DEFAULT NULL AFTER `execution_mode`',
    'SELECT 1'
);
PREPARE stmt_add_pd_source_bt FROM @add_pd_source_bt_sql;
EXECUTE stmt_add_pd_source_bt;
DEALLOCATE PREPARE stmt_add_pd_source_bt;

SET @has_pd_source_ver := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'source_version_id'
);
SET @add_pd_source_ver_sql := IF(
    @has_pd_source_ver = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `source_version_id` INT DEFAULT NULL AFTER `source_backtest_job_id`',
    'SELECT 1'
);
PREPARE stmt_add_pd_source_ver FROM @add_pd_source_ver_sql;
EXECUTE stmt_add_pd_source_ver;
DEALLOCATE PREPARE stmt_add_pd_source_ver;

SET @has_pd_risk_status := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'risk_check_status'
);
SET @add_pd_risk_status_sql := IF(
    @has_pd_risk_status = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `risk_check_status` VARCHAR(16) DEFAULT NULL AFTER `source_version_id`',
    'SELECT 1'
);
PREPARE stmt_add_pd_risk_status FROM @add_pd_risk_status_sql;
EXECUTE stmt_add_pd_risk_status;
DEALLOCATE PREPARE stmt_add_pd_risk_status;

SET @has_pd_risk_summary := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'risk_check_summary'
);
SET @add_pd_risk_summary_sql := IF(
    @has_pd_risk_summary = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `risk_check_summary` JSON DEFAULT NULL AFTER `risk_check_status`',
    'SELECT 1'
);
PREPARE stmt_add_pd_risk_summary FROM @add_pd_risk_summary_sql;
EXECUTE stmt_add_pd_risk_summary;
DEALLOCATE PREPARE stmt_add_pd_risk_summary;

-- Migration 030: Tushare full API catalog
-- Extends data_source_items with category/sub_category/api_name/permission_points
-- Seeds all 130+ Tushare Pro interfaces from official API catalog
-- Adds sync_status_init tracking table

-- Step 1: Add new columns to data_source_items
ALTER TABLE `quantmate`.`data_source_items`
  ADD COLUMN `category`          VARCHAR(50)  DEFAULT NULL COMMENT '数据大类: 股票数据, 指数数据, etc.' AFTER `description`,
  ADD COLUMN `sub_category`      VARCHAR(50)  DEFAULT NULL COMMENT '数据子类: 基础数据, 行情数据, etc.' AFTER `category`,
  ADD COLUMN `api_name`          VARCHAR(100) DEFAULT NULL COMMENT 'Tushare接口名: stock_basic, daily, etc.' AFTER `sub_category`,
  ADD COLUMN `permission_points` VARCHAR(50)  DEFAULT NULL COMMENT '权限积分: 120积分, 2000积分, etc.' AFTER `api_name`,
  ADD COLUMN `rate_limit_note`   VARCHAR(200) DEFAULT NULL COMMENT '限量说明' AFTER `permission_points`;

-- Add index for category-based queries
ALTER TABLE `quantmate`.`data_source_items`
  ADD INDEX `idx_category` (`category`, `sub_category`),
  ADD INDEX `idx_permission` (`permission_points`);

-- Step 2: Create sync_status_init tracking table
CREATE TABLE IF NOT EXISTS `quantmate`.`sync_status_init` (
    `id`               INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `source`           VARCHAR(50)  NOT NULL,
    `interface_key`    VARCHAR(100) NOT NULL,
    `initialized_from` DATE         NOT NULL COMMENT 'Earliest date with seeded status rows',
    `initialized_to`   DATE         NOT NULL COMMENT 'Latest date with seeded status rows',
    `created_at`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_source_iface` (`source`, `interface_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Migration 037: Store permission_points as integer thresholds
-- Date: 2026-04-17

-- Canonical rule:
-- - permission_points stores only the numeric threshold
-- - separately paid interfaces use permission_points = 0 and requires_permission = '1'
-- - AkShare interfaces use permission_points = 0

UPDATE `quantmate`.`data_source_items`
SET requires_permission = CASE
    WHEN source = 'tushare' AND LOWER(TRIM(COALESCE(requires_permission, ''))) IN ('1', 'true', 'yes', 'paid')
      THEN '1'
    WHEN source IN ('tushare', 'akshare') THEN '0'
    ELSE requires_permission
END;

UPDATE `quantmate`.`data_source_items`
SET permission_points = CASE
    WHEN source = 'akshare' THEN '0'
    WHEN LOWER(TRIM(COALESCE(requires_permission, ''))) IN ('1', 'true', 'yes', 'paid') THEN '0'
    ELSE COALESCE(CAST(REGEXP_SUBSTR(COALESCE(permission_points, ''), '[0-9]+') AS UNSIGNED), 0)
END;

ALTER TABLE `quantmate`.`data_source_items`
  MODIFY COLUMN `permission_points` INT NOT NULL DEFAULT 0 COMMENT '权限积分门槛; 0 表示不依赖积分判断';

-- Migration 038: Refresh Tushare catalog from current official CSV
-- Date: 2026-04-20
-- Generated from quantmate-docs/reference/tushare_api_full.csv

CREATE TEMPORARY TABLE IF NOT EXISTS `_tmp_tushare_catalog_refresh` (
    `source` VARCHAR(50) NOT NULL,
    `item_key` VARCHAR(100) NOT NULL,
    `item_name` VARCHAR(200) NOT NULL,
    `enabled` TINYINT(1) NOT NULL DEFAULT 0,
    `description` TEXT DEFAULT NULL,
    `category` VARCHAR(50) DEFAULT NULL,
    `sub_category` VARCHAR(50) DEFAULT NULL,
    `api_name` VARCHAR(100) DEFAULT NULL,
    `permission_points` INT NOT NULL DEFAULT 0,
    `rate_limit_note` VARCHAR(200) DEFAULT NULL,
    `requires_permission` VARCHAR(50) DEFAULT NULL,
    `target_database` VARCHAR(50) NOT NULL DEFAULT "tushare",
    `target_table` VARCHAR(100) NOT NULL,
    `table_created` TINYINT(1) NOT NULL DEFAULT 0,
    `sync_priority` INT NOT NULL DEFAULT 100,
    PRIMARY KEY (`source`, `item_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DELETE FROM `_tmp_tushare_catalog_refresh`;

INSERT INTO `_tmp_tushare_catalog_refresh`
  (`source`, `item_key`, `item_name`, `enabled`, `description`, `category`, `sub_category`, `api_name`, `permission_points`, `rate_limit_note`, `requires_permission`, `target_database`, `target_table`, `table_created`, `sync_priority`)
VALUES
    ('tushare', 'trade_cal', '交易日历', 1, '获取各大交易所交易日历数据,默认提取的是上交所', '股票数据', '基础数据', 'trade_cal', 2000, '', '0', 'tushare', 'trade_cal', 0, 5),
    ('tushare', 'stock_basic', '股票基础列表', 1, '获取基础信息数据，包括股票代码、名称、上市日期、退市日期等', '股票数据', '基础数据', 'stock_basic', 2000, '每次最多返回6000行数据（覆盖全市场A股，会随股票总数增长而增加）', '0', 'tushare', 'stock_basic', 0, 10),
    ('tushare', 'stock_company', '上市公司信息', 1, '获取上市公司基础信息，单次提取4500条，可以根据交易所分批提取', '股票数据', '基础数据', 'stock_company', 120, '', '0', 'tushare', 'stock_company', 1, 15),
    ('tushare', 'stock_daily', 'A股日线行情', 1, '获取股票行情数据，或通过', '股票数据', '行情数据', 'daily', 120, '', '0', 'tushare', 'stock_daily', 1, 20),
    ('tushare', 'bak_daily', '备用行情', 1, '获取备用行情，包括特定的行情指标(数据从2017年中左右开始，早期有几天数据缺失，近期正常)', '股票数据', '行情数据', 'bak_daily', 5000, '单次最大7000行数据，可以根据日期参数循环获取，正式权限需要5000积分', '0', 'tushare', 'bak_daily', 0, 22),
    ('tushare', 'suspend_d', '每日停复牌信息', 1, '按日期方式获取股票每日停复牌信息', '股票数据', '行情数据', 'suspend_d', 120, '', '0', 'tushare', 'suspend_d', 1, 23),
    ('tushare', 'suspend', '停复牌历史', 1, '停复牌历史数据', '股票数据', '行情数据', 'suspend', 120, '单次最大6000行', '0', 'tushare', 'suspend', 0, 24),
    ('tushare', 'moneyflow', '个股资金流向', 1, '获取沪深A股票资金流向数据，分析大单小单成交情况，用于判别资金动向，数据开始于2010年', '股票数据', '行情数据', 'moneyflow', 2000, '单次最大提取6000行记录，总量不限制', '0', 'tushare', 'moneyflow', 0, 25),
    ('tushare', 'stock_weekly', '周线行情', 1, '获取A股周线行情，本接口每周最后一个交易日更新，如需要使用每天更新的周线数据，请使用', '股票数据', '行情数据', 'weekly', 2000, '单次最大6000行，可使用交易日期循环提取，总量不限制', '0', 'tushare', 'stock_weekly', 0, 25),
    ('tushare', 'stock_monthly', '月线行情', 1, '获取A股月线数据', '股票数据', '行情数据', 'monthly', 2000, '单次最大4500行，总量不限制', '0', 'tushare', 'stock_monthly', 0, 26),
    ('tushare', 'index_daily', '指数日线行情', 1, '获取指数每日行情，还可以通过bar接口获取。由于服务器压力，目前规则是单次调取最多取8000行记录，可以设置start和end日期补全。指数行情也可以通过', '指数数据', '行情数据', 'index_daily', 2000, '', '0', 'tushare', 'index_daily', 0, 27),
    ('tushare', 'index_weekly', '指数周线行情', 1, '获取指数周线行情', '指数数据', '行情数据', 'index_weekly', 600, '单次最大1000行记录，可分批获取，总量不限制', '0', 'tushare', 'index_weekly', 0, 28),
    ('tushare', 'adj_factor', '复权因子', 0, '本接口由Tushare自行生产，获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子', '股票数据', '行情数据', 'adj_factor', 120, '', '0', 'tushare', 'adj_factor', 0, 30),
    ('tushare', 'daily_basic', '每日指标数据', 0, '获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。单次请求最大返回6000条数据，可按日线循环提取全部历史', '股票数据', '行情数据', 'daily_basic', 2000, '', '0', 'tushare', 'daily_basic', 0, 31),
    ('tushare', 'dividend', '分红送股', 0, '分红送股数据', '股票数据', '财务数据', 'dividend', 2000, '', '0', 'tushare', 'dividend', 0, 50),
    ('tushare', 'fina_indicator', '财务指标数据', 0, '获取上市公司财务指标数据，为避免服务器压力，现阶段每次请求最多返回100条记录，可通过设置日期多次请求获取更多数据', '股票数据', '财务数据', 'fina_indicator', 2000, '', '0', 'tushare', 'fina_indicator', 0, 55),
    ('tushare', 'income', '利润表', 0, '获取上市公司财务利润表数据', '股票数据', '财务数据', 'income', 2000, '', '0', 'tushare', 'income', 0, 56),
    ('tushare', 'top10_holders', '前十大股东', 0, '获取上市公司前十大股东数据，包括持有数量和比例等信息', '股票数据', '参考数据', 'top10_holders', 2000, '', '0', 'tushare', 'top10_holders', 0, 57),
    ('tushare', 'limit_list_d', '涨跌停统计', 0, '获取A股每日涨跌停、炸板数据情况，数据从2020年开始（不提供ST股票的统计）', '股票数据', '行情数据', 'limit_list_d', 5000, '单次最大可以获取2500条数据，可通过日期或者股票循环提取', '0', 'tushare', 'limit_list_d', 0, 70),
    ('tushare', 'stk_limit', '每日涨跌停价格', 1, '获取全市场（包含A/B股和基金）每日涨跌停价格，包括涨停价格，跌停价格等，每个交易日8点40左右更新当日股票涨跌停价格', '股票数据', '行情数据', 'stk_limit', 2000, '单次最多提取5800条记录，可循环调取，总量不限制', '0', 'tushare', 'stk_limit', 0, 70),
    ('tushare', 'margin_detail', '融资融券明细', 0, '获取沪深两市每日融资融券明细', '股票数据', '行情数据', 'margin_detail', 2000, '单次请求最大返回6000行数据，可根据日期循环', '0', 'tushare', 'margin_detail', 0, 80),
    ('tushare', 'margin', '融资融券', 0, '获取融资融券每日交易汇总数据', '股票数据', '行情数据', 'margin', 2000, '单次请求最大返回4000行数据，可根据日期循环', '0', 'tushare', 'margin', 0, 81),
    ('tushare', 'block_trade', '大宗交易', 1, '大宗交易', '股票数据', '特色数据', 'block_trade', 300, '单次最大1000条，总量不限制', '0', 'tushare', 'block_trade', 0, 90),
    ('tushare', 'hsgt_top10', '沪深股通成份股', 0, '获取沪股通、深股通每日前十大成交详细数据，每天18~20点之间完成当日更新', '股票数据', '基础数据', 'hsgt_top10', 2000, '', '0', 'tushare', 'hsgt_top10', 0, 100),
    ('tushare', 'namechange', '股票曾用名', 0, '历史名称变更记录', '股票数据', '基础数据', 'namechange', 2000, '', '0', 'tushare', 'namechange', 0, 103),
    ('tushare', 'new_share', 'IPO新股列表', 0, '获取新股上市列表数据', '股票数据', '基础数据', 'new_share', 120, '单次最大2000条，总量不限制', '0', 'tushare', 'new_share', 1, 104),
    ('tushare', 'stk_holdertrade', '董监高持股', 0, '获取上市公司增减持数据，了解重要股东近期及历史上的股份增减变化', '股票数据', '基础数据', 'stk_holdertrade', 2000, '单次最大提取3000行记录，总量不限制', '0', 'tushare', 'stk_holdertrade', 0, 109),
    ('tushare', 'balancesheet', '资产负债表', 0, '获取上市公司资产负债表', '股票数据', '财务数据', 'balancesheet', 2000, '', '0', 'tushare', 'balancesheet', 0, 201),
    ('tushare', 'cashflow', '现金流量表', 0, '获取上市公司现金流量表', '股票数据', '财务数据', 'cashflow', 2000, '', '0', 'tushare', 'cashflow', 0, 203),
    ('tushare', 'forecast', '业绩预告', 0, '获取业绩预告数据', '股票数据', '财务数据', 'forecast', 2000, '', '0', 'tushare', 'forecast', 0, 205),
    ('tushare', 'express', '业绩快报', 0, '获取上市公司业绩快报', '股票数据', '财务数据', 'express', 2000, '', '0', 'tushare', 'express', 0, 206),
    ('tushare', 'fina_audit', '财务审计意见', 0, '获取上市公司定期财务审计意见数据', '股票数据', '财务数据', 'fina_audit', 2000, '', '0', 'tushare', 'fina_audit', 0, 208),
    ('tushare', 'fina_mainbz', '主营业务构成', 0, '获得上市公司主营业务构成，分地区和产品两种方式', '股票数据', '财务数据', 'fina_mainbz', 2000, '', '0', 'tushare', 'fina_mainbz', 0, 209),
    ('tushare', 'disclosure_date', '财报披露计划', 0, '获取财报披露计划日期', '股票数据', '财务数据', 'disclosure_date', 500, '单次最大3000，总量不限制', '0', 'tushare', 'disclosure_date', 0, 211),
    ('tushare', 'top_list', '龙虎榜每日明细', 0, '龙虎榜每日交易明细', '股票数据', '特色数据', 'top_list', 2000, '单次请求返回最大10000行数据，可通过参数循环获取全部历史', '0', 'tushare', 'top_list', 0, 300),
    ('tushare', 'top_inst', '龙虎榜机构交易明细', 0, '龙虎榜机构成交明细', '股票数据', '特色数据', 'top_inst', 5000, '单次请求最大返回10000行数据，可根据参数循环获取全部历史', '0', 'tushare', 'top_inst', 0, 301),
    ('tushare', 'pledge_detail', '股权质押明细', 0, '获取股票质押明细数据', '股票数据', '特色数据', 'pledge_detail', 500, '单次最大1000', '0', 'tushare', 'pledge_detail', 0, 302),
    ('tushare', 'pledge_stat', '股权质押统计', 0, '获取股票质押统计数据', '股票数据', '特色数据', 'pledge_stat', 2000, '单次最大1000', '0', 'tushare', 'pledge_stat', 0, 303),
    ('tushare', 'repurchase', '股票回购', 0, '获取上市公司回购股票数据', '股票数据', '特色数据', 'repurchase', 600, '', '0', 'tushare', 'repurchase', 0, 304),
    ('tushare', 'share_float', '限售股解禁', 0, '获取限售股解禁', '股票数据', '特色数据', 'share_float', 120, '单次最大6000条，总量不限制', '0', 'tushare', 'share_float', 0, 305),
    ('tushare', 'stk_factor_pro', '股票技术面因子', 0, '获取股票每日技术面因子数据，用于跟踪股票当前走势情况，数据由Tushare社区自产，覆盖全历史；输出参数_bfq表示不复权，_qfq表示前复权 _hfq表示后复权，描述中说明了因子的默认传参，如需要特殊参数或者更多因子可以联系管理员评估', '股票数据', '股票因子', 'stk_factor_pro', 5000, '单次调取最多返回10000条数据，可以通过日期参数循环', '0', 'tushare', 'stk_factor_pro', 0, 307),
    ('tushare', 'cyq_perf', '每日筹码及胜率', 0, '获取A股每日筹码平均成本和胜率情况，每天18~19点左右更新，数据从2018年开始', '股票数据', '特色数据', 'cyq_perf', 5000, '单次最大5000条，可以分页或者循环提取', '0', 'tushare', 'cyq_perf', 0, 309),
    ('tushare', 'cyq_chips', '筹码分布', 0, '获取A股每日的筹码分布情况，提供各价位占比，数据从2018年开始，每天18~19点之间更新当日数据', '股票数据', '特色数据', 'cyq_chips', 5000, '单次最大2000条，可以按股票代码和日期循环提取', '0', 'tushare', 'cyq_chips', 0, 310),
    ('tushare', 'kpl_list', '开盘啦榜单', 0, '获取开盘啦涨停、跌停、炸板等榜单数据', '股票数据', '特色数据', 'kpl_list', 5000, '单次最大8000条数据，可根据日期循环获取历史数据', '0', 'tushare', 'kpl_list', 0, 311),
    ('tushare', 'dc_hot', '东方财富热榜', 0, '获取东方财富App热榜数据，包括A股市场、ETF基金、港股市场、美股市场等等，每日盘中提取4次，收盘后4次，最晚22点提取一次', '股票数据', '特色数据', 'dc_hot', 8000, '单次最大2000条，可根据日期等参数循环获取全部数据', '0', 'tushare', 'dc_hot', 0, 312),
    ('tushare', 'dc_member', '东方财富板块成分', 0, '获取东方财富板块每日成分数据，可以根据概念板块代码和交易日期，获取历史成分', '股票数据', '特色数据', 'dc_member', 6000, '单次最大获取5000条数据，可以通过日期和代码循环获取', '0', 'tushare', 'dc_member', 0, 313),
    ('tushare', 'report_rc', '盈利预测数据', 0, '获取券商（卖方）每天研报的盈利预测数据，数据从2010年开始，每晚19~22点更新当日数据', '股票数据', '特色数据', 'report_rc', 120, '单次最大3000条，可分页和循环提取所有数据', '0', 'tushare', 'report_rc', 1, 315),
    ('tushare', 'index_basic', '指数基本信息', 0, '获取指数基础信息', '指数数据', '基础数据', 'index_basic', 2000, '', '0', 'tushare', 'index_basic', 0, 400),
    ('tushare', 'index_monthly', '指数月线行情', 0, '获取指数月线行情,每月更新一次', '指数数据', '行情数据', 'index_monthly', 600, '单次最大1000行记录,可多次获取,总量不限制', '0', 'tushare', 'index_monthly', 0, 401),
    ('tushare', 'index_weight', '指数成分和权重', 0, '获取各类指数成分和权重，', '指数数据', '成分数据', 'index_weight', 2000, '', '0', 'tushare', 'index_weight', 0, 402),
    ('tushare', 'index_dailybasic', '大盘指数每日指标', 0, '目前只提供上证综指，深证成指，上证50，中证500，中小板指，创业板指的每日指标数据', '指数数据', '大盘数据', 'index_dailybasic', 4000, '', '0', 'tushare', 'index_dailybasic', 0, 403),
    ('tushare', 'index_classify', '申万行业分类', 0, '获取申万行业分类，可以获取申万2014年版本（28个一级分类，104个二级分类，227个三级分类）和2021年本版（31个一级分类，134个二级分类，346个三级分类）列表信息', '指数数据', '行业分类', 'index_classify', 2000, '', '0', 'tushare', 'index_classify', 0, 404),
    ('tushare', 'index_member_all', '申万行业成分', 0, '按三级分类提取申万行业成分，可提供某个分类的所有成分，也可按股票代码提取所属分类，参数灵活', '指数数据', '行业分类', 'index_member_all', 2000, '单次最大2000行，总量不限制', '0', 'tushare', 'index_member_all', 0, 405),
    ('tushare', 'index_global', '国际指数', 0, '获取国际主要指数日线行情', '指数数据', '国际指数', 'index_global', 6000, '单次最大提取4000行情数据，可循环获取，总量不限制', '0', 'tushare', 'index_global', 0, 406),
    ('tushare', 'fund_share', 'ETF基金规模', 0, '获取基金规模数据，包含上海和深圳ETF基金', 'ETF专题', 'ETF数据', 'fund_share', 2000, '单次最大提取2000行数据', '0', 'tushare', 'fund_share', 0, 501),
    ('tushare', 'fund_daily', '场内基金日线行情', 0, '获取ETF行情每日收盘后成交数据，历史超过10年', 'ETF专题', 'ETF数据', 'fund_daily', 5000, '单次最大5000行记录，可以根据ETF代码和日期循环获取历史，总量不限制', '0', 'tushare', 'fund_daily', 0, 502),
    ('tushare', 'fund_adj', 'ETF复权因子', 0, '获取基金复权因子，用于计算基金复权行情', 'ETF专题', 'ETF数据', 'fund_adj', 600, '单次最大提取2000行记录，可循环提取，数据总量不限制', '0', 'tushare', 'fund_adj', 0, 503),
    ('tushare', 'fund_basic', '公募基金列表', 0, '获取公募基金数据列表，包括场内和场外基金', '公募基金', '基金基础', 'fund_basic', 2000, '单次最大可以提取15000条数据', '0', 'tushare', 'fund_basic', 0, 510),
    ('tushare', 'fund_company', '公募基金公司', 0, '获取公募基金管理人列表', '公募基金', '基金基础', 'fund_company', 1500, '', '0', 'tushare', 'fund_company', 0, 511),
    ('tushare', 'fund_nav', '公募基金净值', 0, '获取公募基金净值数据', '公募基金', '基金净值', 'fund_nav', 2000, '', '0', 'tushare', 'fund_nav', 0, 512),
    ('tushare', 'fund_div', '公募基金分红', 0, '获取公募基金分红数据', '公募基金', '基金分红', 'fund_div', 400, '', '0', 'tushare', 'fund_div', 0, 514),
    ('tushare', 'fund_portfolio', '公募基金持仓数据', 0, '获取公募基金持仓数据，季度更新', '公募基金', '基金持仓', 'fund_portfolio', 5000, '', '0', 'tushare', 'fund_portfolio', 0, 515),
    ('tushare', 'fut_basic', '期货合约列表', 0, '获取期货合约列表数据', '期货数据', '合约数据', 'fut_basic', 2000, '单次最大10000', '0', 'tushare', 'fut_basic', 0, 600),
    ('tushare', 'fut_daily', '期货日线行情', 0, '期货日线行情数据', '期货数据', '行情数据', 'fut_daily', 2000, '单次最大2000条，总量不限制', '0', 'tushare', 'fut_daily', 0, 602),
    ('tushare', 'fut_holding', '每日成交持仓排名', 0, '获取每日成交持仓排名数据', '期货数据', '持仓数据', 'fut_holding', 2000, '单次最大2000，总量不限制', '0', 'tushare', 'fut_holding', 0, 603),
    ('tushare', 'fut_wsr', '仓单日报', 0, '获取仓单日报数据，了解各仓库/厂库的仓单变化', '期货数据', '仓单数据', 'fut_wsr', 2000, '单次最大1000，总量不限制', '0', 'tushare', 'fut_wsr', 0, 604),
    ('tushare', 'fut_settle', '结算参数', 0, '获取每日结算参数数据，包括交易和交割费率等', '期货数据', '结算数据', 'fut_settle', 2000, '单次最大返回1600行数据，可根据日期循环，总量不限制', '0', 'tushare', 'fut_settle', 0, 605),
    ('tushare', 'fut_mapping', '合约交叉引用', 0, '获取期货主力（或连续）合约与月合约映射数据', '期货数据', '关联合约', 'fut_mapping', 2000, '单次最大2000条，总量不限制', '0', 'tushare', 'fut_mapping', 0, 607),
    ('tushare', 'opt_basic', '期权合约列表', 0, '获取期权合约信息', '期权数据', '合约数据', 'opt_basic', 5000, '', '0', 'tushare', 'opt_basic', 0, 700),
    ('tushare', 'cb_basic', '可转债基础信息', 0, '获取可转债基本信息', '债券专题', '可转债数据', 'cb_basic', 2000, '单次最大2000，总量不限制', '0', 'tushare', 'cb_basic', 0, 750),
    ('tushare', 'cb_issue', '可转债发行数据', 0, '获取可转债发行数据', '债券专题', '可转债数据', 'cb_issue', 2000, '单次最大2000，可多次提取，总量不限制', '0', 'tushare', 'cb_issue', 0, 751),
    ('tushare', 'cb_daily', '可转债日线数据', 0, '获取可转债行情', '债券专题', '可转债数据', 'cb_daily', 2000, '单次最大2000条，可多次提取，总量不限制', '0', 'tushare', 'cb_daily', 0, 752),
    ('tushare', 'cb_share', '可转债待发', 0, '获取可转债转股结果', '债券专题', '可转债数据', 'cb_share', 2000, '单次最大2000，总量不限制', '0', 'tushare', 'cb_share', 0, 753),
    ('tushare', 'fx_obasic', '外汇基础信息', 0, '获取海外外汇基础信息，目前只有FXCM交易商的数据', '外汇数据', '外汇基础', 'fx_obasic', 2000, '', '0', 'tushare', 'fx_obasic', 0, 770),
    ('tushare', 'fx_daily', '外汇日线行情', 0, '获取外汇日线行情', '外汇数据', '外汇行情', 'fx_daily', 2000, '单次最大提取1000行记录，可多次提取，总量不限制', '0', 'tushare', 'fx_daily', 0, 771),
    ('tushare', 'hk_basic', '港股列表', 0, '获取港股列表信息', '港股数据', '港股基础', 'hk_basic', 2000, '', '0', 'tushare', 'hk_basic', 0, 780),
    ('tushare', 'us_basic', '美股列表', 0, '获取美股列表信息', '美股数据', '美股基础', 'us_basic', 120, '单次最大6000，可分页提取', '0', 'tushare', 'us_basic', 1, 800),
    ('tushare', 'us_daily', '美股日线', 0, '获取美股行情（未复权），包括全部股票全历史行情，以及重要的市场和估值指标', '美股数据', '美股行情', 'us_daily', 120, '单次最大6000行数据，可根据日期参数循环提取，开通正式权限后也可支持分页提取全部历史', '0', 'tushare', 'us_daily', 1, 801),
    ('tushare', 'bo_monthly', '电影月度票房', 0, '获取电影月度票房数据', '行业经济', '电影数据', 'bo_monthly', 500, '', '0', 'tushare', 'bo_monthly', 0, 820),
    ('tushare', 'bo_weekly', '电影周票房', 0, '获取周度票房数据', '行业经济', '电影数据', 'bo_weekly', 500, '', '0', 'tushare', 'bo_weekly', 0, 821),
    ('tushare', 'daily_info', '市场交易统计', 0, '获取交易所股票交易统计，包括各板块明细', '行业经济', '市场统计', 'daily_info', 600, '单次最大4000，可循环获取，总量不限制', '0', 'tushare', 'daily_info', 0, 825),
    ('tushare', 'shibor_lpr', 'LPR贷款基础利率', 0, 'LPR贷款基础利率', '宏观经济', '利率数据', 'shibor_lpr', 120, '单次最大4000(相当于单次可提取18年历史)，总量不限制，可通过设置开始和结束日期分段获取', '0', 'tushare', 'shibor_lpr', 1, 840),
    ('tushare', 'shibor', '银行间拆借', 0, 'shibor利率', '宏观经济', '利率数据', 'shibor', 120, '单次最大2000，总量不限制，可通过设置开始和结束日期分段获取', '0', 'tushare', 'shibor', 0, 841),
    ('tushare', 'cn_cpi', '居民消费价格指数（CPI）', 0, '获取CPI居民消费价格数据，包括全国、城市和农村的数据', '宏观经济', '国内宏观', 'cn_cpi', 600, '单次最大5000行，一次可以提取全部数据', '0', 'tushare', 'cn_cpi', 0, 842),
    ('tushare', 'cn_gdp', '国内生产总值（GDP）', 0, '获取国民经济之GDP数据', '宏观经济', '国内宏观', 'cn_gdp', 600, '单次最大10000，一次可以提取全部数据', '0', 'tushare', 'cn_gdp', 0, 843),
    ('tushare', 'stk_mins', '股票实时分钟', 0, '获取A股分钟数据，支持1min/5min/15min/30min/60min行情，提供Python SDK和 http Restful API两种方式', '股票数据', '行情数据', 'stk_mins', 0, '单次最大8000行数据，可以通过股票代码和时间循环获取，本接口可以提供超过10年历史分钟数据', '1', 'tushare', 'stk_mins', 0, 900),
    ('tushare', 'rt_etf_k', 'ETF实时日线', 0, '获取ETF实时日k线行情，支持按ETF代码或代码通配符一次性提取全部ETF实时日k线行情', 'ETF专题', 'ETF数据', 'rt_etf_k', 0, '', '1', 'tushare', 'rt_etf_k', 0, 905),
    ('tushare', 'ft_mins', '期货历史分钟', 0, '获取全市场期货合约分钟数据，支持1min/5min/15min/30min/60min行情，提供Python SDK和 http Restful API两种方式，如果需要主力合约分钟，请先通过主力', '期货数据', '历史分钟', 'ft_mins', 120, '单次最大8000行数据，可以通过期货合约代码和时间循环获取，本接口可以提供超过10年历史分钟数据', '0', 'tushare', 'ft_mins', 0, 906),
    ('tushare', 'opt_mins', '期权历史分钟', 0, '获取全市场期权合约分钟数据，支持1min/5min/15min/30min/60min行情，提供Python SDK和 http Restful API两种方式', '期权数据', '历史分钟', 'opt_mins', 120, '单次最大8000行数据，可以通过合约代码和时间循环获取', '0', 'tushare', 'opt_mins', 0, 909),
    ('tushare', 'hk_daily', '港股日线', 0, '获取港股每日增量和历史行情，每日18点左右更新当日数据', '港股数据', '港股行情', 'hk_daily', 0, '单次最大提取5000行记录，可多次提取，总量不限制', '1', 'tushare', 'hk_daily', 0, 910),
    ('tushare', 'hk_mins', '港股分钟', 0, '港股分钟数据，支持1min/5min/15min/30min/60min行情，提供Python SDK和 http Restful API两种方式', '港股数据', '港股分钟', 'hk_mins', 120, '单次最大8000行数据，可以通过股票代码和日期循环获取', '0', 'tushare', 'hk_mins', 0, 911),
    ('tushare', 'rt_hk_k', '港股实时日线', 0, '获取港股实时日k线行情，支持按股票代码及股票代码通配符一次性提取全部股票实时日k线行情', '港股数据', '实时数据', 'rt_hk_k', 0, '单次最大可提取5000条数据', '1', 'tushare', 'rt_hk_k', 0, 913),
    ('tushare', 'hk_hold', '港股通', 0, '获取沪深港股通持股明细，数据来源港交所', '港股数据', '港股资金', 'hk_hold', 120, '单次最多提取3800条记录，可循环调取，总量不限制', '0', 'tushare', 'hk_hold', 0, 914),
    ('tushare', 'concept_corpus', '概念股语料', 0, '概念股数据用于训练', '大模型语料', '数据标注', 'concept_corpus', 0, '单次最大5000', '1', 'tushare', 'concept_corpus', 0, 919),
    ('tushare', 'stock_corpus', '股吧评论', 0, '股吧评论文本数据', '大模型语料', '数据标注', 'stock_corpus', 0, '单次最大5000', '1', 'tushare', 'stock_corpus', 0, 920),
    ('tushare', 'ann_corpus', '公告摘要', 0, '公告摘要数据', '大模型语料', '数据标注', 'ann_corpus', 0, '单次最大5000', '1', 'tushare', 'ann_corpus', 0, 921),
    ('tushare', 'report_corpus', '研报语料', 0, '券商研报数据', '大模型语料', '数据标注', 'report_corpus', 0, '单次最大5000', '1', 'tushare', 'report_corpus', 0, 922),
    ('tushare', 'news', '新闻快讯', 0, '获取主流新闻网站的快讯新闻数据,提供超过6年以上历史新闻', '资讯数据', '新闻快讯', 'news', 0, '单次最大1500条新闻，可根据时间参数循环提取历史', '1', 'tushare', 'news', 0, 923),
    ('tushare', 'major_news', '新闻通讯', 0, '获取长篇通讯信息，覆盖主要新闻资讯网站，提供超过8年历史新闻', '资讯数据', '新闻通讯', 'major_news', 0, '单次最大400行记录，可循环提取保存到本地', '1', 'tushare', 'major_news', 0, 924),
    ('tushare', 'cctv_news', '新闻联播', 0, '获取新闻联播文字稿数据，数据开始于2017年', '资讯数据', '新闻联播', 'cctv_news', 0, '可根据日期参数循环提取，总量不限制', '1', 'tushare', 'cctv_news', 0, 925),
    ('tushare', 'irm_qa_sh', '上证e互动', 0, '获取上交所e互动董秘问答文本数据。上证e互动是由上海证券交易所建立、上海证券市场所有参与主体无偿使用的沟通平台,旨在引导和促进上市公司、投资者等各市场参与主体之间的信息沟通,构建集中、便捷的互动渠道。本接口数据记录了以上沟通问答的文本数据', '资讯数据', '互动易', 'irm_qa_sh', 120, '单次请求最大返回3000行数据，可根据股票代码，日期等参数循环提取全部数据', '0', 'tushare', 'irm_qa_sh', 0, 928),
    ('tushare', 'irm_qa_sz', '深证互动易', 0, '互动易是由深交所官方推出,供投资者与上市公司直接沟通的平台,一站式公司资讯汇集,提供第一手的互动问答、投资者关系信息、公司声音等内容', '资讯数据', '互动易', 'irm_qa_sz', 120, '单次请求最大返回3000行数据，可根据股票代码，日期等参数循环提取全部数据', '0', 'tushare', 'irm_qa_sz', 0, 929),
    ('tushare', 'stk_premarket', '每日股本（盘前）', 0, '每日开盘前获取当日股票的股本情况，包括总股本和流通股本，涨跌停价格等', '股票数据', '基础数据', 'stk_premarket', 0, '单次最大8000条数据，可循环提取', '0', 'tushare', 'stk_premarket', 0, 1000),
    ('tushare', 'stock_st', 'ST股票列表', 0, '获取ST股票列表，可根据交易日期获取历史上每天的ST列表', '股票数据', '基础数据', 'stock_st', 3000, '', '0', 'tushare', 'stock_st', 0, 1001),
    ('tushare', 'st', 'ST风险警示板股票', 0, 'ST风险警示板股票列表', '股票数据', '基础数据', 'st', 6000, '单次最大1000，可根据股票代码循环获取历史数据', '0', 'tushare', 'st', 0, 1002),
    ('tushare', 'stock_hsgt', '沪深港通股票列表', 0, '获取沪深港通股票列表', '股票数据', '基础数据', 'stock_hsgt', 3000, '', '0', 'tushare', 'stock_hsgt', 0, 1003),
    ('tushare', 'stk_managers', '上市公司管理层', 0, '获取上市公司管理层', '股票数据', '基础数据', 'stk_managers', 2000, '', '0', 'tushare', 'stk_managers', 0, 1004),
    ('tushare', 'stk_rewards', '管理层薪酬和持股', 0, '获取上市公司管理层薪酬和持股', '股票数据', '基础数据', 'stk_rewards', 2000, '', '0', 'tushare', 'stk_rewards', 0, 1005),
    ('tushare', 'bse_mapping', '北交所新旧代码对照', 0, '获取北交所股票代码变更后新旧代码映射表数据', '股票数据', '基础数据', 'bse_mapping', 120, '单次最大1000条（本接口总数据量300以内）', '0', 'tushare', 'bse_mapping', 0, 1006),
    ('tushare', 'bak_basic', '股票历史列表', 0, '获取备用基础列表，数据从2016年开始', '股票数据', '基础数据', 'bak_basic', 0, '单次最大7000条，可以根据日期参数循环获取历史，正式权限需要5000积分', '0', 'tushare', 'bak_basic', 0, 1007),
    ('tushare', 'rt_k', '实时日线', 0, '获取实时日k线行情，支持按股票代码及股票代码通配符一次性提取全部股票实时日k线行情', '股票数据', '行情数据', 'rt_k', 0, '单次最大可提取6000条数据，等同于一次提取全市场', '1', 'tushare', 'rt_k', 0, 1008),
    ('tushare', 'rt_min', '实时分钟', 0, '获取全A股票实时分钟数据，包括1~60min', '股票数据', '行情数据', 'rt_min', 0, '单次最大1000行数据，可以通过股票代码提取数据，支持逗号分隔的多个代码同时提取', '1', 'tushare', 'rt_min', 0, 1009),
    ('tushare', 'rt_min_daily', 'A股实时分钟-日累计', 0, '获取A股当日盘中历史分钟数据，可以提取单只股票当日开盘以来的所有分钟数据', '股票数据', '行情数据', 'rt_min_daily', 0, '', '1', 'tushare', 'rt_min_daily', 0, 1010),
    ('tushare', 'stk_weekly_monthly', '周/月线行情(每日更新)', 0, '股票周/月线行情(每日更新)', '股票数据', '行情数据', 'stk_weekly_monthly', 2000, '单次最大6000,可使用交易日期循环提取，总量不限制', '0', 'tushare', 'stk_weekly_monthly', 0, 1011),
    ('tushare', 'stk_week_month_adj', '周/月线复权行情(每日更新)', 0, '股票周/月线行情(复权--每日更新)', '股票数据', '行情数据', 'stk_week_month_adj', 2000, '单次最大6000,可使用交易日期循环提取，总量不限制', '0', 'tushare', 'stk_week_month_adj', 0, 1012),
    ('tushare', 'realtime_quote', '实时Tick（爬虫）', 0, '本接口是tushare org版实时接口的顺延，数据来自网络，且不进入tushare服务器，属于爬虫接口，请将tushare升级到1.3.3版本以上', '股票数据', '行情数据', 'realtime_quote', 0, '', '0', 'tushare', 'realtime_quote', 0, 1013),
    ('tushare', 'realtime_tick', '实时成交（爬虫）', 0, '本接口是tushare org版实时接口的顺延，数据来自网络，且不进入tushare服务器，属于爬虫接口，数据包括该股票当日开盘以来的所有分笔成交数据', '股票数据', '行情数据', 'realtime_tick', 0, '', '0', 'tushare', 'realtime_tick', 0, 1014),
    ('tushare', 'realtime_list', '实时排名（爬虫）', 0, '本接口是tushare org版实时接口的顺延，数据来自网络，且不进入tushare服务器，属于爬虫接口，数据包括该股票当日开盘以来的所有分笔成交数据', '股票数据', '行情数据', 'realtime_list', 0, '', '0', 'tushare', 'realtime_list', 0, 1015),
    ('tushare', 'ggt_top10', '港股通十大成交股', 0, '获取港股通每日成交数据，其中包括沪市、深市详细数据，每天18~20点之间完成当日更新', '股票数据', '行情数据', 'ggt_top10', 0, '', '0', 'tushare', 'ggt_top10', 0, 1016),
    ('tushare', 'ggt_daily', '港股通每日成交统计', 0, '获取港股通每日成交信息，数据从2014年开始', '股票数据', '行情数据', 'ggt_daily', 2000, '单次最大1000，总量数据不限制', '0', 'tushare', 'ggt_daily', 0, 1017),
    ('tushare', 'ggt_monthly', '港股通每月成交统计', 0, '港股通每月成交信息，数据从2014年开始', '股票数据', '行情数据', 'ggt_monthly', 5000, '单次最大1000', '0', 'tushare', 'ggt_monthly', 0, 1018),
    ('tushare', 'stk_shock', '个股异常波动', 0, '根据证券交易所交易规则的有关规定，交易所每日发布股票交易异常波动情况', '股票数据', '参考数据', 'stk_shock', 6000, '单次最大1000条，可根据代码或日期循环提取', '0', 'tushare', 'stk_shock', 0, 1019),
    ('tushare', 'stk_high_shock', '个股严重异常波动', 0, '根据证券交易所交易规则的有关规定，交易所每日发布股票交易严重异常波动情况', '股票数据', '参考数据', 'stk_high_shock', 6000, '单次最大1000条，可根据代码或日期循环提取', '0', 'tushare', 'stk_high_shock', 0, 1020),
    ('tushare', 'stk_alert', '交易所重点提示证券', 0, '根据证券交易所交易规则的有关规定，交易所每日发布重点提示证券', '股票数据', '参考数据', 'stk_alert', 6000, '单次最大1000条，可根据代码或日期循环提取', '0', 'tushare', 'stk_alert', 0, 1021),
    ('tushare', 'top10_floatholders', '前十大流通股东', 0, '获取上市公司前十大流通股东数据', '股票数据', '参考数据', 'top10_floatholders', 2000, '', '0', 'tushare', 'top10_floatholders', 0, 1022),
    ('tushare', 'stk_account', '股票开户数据（停）', 0, '获取股票账户开户数据，统计周期为一周', '股票数据', '参考数据', 'stk_account', 600, '', '0', 'tushare', 'stk_account', 0, 1023),
    ('tushare', 'stk_account_old', '股票开户数据（旧）', 0, '获取股票账户开户数据旧版格式数据，数据从2008年1月开始，到2015年5月29，新数据请通过', '股票数据', '参考数据', 'stk_account_old', 600, '', '0', 'tushare', 'stk_account_old', 0, 1024),
    ('tushare', 'stk_holdernumber', '股东人数', 0, '获取上市公司股东户数数据，数据不定期公布', '股票数据', '参考数据', 'stk_holdernumber', 600, '单次最大3000,总量不限制', '0', 'tushare', 'stk_holdernumber', 0, 1025),
    ('tushare', 'ccass_hold', '中央结算系统持股统计', 0, '获取中央结算系统持股汇总数据，覆盖全部历史数据，根据交易所披露时间，当日数据在下一交易日早上9点前完成入库', '股票数据', '特色数据', 'ccass_hold', 120, '单次最大5000条数据，可循环或分页提供全部', '0', 'tushare', 'ccass_hold', 0, 1026),
    ('tushare', 'ccass_hold_detail', '中央结算系统持股明细', 0, '获取中央结算系统机构席位持股明细，数据覆盖', '股票数据', '特色数据', 'ccass_hold_detail', 8000, '单次最大返回6000条数据，可以循环或分页提取', '0', 'tushare', 'ccass_hold_detail', 0, 1027),
    ('tushare', 'stk_auction_o', '股票开盘集合竞价数据', 0, '股票开盘9:30集合竞价数据，每天盘后更新', '股票数据', '特色数据', 'stk_auction_o', 0, '单次请求最大返回10000行数据，可根据日期循环', '1', 'tushare', 'stk_auction_o', 0, 1028),
    ('tushare', 'stk_auction_c', '股票收盘集合竞价数据', 0, '股票收盘15:00集合竞价数据，每天盘后更新', '股票数据', '特色数据', 'stk_auction_c', 0, '单次请求最大返回10000行数据，可根据日期循环', '1', 'tushare', 'stk_auction_c', 0, 1029),
    ('tushare', 'stk_nineturn', '神奇九转指标', 0, '神奇九转（又称“九转序列”）是一种基于技术分析的股票趋势反转指标，其思想来源于技术分析大师汤姆·迪马克（Tom DeMark）的TD序列。该指标的核心功能是通过识别股价在上涨或下跌过程中连续9天的特定走势，来判断股价的潜在反转点，从而帮助投资者提高抄底和逃顶的成功率，日线级别配合60min的九转效果更好，数据从20230101开始', '股票数据', '特色数据', 'stk_nineturn', 6000, '单次提取最大返回10000行数据，可通过股票代码和日期循环获取全部数据', '0', 'tushare', 'stk_nineturn', 0, 1030),
    ('tushare', 'stk_ah_comparison', 'AH股比价', 0, 'AH股比价数据，可根据交易日期获取历史', '股票数据', '特色数据', 'stk_ah_comparison', 5000, '', '0', 'tushare', 'stk_ah_comparison', 0, 1031),
    ('tushare', 'stk_surv', '机构调研数据', 0, '获取上市公司机构调研记录数据', '股票数据', '特色数据', 'stk_surv', 5000, '单次最大获取100条数据，可循环或分页提取', '0', 'tushare', 'stk_surv', 0, 1032),
    ('tushare', 'broker_recommend', '券商月度金股', 0, '获取券商月度金股，一般1日~3日内更新当月数据', '股票数据', '特色数据', 'broker_recommend', 6000, '单次最大1000行数据，可循环提取', '0', 'tushare', 'broker_recommend', 0, 1033),
    ('tushare', 'margin_secs', '融资融券标的（盘前）', 0, '获取沪深京三大交易所融资融券标的（包括ETF），每天盘前更新', '股票数据', '两融及转融通', 'margin_secs', 2000, '单次最大6000行数据，可根据股票代码、交易日期、交易所代码循环提取', '0', 'tushare', 'margin_secs', 0, 1034),
    ('tushare', 'slb_sec', '转融券交易汇总(停）', 0, '转融通转融券交易汇总', '股票数据', '两融及转融通', 'slb_sec', 2000, '单次最大可以提取5000行数据，可循环获取所有历史', '0', 'tushare', 'slb_sec', 0, 1035),
    ('tushare', 'slb_len', '转融资交易汇总', 0, '转融通融资汇总', '股票数据', '两融及转融通', 'slb_len', 2000, '单次最大可以提取5000行数据，可循环获取所有历史', '0', 'tushare', 'slb_len', 0, 1036),
    ('tushare', 'slb_sec_detail', '转融券交易明细(停）', 0, '转融券交易明细', '股票数据', '两融及转融通', 'slb_sec_detail', 2000, '单次最大可以提取5000行数据，可循环获取所有历史', '0', 'tushare', 'slb_sec_detail', 0, 1037),
    ('tushare', 'slb_len_mm', '做市借券交易汇总(停）', 0, '做市借券交易汇总', '股票数据', '两融及转融通', 'slb_len_mm', 2000, '单次最大可以提取5000行数据，可循环获取所有历史', '0', 'tushare', 'slb_len_mm', 0, 1038),
    ('tushare', 'moneyflow_ths', '个股资金流向（THS）', 0, '获取同花顺个股资金流向数据，每日盘后更新', '股票数据', '资金流向数据', 'moneyflow_ths', 6000, '单次最大6000，可根据日期或股票代码循环提取数据', '0', 'tushare', 'moneyflow_ths', 0, 1039),
    ('tushare', 'moneyflow_dc', '个股资金流向（DC）', 0, '获取东方财富个股资金流向数据，每日盘后更新，数据开始于20230911', '股票数据', '资金流向数据', 'moneyflow_dc', 5000, '单次最大获取6000条数据，可根据日期或股票代码循环提取数据', '0', 'tushare', 'moneyflow_dc', 0, 1040),
    ('tushare', 'moneyflow_cnt_ths', '板块资金流向（THS)', 0, '获取同花顺概念板块每日资金流向', '股票数据', '资金流向数据', 'moneyflow_cnt_ths', 6000, '单次最大可调取5000条数据，可以根据日期和代码循环提取全部数据', '0', 'tushare', 'moneyflow_cnt_ths', 0, 1041),
    ('tushare', 'moneyflow_ind_ths', '行业资金流向（THS）', 0, '获取同花顺行业资金流向，每日盘后更新', '股票数据', '资金流向数据', 'moneyflow_ind_ths', 6000, '单次最大可调取5000条数据，可以根据日期和代码循环提取全部数据', '0', 'tushare', 'moneyflow_ind_ths', 0, 1042),
    ('tushare', 'moneyflow_ind_dc', '板块资金流向（DC）', 0, '获取东方财富板块资金流向，每天盘后更新', '股票数据', '资金流向数据', 'moneyflow_ind_dc', 6000, '单次最大可调取5000条数据，可以根据日期和代码循环提取全部数据', '0', 'tushare', 'moneyflow_ind_dc', 0, 1043),
    ('tushare', 'moneyflow_mkt_dc', '大盘资金流向（DC）', 0, '获取东方财富大盘资金流向数据，每日盘后更新', '股票数据', '资金流向数据', 'moneyflow_mkt_dc', 120, '单次最大3000条，可根据日期或日期区间循环获取', '0', 'tushare', 'moneyflow_mkt_dc', 0, 1044),
    ('tushare', 'moneyflow_hsgt', '沪深港通资金流向', 0, '获取沪股通、深股通、港股通每日资金流向数据，每次最多返回300条记录，总量不限制', '股票数据', '资金流向数据', 'moneyflow_hsgt', 0, '', '0', 'tushare', 'moneyflow_hsgt', 0, 1045),
    ('tushare', 'limit_list_ths', '同花顺涨跌停榜单', 0, '获取同花顺每日涨跌停榜单数据，历史数据从20231101开始提供，增量每天16点左右更新', '股票数据', '打板专题数据', 'limit_list_ths', 8000, '单次最大4000条，可根据日期或股票代码循环提取', '0', 'tushare', 'limit_list_ths', 0, 1046),
    ('tushare', 'limit_step', '涨停股票连板天梯', 0, '获取每天连板个数晋级的股票，可以分析出每天连续涨停进阶个数，判断强势热度', '股票数据', '打板专题数据', 'limit_step', 8000, '单次最大2000行数据，可根据股票代码或者日期循环提取全部', '0', 'tushare', 'limit_step', 0, 1047),
    ('tushare', 'limit_cpt_list', '涨停最强板块统计', 0, '获取每天涨停股票最多最强的概念板块，可以分析强势板块的轮动，判断资金动向', '股票数据', '打板专题数据', 'limit_cpt_list', 8000, '单次最大2000行数据，可根据股票代码或者日期循环提取全部', '0', 'tushare', 'limit_cpt_list', 0, 1048),
    ('tushare', 'ths_index', '同花顺行业概念板块', 0, '获取同花顺板块指数，包括概念、行业、特色指数', '股票数据', '打板专题数据', 'ths_index', 6000, '单次最大返回5000行数据', '0', 'tushare', 'ths_index', 0, 1049),
    ('tushare', 'ths_daily', '同花顺概念和行业指数行情', 0, '获取同花顺板块指数行情', '股票数据', '打板专题数据', 'ths_daily', 0, '单次最大3000行数据（需6000积分），可根据指数代码、日期参数循环提取', '0', 'tushare', 'ths_daily', 0, 1050),
    ('tushare', 'ths_member', '同花顺行业概念成分', 0, '获取同花顺概念板块成分列表', '股票数据', '打板专题数据', 'ths_member', 0, '用户积累6000积分可调取，每分钟可调取200次，可按概念板块代码循环提取所有成分', '0', 'tushare', 'ths_member', 0, 1051),
    ('tushare', 'dc_index', '东方财富概念板块', 0, '获取东方财富每个交易日的概念板块数据，支持按日期查询', '股票数据', '打板专题数据', 'dc_index', 6000, '单次最大可获取5000条数据，历史数据可根据日期循环获取', '0', 'tushare', 'dc_index', 0, 1052),
    ('tushare', 'dc_daily', '东财概念和行业指数行情', 0, '获取东财概念板块、行业指数板块、地域板块行情数据，历史数据开始于2020年', '股票数据', '打板专题数据', 'dc_daily', 6000, '单次最大2000条数据，可根据日期参数循环获取', '0', 'tushare', 'dc_daily', 0, 1053),
    ('tushare', 'stk_auction', '开盘竞价成交（当日）', 0, '获取当日个股和ETF的集合竞价成交情况，每天9点25~29分之间可以获取当日的集合竞价成交数据', '股票数据', '打板专题数据', 'stk_auction', 0, '单次最大返回8000行数据，可根据日期或代码循环获取历史', '1', 'tushare', 'stk_auction', 0, 1054),
    ('tushare', 'hm_list', '市场游资最全名录', 0, '获取游资分类名录信息', '股票数据', '打板专题数据', 'hm_list', 5000, '单次最大1000条数据，目前总量未超过500', '0', 'tushare', 'hm_list', 0, 1055),
    ('tushare', 'hm_detail', '游资交易每日明细', 0, '获取每日游资交易明细，数据开始于2022年8。游资分类名录，请点击', '股票数据', '打板专题数据', 'hm_detail', 10000, '单次最多提取2000条记录，可循环调取，总量不限制', '0', 'tushare', 'hm_detail', 0, 1056),
    ('tushare', 'ths_hot', '同花顺App热榜数', 0, '获取同花顺App热榜数据，包括热股、概念板块、ETF、可转债、港美股等等，每日盘中提取4次，收盘后4次，最晚22点提取一次', '股票数据', '打板专题数据', 'ths_hot', 6000, '单次最大2000条，可根据日期等参数循环获取全部数据', '0', 'tushare', 'ths_hot', 0, 1057),
    ('tushare', 'tdx_index', '通达信板块信息', 0, '获取通达信板块基础信息，包括概念板块、行业、风格、地域等', '股票数据', '打板专题数据', 'tdx_index', 6000, '单次最大1000条数据，可根据日期参数循环提取', '0', 'tushare', 'tdx_index', 0, 1058),
    ('tushare', 'tdx_member', '通达信板块成分', 0, '获取通达信各板块成分股信息', '股票数据', '打板专题数据', 'tdx_member', 6000, '单次最大3000条数据，可以根据日期和板块代码循环提取', '0', 'tushare', 'tdx_member', 0, 1059),
    ('tushare', 'tdx_daily', '通达信板块行情', 0, '获取通达信各板块行情，包括成交和估值等数据', '股票数据', '打板专题数据', 'tdx_daily', 6000, '单次提取最大3000条数据，可根据板块代码和日期参数循环提取', '0', 'tushare', 'tdx_daily', 0, 1060),
    ('tushare', 'kpl_concept_cons', '题材成分（开盘啦）', 0, '获取开盘啦概念题材的成分股', '股票数据', '打板专题数据', 'kpl_concept_cons', 5000, '单次最大3000条，可根据代码和日期循环获取全部数据', '0', 'tushare', 'kpl_concept_cons', 0, 1061),
    ('tushare', 'dc_concept', '题材数据（东方财富）', 0, '获取东财概念题材列表，每天盘后更新', '股票数据', '打板专题数据', 'dc_concept', 6000, '单次最大5000，可根据日期循环获取历史数据,（数据从20260203开始）', '0', 'tushare', 'dc_concept', 0, 1062),
    ('tushare', 'dc_concept_cons', '题材成分（东方财富）', 0, '获取东方财富概念题材的成分股，每天盘后更新', '股票数据', '打板专题数据', 'dc_concept_cons', 6000, '单次最大3000，可根据日期循环获取历史数据,（数据从20260203开始）', '0', 'tushare', 'dc_concept_cons', 0, 1063),
    ('tushare', 'rt_idx_k', '指数实时日线', 0, '获取交易所指数实时日线行情，支持按代码或代码通配符一次性提取全部交易所指数实时日k线行情', '指数数据', '指数实时日线', 'rt_idx_k', 0, '', '1', 'tushare', 'rt_idx_k', 0, 1064),
    ('tushare', 'rt_idx_min', '指数实时分钟', 0, '获取交易所指数实时分钟数据，包括1~60min', '指数数据', '指数实时分钟', 'rt_idx_min', 0, '单次最大1000行数据，可以通过股票代码提取数据，支持逗号分隔的多个代码同时提取', '1', 'tushare', 'rt_idx_min', 0, 1065),
    ('tushare', 'idx_mins', '指数历史分钟', 0, '获取交易所指数分钟数据，支持1min/5min/15min/30min/60min行情，提供Python SDK和 http Restful API两种方式', '指数数据', '指数历史分钟', 'idx_mins', 0, '单次最大8000行数据，可以通过指数代码和时间循环获取，本接口可以提供超过10年历史分钟数据', '1', 'tushare', 'idx_mins', 0, 1066),
    ('tushare', 'sw_daily', '申万行业指数日行情', 0, '获取申万行业日线行情（默认是申万2021版行情）', '指数数据', '申万行业指数日行情', 'sw_daily', 0, '单次最大4000行数据，可通过指数代码和日期参数循环提取，5000积分可调取', '0', 'tushare', 'sw_daily', 0, 1067),
    ('tushare', 'rt_sw_k', '申万实时行情', 0, '获取申万行业指数的最新截面数据', '指数数据', '申万实时行情', 'rt_sw_k', 0, '', '1', 'tushare', 'rt_sw_k', 0, 1068),
    ('tushare', 'ci_index_member', '中信行业成分', 0, '按三级分类提取中信行业成分，可提供某个分类的所有成分，也可按股票代码提取所属分类，参数灵活', '指数数据', '中信行业成分', 'ci_index_member', 5000, '单次最大5000行，总量不限制', '0', 'tushare', 'ci_index_member', 0, 1069),
    ('tushare', 'ci_daily', '中信行业指数日行情', 0, '获取中信行业指数日线行情', '指数数据', '中信行业指数日行情', 'ci_daily', 5000, '单次最大4000条，可循环提取', '0', 'tushare', 'ci_daily', 0, 1070),
    ('tushare', 'idx_factor_pro', '指数技术面因子(专业版)', 0, '获取指数每日技术面因子数据，用于跟踪指数当前走势情况，数据由Tushare社区自产，覆盖全历史；输出参数_bfq表示不复权描述中说明了因子的默认传参，如需要特殊参数或者更多因子可以联系管理员评估，指数包括大盘指数 申万行业指数 中信指数', '指数数据', '指数技术面因子(专业版)', 'idx_factor_pro', 5000, '单次最大8000', '0', 'tushare', 'idx_factor_pro', 0, 1071),
    ('tushare', 'sz_daily_info', '深圳市场每日交易情况', 0, '获取深圳市场每日交易概况', '指数数据', '深圳市场每日交易情况', 'sz_daily_info', 2000, '单次最大2000，可循环获取，总量不限制', '0', 'tushare', 'sz_daily_info', 0, 1072),
    ('tushare', 'etf_basic', 'ETF基本信息', 0, '获取国内ETF基础信息，包括了QDII。数据来源与沪深交易所公开披露信息', 'ETF专题', 'ETF基本信息', 'etf_basic', 8000, '单次请求最大放回5000条数据（当前ETF总数未超过2000）', '0', 'tushare', 'etf_basic', 0, 1073),
    ('tushare', 'etf_index', 'ETF基准指数', 0, '获取ETF基准指数列表信息', 'ETF专题', 'ETF基准指数', 'etf_index', 8000, '单次请求最大返回5000行数据（当前未超过2000个）', '0', 'tushare', 'etf_index', 0, 1074),
    ('tushare', 'etf_share_size', 'ETF份额规模', 0, '获取沪深ETF每日份额和规模数据，能体现规模份额的变化，掌握ETF资金动向，同时提供每日净值和收盘价；数据指标是分批入库，建议在每日19点后提取；另外，涉及海外的ETF数据更新会晚一些属于正常情况', 'ETF专题', 'ETF份额规模', 'etf_share_size', 8000, '单次最大5000条，可根据代码或日期循环提取', '0', 'tushare', 'etf_share_size', 0, 1075),
    ('tushare', 'rt_etf_sz_iopv', 'ETF实时参考', 0, 'ETF实时净值和申购赎回数据参考，目前只提供深市', 'ETF专题', 'ETF实时参考', 'rt_etf_sz_iopv', 0, '单次最大5000条，完全覆盖当前总量', '1', 'tushare', 'rt_etf_sz_iopv', 0, 1076),
    ('tushare', 'fund_manager', '基金经理', 0, '获取公募基金经理数据，包括基金经理简历等数据', '公募基金', '基金经理', 'fund_manager', 500, '单次最大5000，支持分页提取数据', '0', 'tushare', 'fund_manager', 0, 1077),
    ('tushare', 'fund_factor_pro', '基金技术面因子(专业版)', 0, '获取场内基金每日技术面因子数据，用于跟踪场内基金当前走势情况，数据由Tushare社区自产，覆盖全历史；输出参数_bfq表示不复权，描述中说明了因子的默认传参，如需要特殊参数或者更多因子可以联系管理员评估', '公募基金', '基金技术面因子(专业版)', 'fund_factor_pro', 5000, '单次最大8000', '0', 'tushare', 'fund_factor_pro', 0, 1078),
    ('tushare', 'fut_weekly_monthly', '期货周/月线行情(每日更新)', 0, '期货周/月线行情(每日更新)', '期货数据', '期货周/月线行情(每日更新)', 'fut_weekly_monthly', 0, '单次最大6000', '0', 'tushare', 'fut_weekly_monthly', 0, 1079),
    ('tushare', 'rt_fut_min', '实时分钟行情', 0, '获取全市场期货合约实时分钟数据，支持1min/5min/15min/30min/60min行情，提供Python SDK、 http Restful API和websocket三种方式，如果需要主力合约分钟，请先通过主力', '期货数据', '实时分钟行情', 'rt_fut_min', 0, '每分钟可以请求500次，支持多个合约同时提取', '1', 'tushare', 'rt_fut_min', 0, 1080),
    ('tushare', 'fut_weekly_detail', '期货主要品种交易周报', 0, '获取期货交易所主要品种每周交易统计信息，数据从2010年3月开始', '期货数据', '期货主要品种交易周报', 'fut_weekly_detail', 600, '单次最大获取4000行数据', '0', 'tushare', 'fut_weekly_detail', 0, 1081),
    ('tushare', 'ft_limit', '期货合约涨跌停价格', 0, '获取所有期货合约每天的涨跌停价格及最低保证金率，数据开始于2005年', '期货数据', '期货合约涨跌停价格', 'ft_limit', 5000, '单次最大获取4000行数据，可以通过日期、合约代码等参数循环获取所有历史', '0', 'tushare', 'ft_limit', 0, 1082),
    ('tushare', 'sge_basic', '上海黄金基础信息', 0, '获取上海黄金交易所现货合约基础信息', '现货数据', '上海黄金基础信息', 'sge_basic', 5000, '单次最大100条，当前现货合约数不足20个，可以一次提取全部，不需要循环提取', '0', 'tushare', 'sge_basic', 0, 1083),
    ('tushare', 'sge_daily', '上海黄金现货日行情', 0, '获取上海黄金交易所现货合约日线行情', '现货数据', '上海黄金现货日行情', 'sge_daily', 2000, '单次最大2000，可循环或者分页提取', '0', 'tushare', 'sge_daily', 0, 1084),
    ('tushare', 'cb_call', '可转债赎回信息', 0, '获取可转债到期赎回、强制赎回等信息。数据来源于公开披露渠道，供个人和机构研究使用，请不要用于数据商业目的', '债券专题', '可转债赎回信息', 'cb_call', 0, '单次最大2000条数据，可以根据日期循环提取，本接口需5000积分', '0', 'tushare', 'cb_call', 0, 1085),
    ('tushare', 'cb_rate', '可转债票面利率', 0, '获取可转债票面利率', '债券专题', '可转债票面利率', 'cb_rate', 5000, '单次最大2000，总量不限制', '0', 'tushare', 'cb_rate', 0, 1086),
    ('tushare', 'cb_factor_pro', '可转债技术面因子(专业版)', 0, '获取可转债每日技术面因子数据，用于跟踪可转债当前走势情况，数据由Tushare社区自产，覆盖全历史；输出参数_bfq表示不复权，_qfq表示前复权 _hfq表示后复权，描述中说明了因子的默认传参，如需要特殊参数或者更多因子可以联系管理员评估', '债券专题', '可转债技术面因子(专业版)', 'cb_factor_pro', 5000, '单次调取最多返回10000条数据，可以通过日期参数循环', '0', 'tushare', 'cb_factor_pro', 0, 1087),
    ('tushare', 'cb_price_chg', '可转债转股价变动', 0, '获取可转债转股价变动', '债券专题', '可转债转股价变动', 'cb_price_chg', 0, '单次最大2000，总量不限制', '1', 'tushare', 'cb_price_chg', 0, 1088),
    ('tushare', 'cb_rating', '可转债债券评级', 0, '获取可转债评级历史记录', '债券专题', '可转债债券评级', 'cb_rating', 2000, '单次最大3000条，可根据代码或日期循环提取', '0', 'tushare', 'cb_rating', 0, 1089),
    ('tushare', 'repo_daily', '债券回购日行情', 0, '债券回购日行情', '债券专题', '债券回购日行情', 'repo_daily', 2000, '单次最大2000条，可多次提取，总量不限制', '0', 'tushare', 'repo_daily', 0, 1090),
    ('tushare', 'bc_otcqt', '柜台流通式债券报价', 0, '柜台流通式债券报价', '债券专题', '柜台流通式债券报价', 'bc_otcqt', 500, '单次最大2000条，可多次提取，总量不限制', '0', 'tushare', 'bc_otcqt', 0, 1091),
    ('tushare', 'bc_bestotcqt', '柜台流通式债券最优报价', 0, '柜台流通式债券最优报价', '债券专题', '柜台流通式债券最优报价', 'bc_bestotcqt', 500, '单次最大2000，可多次提取，总量不限制', '0', 'tushare', 'bc_bestotcqt', 0, 1092),
    ('tushare', 'bond_blk', '大宗交易', 0, '获取沪深交易所债券大宗交易数据', '债券专题', '大宗交易', 'bond_blk', 0, '单次最大1000条', '1', 'tushare', 'bond_blk', 0, 1093),
    ('tushare', 'bond_blk_detail', '大宗交易明细', 0, '获取沪深交易所债券大宗交易数据', '债券专题', '大宗交易明细', 'bond_blk_detail', 0, '单次最大1000条', '1', 'tushare', 'bond_blk_detail', 0, 1094),
    ('tushare', 'yc_cb', '国债收益率曲线', 0, '获取中债收益率曲线，目前可获取中债国债收益率曲线即期和到期收益率曲线数据', '债券专题', '国债收益率曲线', 'yc_cb', 0, '单次最大2000，总量不限制，可循环提取', '1', 'tushare', 'yc_cb', 0, 1095),
    ('tushare', 'eco_cal', '全球财经事件', 0, '获取全球财经日历、包括经济事件数据更新', '债券专题', '全球财经事件', 'eco_cal', 2000, '单次最大获取100行数据', '0', 'tushare', 'eco_cal', 0, 1096),
    ('tushare', 'hk_tradecal', '港股交易日历', 0, '获取交易日历', '港股数据', '港股交易日历', 'hk_tradecal', 2000, '单次最大2000', '0', 'tushare', 'hk_tradecal', 0, 1097),
    ('tushare', 'hk_daily_adj', '港股复权行情', 0, '获取港股复权行情，提供股票股本、市值和成交及换手多个数据指标', '港股数据', '港股复权行情', 'hk_daily_adj', 0, '单次最大可以提取6000条数据，可循环获取全部，支持分页提取', '0', 'tushare', 'hk_daily_adj', 0, 1098),
    ('tushare', 'hk_adjfactor', '港股复权因子', 0, '获取港股每日复权因子数据，每天滚动刷新', '港股数据', '港股复权因子', 'hk_adjfactor', 0, '单次最大6000行数据，可以根据日期循环', '1', 'tushare', 'hk_adjfactor', 0, 1099),
    ('tushare', 'hk_income', '港股利润表', 0, '获取港股上市公司财务利润表数据', '港股数据', '港股利润表', 'hk_income', 15000, '', '0', 'tushare', 'hk_income', 0, 1100),
    ('tushare', 'hk_balancesheet', '港股资产负债表', 0, '获取港股上市公司资产负债表', '港股数据', '港股资产负债表', 'hk_balancesheet', 15000, '', '0', 'tushare', 'hk_balancesheet', 0, 1101),
    ('tushare', 'hk_cashflow', '港股现金流量表', 0, '获取港股上市公司现金流量表数据', '港股数据', '港股现金流量表', 'hk_cashflow', 15000, '', '0', 'tushare', 'hk_cashflow', 0, 1102),
    ('tushare', 'hk_fina_indicator', '港股财务指标数据', 0, '获取港股上市公司财务指标数据，为避免服务器压力，现阶段每次请求最多返回200条记录，可通过设置日期多次请求获取更多数据', '港股数据', '港股财务指标数据', 'hk_fina_indicator', 15000, '', '0', 'tushare', 'hk_fina_indicator', 0, 1103),
    ('tushare', 'us_tradecal', '美股交易日历', 0, '获取美股交易日历信息', '美股数据', '美股交易日历', 'us_tradecal', 0, '单次最大6000，可根据日期阶段获取', '0', 'tushare', 'us_tradecal', 0, 1104),
    ('tushare', 'us_daily_adj', '美股复权行情', 0, '获取美股复权行情，支持美股全市场股票，提供股本、市值、复权因子和成交信息等多个数据指标', '美股数据', '美股复权行情', 'us_daily_adj', 0, '单次最大可以提取8000条数据，可循环获取全部，支持分页提取', '0', 'tushare', 'us_daily_adj', 0, 1105),
    ('tushare', 'us_adjfactor', '美股复权因子', 0, '获取美股每日复权因子数据，在每天美股收盘后滚动刷新', '美股数据', '美股复权因子', 'us_adjfactor', 0, '单次最大15000行数据，可以根据日期循环', '1', 'tushare', 'us_adjfactor', 0, 1106),
    ('tushare', 'us_income', '美股利润表', 0, '获取美股上市公司财务利润表数据（目前只覆盖主要美股和中概股）', '美股数据', '美股利润表', 'us_income', 15000, '', '0', 'tushare', 'us_income', 0, 1107),
    ('tushare', 'us_balancesheet', '美股资产负债表', 0, '获取美股上市公司资产负债表（目前只覆盖主要美股和中概股）', '美股数据', '美股资产负债表', 'us_balancesheet', 15000, '', '0', 'tushare', 'us_balancesheet', 0, 1108),
    ('tushare', 'us_cashflow', '美股现金流量表', 0, '获取美股上市公司现金流量表数据（目前只覆盖主要美股和中概股）', '美股数据', '美股现金流量表', 'us_cashflow', 15000, '', '0', 'tushare', 'us_cashflow', 0, 1109),
    ('tushare', 'us_fina_indicator', '美股财务指标数据', 0, '获取美股上市公司财务指标数据，目前只覆盖主要美股和中概股。为避免服务器压力，现阶段每次请求最多返回200条记录，可通过设置日期多次请求获取更多数据', '美股数据', '美股财务指标数据', 'us_fina_indicator', 15000, '', '0', 'tushare', 'us_fina_indicator', 0, 1110),
    ('tushare', 'tmt_twincome', '台湾电子产业月营收', 0, '获取台湾TMT电子产业领域各类产品月度营收数据', '行业经济', 'TMT行业', 'tmt_twincome', 0, '', '0', 'tushare', 'tmt_twincome', 0, 1111),
    ('tushare', 'tmt_twincomedetail', '台湾电子产业月营收明细', 0, '获取台湾TMT行业上市公司各类产品月度营收情况', '行业经济', 'TMT行业', 'tmt_twincomedetail', 0, '', '0', 'tushare', 'tmt_twincomedetail', 0, 1112),
    ('tushare', 'bo_daily', '电影日度票房', 0, '获取电影日度票房', '行业经济', 'TMT行业', 'bo_daily', 0, '', '0', 'tushare', 'bo_daily', 0, 1113),
    ('tushare', 'bo_cinema', '影院日度票房', 0, '获取每日各影院的票房数据', '行业经济', 'TMT行业', 'bo_cinema', 0, '', '0', 'tushare', 'bo_cinema', 0, 1114),
    ('tushare', 'film_record', '全国电影剧本备案数据', 0, '获取全国电影剧本备案的公示数据', '行业经济', 'TMT行业', 'film_record', 0, '单次最大500，总量不限制', '0', 'tushare', 'film_record', 0, 1115),
    ('tushare', 'teleplay_record', '全国电视剧备案公示数据', 0, '获取2009年以来全国拍摄制作电视剧备案公示数据', '行业经济', 'TMT行业', 'teleplay_record', 0, '单次最大1000，总量不限制', '0', 'tushare', 'teleplay_record', 0, 1116),
    ('tushare', 'shibor_quote', 'Shibor报价数据', 0, 'Shibor报价数据', '宏观经济', '国内宏观', 'shibor_quote', 120, '单次最大4000行数据，总量不限制，可通过设置开始和结束日期分段获取', '0', 'tushare', 'shibor_quote', 0, 1117),
    ('tushare', 'libor', 'Libor利率', 0, 'Libor拆借利率', '宏观经济', '国内宏观', 'libor', 120, '单次最大4000行数据，总量不限制，可通过设置开始和结束日期分段获取', '0', 'tushare', 'libor', 0, 1118),
    ('tushare', 'hibor', 'Hibor利率', 0, 'Hibor利率', '宏观经济', '国内宏观', 'hibor', 120, '单次最大4000行数据，总量不限制，可通过设置开始和结束日期分段获取', '0', 'tushare', 'hibor', 0, 1119),
    ('tushare', 'wz_index', '温州民间借贷利率', 0, '温州民间借贷利率，即温州指数', '宏观经济', '国内宏观', 'wz_index', 2000, '不限量，一次可取全部指标全部历史数据', '0', 'tushare', 'wz_index', 0, 1120),
    ('tushare', 'gz_index', '广州民间借贷利率', 0, '广州民间借贷利率', '宏观经济', '国内宏观', 'gz_index', 2000, '不限量，一次可取全部指标全部历史数据', '0', 'tushare', 'gz_index', 0, 1121),
    ('tushare', 'cn_ppi', '工业生产者出厂价格指数（PPI）', 0, '获取PPI工业生产者出厂价格指数数据', '宏观经济', '国内宏观', 'cn_ppi', 600, '单次最大5000，一次可以提取全部数据', '0', 'tushare', 'cn_ppi', 0, 1122),
    ('tushare', 'cn_m', '货币供应量（月）', 0, '获取货币供应量之月度数据', '宏观经济', '国内宏观', 'cn_m', 600, '单次最大5000，一次可以提取全部数据', '0', 'tushare', 'cn_m', 0, 1123),
    ('tushare', 'sf_month', '社融增量（月度）', 0, '获取月度社会融资数据', '宏观经济', '国内宏观', 'sf_month', 2000, '单次最大2000条数据，可循环提取', '0', 'tushare', 'sf_month', 0, 1124),
    ('tushare', 'cn_pmi', '采购经理指数（PMI）', 0, '采购经理人指数', '宏观经济', '国内宏观', 'cn_pmi', 2000, '单次最大2000，一次可以提取全部数据', '0', 'tushare', 'cn_pmi', 0, 1125),
    ('tushare', 'us_tycr', '国债收益率曲线利率', 0, '获取美国每日国债收益率曲线利率', '宏观经济', '国际宏观', 'us_tycr', 120, '单次最大可获取2000条数据', '0', 'tushare', 'us_tycr', 0, 1126),
    ('tushare', 'us_trycr', '国债实际收益率曲线利率', 0, '国债实际收益率曲线利率', '宏观经济', '国际宏观', 'us_trycr', 120, '单次最大可获取2000行数据，可循环获取', '0', 'tushare', 'us_trycr', 0, 1127),
    ('tushare', 'us_tbr', '短期国债利率', 0, '获取美国短期国债利率数据', '宏观经济', '国际宏观', 'us_tbr', 120, '单次最大可获取2000行数据，可循环获取', '0', 'tushare', 'us_tbr', 0, 1128),
    ('tushare', 'us_tltr', '国债长期利率', 0, '国债长期利率', '宏观经济', '国际宏观', 'us_tltr', 120, '单次最大可获取2000行数据，可循环获取', '0', 'tushare', 'us_tltr', 0, 1129),
    ('tushare', 'us_trltr', '国债长期利率平均值', 0, '国债实际长期利率平均值', '宏观经济', '国际宏观', 'us_trltr', 120, '单次最大可获取2000行数据，可循环获取', '0', 'tushare', 'us_trltr', 0, 1130),
    ('tushare', 'npr', '国家政策库', 0, '获取国家行政机关公开披露的各类法规、条例政策、批复、通知等文本数据', '资讯数据', '国家政策库', 'npr', 0, '单次最大500条，可根据参数循环提取', '1', 'tushare', 'npr', 0, 1131),
    ('tushare', 'research_report', '券商研究报告', 0, '获取券商研究报告-个股、行业等，历史数据从20170101开始提供，增量每天两次更新', '资讯数据', '券商研究报告', 'research_report', 0, '单次最大1000条，可根据日期或券商名称代码循环提取，每天总量不限制', '1', 'tushare', 'research_report', 0, 1132),
    ('tushare', 'anns_d', '上市公司公告', 0, '获取全量公告数据，提供pdf下载URL', '资讯数据', '上市公司公告', 'anns_d', 0, '单次最大2000条数，可以跟进日期循环获取全量', '1', 'tushare', 'anns_d', 0, 1133),
    ('tushare', 'fund_sales_ratio', '各渠道公募基金销售保有规模占比', 0, '获取各渠道公募基金销售保有规模占比数据，年度更新', '财富管理', '基金销售行业数据', 'fund_sales_ratio', 0, '单次最大100行数据，数据从2015年开始公布，当前数据量很小', '0', 'tushare', 'fund_sales_ratio', 0, 1134),
    ('tushare', 'fund_sales_vol', '销售机构公募基金销售保有规模', 0, '获取销售机构公募基金销售保有规模数据，本数据从2021年Q1开始公布，季度更新', '财富管理', '基金销售行业数据', 'fund_sales_vol', 0, '单次最大500行数据，目前总量只有100行，未来随着数据量增加会提高上限', '0', 'tushare', 'fund_sales_vol', 0, 1135);

INSERT INTO `quantmate`.`data_source_items`
  (`source`, `item_key`, `item_name`, `enabled`, `description`, `category`, `sub_category`, `api_name`, `permission_points`, `rate_limit_note`, `requires_permission`, `target_database`, `target_table`, `table_created`, `sync_priority`)
SELECT
  `source`, `item_key`, `item_name`, `enabled`, `description`, `category`, `sub_category`, `api_name`, `permission_points`, `rate_limit_note`, `requires_permission`, `target_database`, `target_table`, `table_created`, `sync_priority`
FROM `_tmp_tushare_catalog_refresh`
ON DUPLICATE KEY UPDATE
  item_name = VALUES(item_name),
  description = VALUES(description),
  category = VALUES(category),
  sub_category = VALUES(sub_category),
  api_name = VALUES(api_name),
  permission_points = VALUES(permission_points),
  rate_limit_note = VALUES(rate_limit_note),
  requires_permission = VALUES(requires_permission),
  target_database = VALUES(target_database),
  target_table = VALUES(target_table),
  sync_priority = VALUES(sync_priority);

DELETE dsi
FROM `quantmate`.`data_source_items` dsi
LEFT JOIN `_tmp_tushare_catalog_refresh` tmp
  ON tmp.source = dsi.source AND tmp.item_key = dsi.item_key
WHERE dsi.source = 'tushare'
  AND tmp.item_key IS NULL;

DELETE ssi
FROM `quantmate`.`sync_status_init` ssi
LEFT JOIN `_tmp_tushare_catalog_refresh` tmp
  ON tmp.source = ssi.source AND tmp.item_key = ssi.interface_key
WHERE ssi.source = 'tushare'
  AND tmp.item_key IS NULL;

DELETE dss
FROM `quantmate`.`data_sync_status` dss
LEFT JOIN `_tmp_tushare_catalog_refresh` tmp
  ON tmp.source = dss.source AND tmp.item_key = dss.interface_key
WHERE dss.source = 'tushare'
  AND tmp.item_key IS NULL;

DROP TEMPORARY TABLE `_tmp_tushare_catalog_refresh`;

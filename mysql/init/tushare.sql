-- =============================================================================
-- QuantMate Tushare bootstrap schema
-- Database: tushare
--
-- Only low-permission Tushare tables are precreated here.
-- All other Tushare interface tables are created on demand from
-- app/datasync/sources/tushare/ddl.py when an interface is enabled or when
-- sync/backfill/init executes that specific interface.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tushare CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE tushare;

CREATE TABLE IF NOT EXISTS ingest_audit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    api_name VARCHAR(64) NOT NULL,
    params JSON,
    status VARCHAR(32) DEFAULT 'running',
    fetched_rows INT DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    INDEX idx_audit_api (api_name),
    INDEX idx_audit_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_company (
    ts_code VARCHAR(20) NOT NULL PRIMARY KEY,
    com_name VARCHAR(255),
    com_id VARCHAR(64),
    exchange VARCHAR(10),
    chairman VARCHAR(50),
    manager VARCHAR(50),
    secretary VARCHAR(50),
    reg_capital DECIMAL(18,4),
    setup_date DATE,
    province VARCHAR(20),
    city VARCHAR(30),
    introduction TEXT,
    website VARCHAR(200),
    email VARCHAR(100),
    office VARCHAR(200),
    employees INT,
    main_business TEXT,
    business_scope TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS new_share (
    ts_code VARCHAR(32) NOT NULL,
    sub_code VARCHAR(32),
    name VARCHAR(255),
    ipo_date DATE,
    issue_date DATE,
    market_amount DECIMAL(18,4),
    issue_price DECIMAL(12,2),
    pe DECIMAL(12,2),
    limit_amount DECIMAL(18,4),
    funds DECIMAL(18,4),
    ballot DECIMAL(18,4),
    amount BIGINT,
    market VARCHAR(32),
    PRIMARY KEY (ts_code, ipo_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_daily_ts (ts_code),
    INDEX idx_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS suspend_d (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    suspend_timing VARCHAR(64),
    suspend_type VARCHAR(32),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_suspend_d_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS report_rc (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) DEFAULT NULL,
    symbol VARCHAR(32) DEFAULT NULL,
    code VARCHAR(32) DEFAULT NULL,
    name VARCHAR(255) DEFAULT NULL,
    exchange VARCHAR(32) DEFAULT NULL,
    market VARCHAR(64) DEFAULT NULL,
    trade_date DATE DEFAULT NULL,
    ann_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL,
    key_hash CHAR(64) NOT NULL,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_report_rc_key_hash (key_hash),
    INDEX idx_report_rc_ts_code (ts_code),
    INDEX idx_report_rc_trade_date (trade_date),
    INDEX idx_report_rc_ann_date (ann_date),
    INDEX idx_report_rc_end_date (end_date),
    INDEX idx_report_rc_code (code),
    INDEX idx_report_rc_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS us_basic (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) DEFAULT NULL,
    symbol VARCHAR(32) DEFAULT NULL,
    code VARCHAR(32) DEFAULT NULL,
    name VARCHAR(255) DEFAULT NULL,
    exchange VARCHAR(32) DEFAULT NULL,
    market VARCHAR(64) DEFAULT NULL,
    trade_date DATE DEFAULT NULL,
    ann_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL,
    key_hash CHAR(64) NOT NULL,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_us_basic_key_hash (key_hash),
    INDEX idx_us_basic_ts_code (ts_code),
    INDEX idx_us_basic_trade_date (trade_date),
    INDEX idx_us_basic_ann_date (ann_date),
    INDEX idx_us_basic_end_date (end_date),
    INDEX idx_us_basic_code (code),
    INDEX idx_us_basic_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS us_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) DEFAULT NULL,
    symbol VARCHAR(32) DEFAULT NULL,
    code VARCHAR(32) DEFAULT NULL,
    name VARCHAR(255) DEFAULT NULL,
    exchange VARCHAR(32) DEFAULT NULL,
    market VARCHAR(64) DEFAULT NULL,
    trade_date DATE DEFAULT NULL,
    ann_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL,
    key_hash CHAR(64) NOT NULL,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_us_daily_key_hash (key_hash),
    INDEX idx_us_daily_ts_code (ts_code),
    INDEX idx_us_daily_trade_date (trade_date),
    INDEX idx_us_daily_ann_date (ann_date),
    INDEX idx_us_daily_end_date (end_date),
    INDEX idx_us_daily_code (code),
    INDEX idx_us_daily_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS shibor_lpr (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) DEFAULT NULL,
    symbol VARCHAR(32) DEFAULT NULL,
    code VARCHAR(32) DEFAULT NULL,
    name VARCHAR(255) DEFAULT NULL,
    exchange VARCHAR(32) DEFAULT NULL,
    market VARCHAR(64) DEFAULT NULL,
    trade_date DATE DEFAULT NULL,
    ann_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL,
    key_hash CHAR(64) NOT NULL,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_shibor_lpr_key_hash (key_hash),
    INDEX idx_shibor_lpr_ts_code (ts_code),
    INDEX idx_shibor_lpr_trade_date (trade_date),
    INDEX idx_shibor_lpr_ann_date (ann_date),
    INDEX idx_shibor_lpr_end_date (end_date),
    INDEX idx_shibor_lpr_code (code),
    INDEX idx_shibor_lpr_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

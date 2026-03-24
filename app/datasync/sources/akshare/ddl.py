"""DDL definitions for AkShare tables."""

INDEX_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS index_daily (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    close DECIMAL(16,4),
    volume BIGINT,
    amount DECIMAL(20,4),
    PRIMARY KEY (index_code, trade_date),
    INDEX idx_index_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

INDEX_SPOT_DDL = """
CREATE TABLE IF NOT EXISTS stock_zh_index_spot (
    symbol       VARCHAR(20) NOT NULL,
    name         VARCHAR(50) DEFAULT NULL,
    latest_price DECIMAL(12,4) DEFAULT NULL,
    change_pct   DECIMAL(10,4) DEFAULT NULL,
    change_amount DECIMAL(12,4) DEFAULT NULL,
    volume       BIGINT DEFAULT NULL,
    amount       DECIMAL(18,4) DEFAULT NULL,
    amplitude    DECIMAL(10,4) DEFAULT NULL,
    high         DECIMAL(12,4) DEFAULT NULL,
    low          DECIMAL(12,4) DEFAULT NULL,
    open         DECIMAL(12,4) DEFAULT NULL,
    prev_close   DECIMAL(12,4) DEFAULT NULL,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

ETF_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS fund_etf_daily (
    symbol       VARCHAR(20) NOT NULL,
    trade_date   DATE NOT NULL,
    open         DECIMAL(10,4) DEFAULT NULL,
    high         DECIMAL(10,4) DEFAULT NULL,
    low          DECIMAL(10,4) DEFAULT NULL,
    close        DECIMAL(10,4) DEFAULT NULL,
    volume       BIGINT DEFAULT NULL,
    amount       DECIMAL(18,4) DEFAULT NULL,
    outstanding_share DECIMAL(18,4) DEFAULT NULL,
    turnover     DECIMAL(10,6) DEFAULT NULL,
    PRIMARY KEY (symbol, trade_date),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

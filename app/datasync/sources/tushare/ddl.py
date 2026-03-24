"""DDL definitions for all Tushare tables."""

STOCK_BASIC_DDL = """
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
    PRIMARY KEY (ts_code),
    INDEX idx_stock_basic_symbol (symbol),
    INDEX idx_stock_basic_exchange (exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_DAILY_DDL = """
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

ADJ_FACTOR_DDL = """
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DECIMAL(24,12),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_DIVIDEND_DDL = """
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
    UNIQUE INDEX ux_stock_dividend_ts_ann (ts_code, ann_date),
    INDEX idx_div_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

TOP10_HOLDERS_DDL = """
CREATE TABLE IF NOT EXISTS top10_holders (
    ts_code       VARCHAR(20) NOT NULL,
    ann_date      DATE DEFAULT NULL,
    end_date      DATE NOT NULL,
    holder_name   VARCHAR(200) NOT NULL,
    hold_amount   DECIMAL(18,4) DEFAULT NULL,
    hold_ratio    DECIMAL(10,6) DEFAULT NULL,
    hold_float_ratio DECIMAL(10,6) DEFAULT NULL,
    hold_change   DECIMAL(18,4) DEFAULT NULL,
    holder_type   VARCHAR(20) DEFAULT NULL,
    INDEX idx_ts_code_end (ts_code, end_date),
    INDEX idx_end_date (end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_WEEKLY_DDL = """
CREATE TABLE IF NOT EXISTS stock_weekly (
    ts_code       VARCHAR(32) NOT NULL,
    trade_date    DATE NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_weekly_ts (ts_code),
    INDEX idx_weekly_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_MONTHLY_DDL = """
CREATE TABLE IF NOT EXISTS stock_monthly (
    ts_code       VARCHAR(32) NOT NULL,
    trade_date    DATE NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_monthly_ts (ts_code),
    INDEX idx_monthly_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

INDEX_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS index_daily (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    close DECIMAL(16,4),
    pre_close DECIMAL(16,4),
    change_amount DECIMAL(16,4),
    pct_change DECIMAL(10,4),
    vol BIGINT,
    amount DECIMAL(20,4),
    PRIMARY KEY (index_code, trade_date),
    INDEX idx_index_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

INDEX_WEEKLY_DDL = """
CREATE TABLE IF NOT EXISTS index_weekly (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    close DECIMAL(16,4),
    pre_close DECIMAL(16,4),
    change_amount DECIMAL(16,4),
    pct_change DECIMAL(10,4),
    vol BIGINT,
    amount DECIMAL(20,4),
    PRIMARY KEY (index_code, trade_date),
    INDEX idx_index_weekly_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

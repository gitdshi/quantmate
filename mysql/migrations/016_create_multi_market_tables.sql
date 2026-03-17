-- Migration 016: Multi-market support tables (HK / US)
-- Stores daily OHLCV data for Hong Kong and US markets

-- ── Hong Kong market (HKEX) ──

CREATE TABLE IF NOT EXISTS `tushare`.`hk_stock_basic` (
    `ts_code`       VARCHAR(20)     NOT NULL    COMMENT 'Tushare code e.g. 00700.HK',
    `name`          VARCHAR(100)    DEFAULT NULL,
    `enname`        VARCHAR(200)    DEFAULT NULL,
    `industry`      VARCHAR(100)    DEFAULT NULL,
    `area`          VARCHAR(50)     DEFAULT 'HK',
    `market`        VARCHAR(20)     DEFAULT 'MAIN'  COMMENT 'MAIN, GEM',
    `list_date`     VARCHAR(8)      DEFAULT NULL,
    `delist_date`   VARCHAR(8)      DEFAULT NULL,
    `list_status`   VARCHAR(2)      DEFAULT 'L'     COMMENT 'L=listed, D=delisted',
    `curr_type`     VARCHAR(5)      DEFAULT 'HKD',
    `updated_at`    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ts_code`),
    KEY `idx_hksb_industry` (`industry`),
    KEY `idx_hksb_status`   (`list_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tushare`.`hk_stock_daily` (
    `ts_code`       VARCHAR(20)     NOT NULL,
    `trade_date`    VARCHAR(8)      NOT NULL,
    `open`          DECIMAL(12,4)   DEFAULT NULL,
    `high`          DECIMAL(12,4)   DEFAULT NULL,
    `low`           DECIMAL(12,4)   DEFAULT NULL,
    `close`         DECIMAL(12,4)   DEFAULT NULL,
    `vol`           DECIMAL(18,2)   DEFAULT NULL    COMMENT 'Volume in shares',
    `amount`        DECIMAL(18,4)   DEFAULT NULL    COMMENT 'Turnover in HKD',
    `pct_chg`       DECIMAL(10,4)   DEFAULT NULL,
    PRIMARY KEY (`ts_code`, `trade_date`),
    KEY `idx_hksd_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── US market (NYSE / NASDAQ) ──

CREATE TABLE IF NOT EXISTS `tushare`.`us_stock_basic` (
    `ts_code`       VARCHAR(20)     NOT NULL    COMMENT 'e.g. AAPL, TSLA',
    `name`          VARCHAR(200)    DEFAULT NULL,
    `enname`        VARCHAR(200)    DEFAULT NULL,
    `industry`      VARCHAR(100)    DEFAULT NULL,
    `exchange`      VARCHAR(20)     DEFAULT NULL    COMMENT 'NYSE, NASDAQ, AMEX',
    `area`          VARCHAR(50)     DEFAULT 'US',
    `market_cap`    DECIMAL(18,2)   DEFAULT NULL,
    `list_date`     VARCHAR(8)      DEFAULT NULL,
    `delist_date`   VARCHAR(8)      DEFAULT NULL,
    `list_status`   VARCHAR(2)      DEFAULT 'L',
    `curr_type`     VARCHAR(5)      DEFAULT 'USD',
    `updated_at`    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ts_code`),
    KEY `idx_ussb_exchange` (`exchange`),
    KEY `idx_ussb_status`   (`list_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tushare`.`us_stock_daily` (
    `ts_code`       VARCHAR(20)     NOT NULL,
    `trade_date`    VARCHAR(8)      NOT NULL,
    `open`          DECIMAL(12,4)   DEFAULT NULL,
    `high`          DECIMAL(12,4)   DEFAULT NULL,
    `low`           DECIMAL(12,4)   DEFAULT NULL,
    `close`         DECIMAL(12,4)   DEFAULT NULL,
    `vol`           DECIMAL(18,2)   DEFAULT NULL,
    `amount`        DECIMAL(18,4)   DEFAULT NULL,
    `pct_chg`       DECIMAL(10,4)   DEFAULT NULL,
    PRIMARY KEY (`ts_code`, `trade_date`),
    KEY `idx_ussd_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Multi-market exchange reference ──

CREATE TABLE IF NOT EXISTS `quantmate`.`market_exchanges` (
    `code`          VARCHAR(20)     NOT NULL    COMMENT 'SSE, SZSE, HKEX, NYSE, NASDAQ',
    `name`          VARCHAR(100)    NOT NULL,
    `country`       VARCHAR(5)      NOT NULL    COMMENT 'CN, HK, US',
    `timezone`      VARCHAR(50)     NOT NULL    COMMENT 'e.g. Asia/Shanghai',
    `currency`      VARCHAR(5)      NOT NULL    COMMENT 'CNY, HKD, USD',
    `open_time`     TIME            DEFAULT NULL,
    `close_time`    TIME            DEFAULT NULL,
    `enabled`       TINYINT(1)      DEFAULT 1,
    `updated_at`    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `quantmate`.`market_exchanges` (`code`, `name`, `country`, `timezone`, `currency`, `open_time`, `close_time`) VALUES
('SSE',    'Shanghai Stock Exchange',    'CN', 'Asia/Shanghai',    'CNY', '09:30:00', '15:00:00'),
('SZSE',   'Shenzhen Stock Exchange',    'CN', 'Asia/Shanghai',    'CNY', '09:30:00', '15:00:00'),
('BSE',    'Beijing Stock Exchange',     'CN', 'Asia/Shanghai',    'CNY', '09:30:00', '15:00:00'),
('HKEX',   'Hong Kong Stock Exchange',   'HK', 'Asia/Hong_Kong',   'HKD', '09:30:00', '16:00:00'),
('NYSE',   'New York Stock Exchange',    'US', 'America/New_York', 'USD', '09:30:00', '16:00:00'),
('NASDAQ', 'NASDAQ',                     'US', 'America/New_York', 'USD', '09:30:00', '16:00:00');

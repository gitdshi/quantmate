-- Create missing tables in vnpy_data database

USE vnpy_data;

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

CREATE TABLE IF NOT EXISTS dbtickoverview (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(45) NOT NULL,
    exchange VARCHAR(45) NOT NULL,
    count INT,
    start DATETIME,
    end DATETIME,
    UNIQUE KEY idx_tick_overview_unique (symbol, exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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

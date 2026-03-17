-- Migration 014: AkShare Data Tables + Minute Data

-- AkShare: 指数实时行情
CREATE TABLE IF NOT EXISTS `akshare`.`stock_zh_index_spot` (
  `symbol`       VARCHAR(20) NOT NULL,
  `name`         VARCHAR(50) DEFAULT NULL,
  `latest_price` DECIMAL(12,4) DEFAULT NULL,
  `change_pct`   DECIMAL(10,4) DEFAULT NULL COMMENT '涨跌幅',
  `change_amount` DECIMAL(12,4) DEFAULT NULL COMMENT '涨跌额',
  `volume`       BIGINT DEFAULT NULL,
  `amount`       DECIMAL(18,4) DEFAULT NULL COMMENT '成交金额',
  `amplitude`    DECIMAL(10,4) DEFAULT NULL COMMENT '振幅',
  `high`         DECIMAL(12,4) DEFAULT NULL,
  `low`          DECIMAL(12,4) DEFAULT NULL,
  `open`         DECIMAL(12,4) DEFAULT NULL,
  `prev_close`   DECIMAL(12,4) DEFAULT NULL,
  `updated_at`   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- AkShare: ETF 日线
CREATE TABLE IF NOT EXISTS `akshare`.`fund_etf_daily` (
  `symbol`       VARCHAR(20) NOT NULL,
  `trade_date`   DATE NOT NULL,
  `open`         DECIMAL(10,4) DEFAULT NULL,
  `high`         DECIMAL(10,4) DEFAULT NULL,
  `low`          DECIMAL(10,4) DEFAULT NULL,
  `close`        DECIMAL(10,4) DEFAULT NULL,
  `volume`       BIGINT DEFAULT NULL,
  `amount`       DECIMAL(18,4) DEFAULT NULL,
  `outstanding_share` DECIMAL(18,4) DEFAULT NULL COMMENT '流通份额',
  `turnover`     DECIMAL(10,6) DEFAULT NULL COMMENT '换手率',
  PRIMARY KEY (`symbol`, `trade_date`),
  INDEX `idx_trade_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Register AkShare items
INSERT IGNORE INTO `quantmate`.`data_source_items` (`source`, `item_key`, `item_name`, `enabled`, `description`, `requires_permission`) VALUES
  ('akshare', 'stock_zh_index_spot', '指数实时行情', 1, 'A股指数实时报价', 0),
  ('akshare', 'fund_etf_daily',     'ETF日线',      1, 'ETF基金日K线数据', 0);

-- Minute-level data (month-partitioned)
CREATE TABLE IF NOT EXISTS `tushare`.`stock_minute` (
  `ts_code`    VARCHAR(20) NOT NULL,
  `trade_time` DATETIME NOT NULL,
  `period`     VARCHAR(5) NOT NULL COMMENT '1min/5min/15min/30min/60min',
  `open`       DECIMAL(10,4) DEFAULT NULL,
  `high`       DECIMAL(10,4) DEFAULT NULL,
  `low`        DECIMAL(10,4) DEFAULT NULL,
  `close`      DECIMAL(10,4) DEFAULT NULL,
  `volume`     BIGINT DEFAULT NULL,
  `amount`     DECIMAL(18,4) DEFAULT NULL,
  PRIMARY KEY (`ts_code`, `trade_time`, `period`),
  INDEX `idx_trade_time` (`trade_time`),
  INDEX `idx_period` (`period`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Strategy parameter presets
CREATE TABLE IF NOT EXISTS `quantmate`.`parameter_presets` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `strategy_id` INT NOT NULL,
  `user_id`     INT NOT NULL,
  `name`        VARCHAR(100) NOT NULL,
  `description` TEXT DEFAULT NULL,
  `params`      JSON NOT NULL,
  `is_default`  BOOLEAN DEFAULT FALSE,
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_strategy_user` (`strategy_id`, `user_id`),
  UNIQUE KEY `uq_strategy_user_name` (`strategy_id`, `user_id`, `name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Position sizing configurations
CREATE TABLE IF NOT EXISTS `quantmate`.`position_sizing_configs` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `user_id`     INT NOT NULL,
  `name`        VARCHAR(100) NOT NULL,
  `method`      VARCHAR(30) NOT NULL COMMENT 'fixed_amount/fixed_pct/kelly/equal_risk/risk_parity',
  `params`      JSON NOT NULL COMMENT 'Method-specific parameters',
  `max_position_pct` DECIMAL(5,2) DEFAULT 20.00 COMMENT 'Max single position %',
  `max_total_pct`    DECIMAL(5,2) DEFAULT 80.00 COMMENT 'Max total position %',
  `is_active`   BOOLEAN DEFAULT TRUE,
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Migration 033: Align Tushare extended schema with current init definitions
-- Date: 2026-04-16
-- Issue: N/A

CREATE DATABASE IF NOT EXISTS `tushare` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `tushare`.`stock_company` (
    `ts_code` VARCHAR(20) NOT NULL PRIMARY KEY,
    `com_name` VARCHAR(255) DEFAULT NULL,
    `com_id` VARCHAR(64) DEFAULT NULL,
    `exchange` VARCHAR(10) DEFAULT NULL,
    `chairman` VARCHAR(50) DEFAULT NULL,
    `manager` VARCHAR(50) DEFAULT NULL,
    `secretary` VARCHAR(50) DEFAULT NULL,
    `reg_capital` DECIMAL(18,4) DEFAULT NULL,
    `setup_date` DATE DEFAULT NULL,
    `province` VARCHAR(20) DEFAULT NULL,
    `city` VARCHAR(30) DEFAULT NULL,
    `introduction` TEXT,
    `website` VARCHAR(200) DEFAULT NULL,
    `email` VARCHAR(100) DEFAULT NULL,
    `office` VARCHAR(200) DEFAULT NULL,
    `employees` INT DEFAULT NULL,
    `main_business` TEXT,
    `business_scope` TEXT,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET @has_stock_company_com_name := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'stock_company' AND column_name = 'com_name'
);
SET @sql_stock_company_com_name := IF(
    @has_stock_company_com_name = 0,
    'ALTER TABLE `tushare`.`stock_company` ADD COLUMN `com_name` VARCHAR(255) DEFAULT NULL AFTER `ts_code`',
    'SELECT 1'
);
PREPARE stmt_stock_company_com_name FROM @sql_stock_company_com_name;
EXECUTE stmt_stock_company_com_name;
DEALLOCATE PREPARE stmt_stock_company_com_name;

SET @has_stock_company_com_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'stock_company' AND column_name = 'com_id'
);
SET @sql_stock_company_com_id := IF(
    @has_stock_company_com_id = 0,
    'ALTER TABLE `tushare`.`stock_company` ADD COLUMN `com_id` VARCHAR(64) DEFAULT NULL AFTER `com_name`',
    'SELECT 1'
);
PREPARE stmt_stock_company_com_id FROM @sql_stock_company_com_id;
EXECUTE stmt_stock_company_com_id;
DEALLOCATE PREPARE stmt_stock_company_com_id;

CREATE TABLE IF NOT EXISTS `tushare`.`new_share` (
    `ts_code` VARCHAR(32) NOT NULL,
    `sub_code` VARCHAR(32) DEFAULT NULL,
    `name` VARCHAR(255) DEFAULT NULL,
    `ipo_date` DATE DEFAULT NULL,
    `issue_date` DATE DEFAULT NULL,
    `market_amount` DECIMAL(18,4) DEFAULT NULL,
    `issue_price` DECIMAL(12,2) DEFAULT NULL,
    `pe` DECIMAL(12,2) DEFAULT NULL,
    `limit_amount` DECIMAL(18,4) DEFAULT NULL,
    `funds` DECIMAL(18,4) DEFAULT NULL,
    `ballot` DECIMAL(18,4) DEFAULT NULL,
    `amount` BIGINT DEFAULT NULL,
    `market` VARCHAR(32) DEFAULT NULL,
    PRIMARY KEY (`ts_code`, `ipo_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET @has_new_share_sub_code := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'new_share' AND column_name = 'sub_code'
);
SET @sql_new_share_sub_code := IF(
    @has_new_share_sub_code = 0,
    'ALTER TABLE `tushare`.`new_share` ADD COLUMN `sub_code` VARCHAR(32) DEFAULT NULL AFTER `ts_code`',
    'SELECT 1'
);
PREPARE stmt_new_share_sub_code FROM @sql_new_share_sub_code;
EXECUTE stmt_new_share_sub_code;
DEALLOCATE PREPARE stmt_new_share_sub_code;

SET @has_new_share_market_amount := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'new_share' AND column_name = 'market_amount'
);
SET @sql_new_share_market_amount := IF(
    @has_new_share_market_amount = 0,
    'ALTER TABLE `tushare`.`new_share` ADD COLUMN `market_amount` DECIMAL(18,4) DEFAULT NULL AFTER `issue_date`',
    'SELECT 1'
);
PREPARE stmt_new_share_market_amount FROM @sql_new_share_market_amount;
EXECUTE stmt_new_share_market_amount;
DEALLOCATE PREPARE stmt_new_share_market_amount;

SET @has_new_share_pe := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'new_share' AND column_name = 'pe'
);
SET @sql_new_share_pe := IF(
    @has_new_share_pe = 0,
    'ALTER TABLE `tushare`.`new_share` ADD COLUMN `pe` DECIMAL(12,2) DEFAULT NULL AFTER `issue_price`',
    'SELECT 1'
);
PREPARE stmt_new_share_pe FROM @sql_new_share_pe;
EXECUTE stmt_new_share_pe;
DEALLOCATE PREPARE stmt_new_share_pe;

SET @has_new_share_limit_amount := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'new_share' AND column_name = 'limit_amount'
);
SET @sql_new_share_limit_amount := IF(
    @has_new_share_limit_amount = 0,
    'ALTER TABLE `tushare`.`new_share` ADD COLUMN `limit_amount` DECIMAL(18,4) DEFAULT NULL AFTER `pe`',
    'SELECT 1'
);
PREPARE stmt_new_share_limit_amount FROM @sql_new_share_limit_amount;
EXECUTE stmt_new_share_limit_amount;
DEALLOCATE PREPARE stmt_new_share_limit_amount;

SET @has_new_share_funds := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'new_share' AND column_name = 'funds'
);
SET @sql_new_share_funds := IF(
    @has_new_share_funds = 0,
    'ALTER TABLE `tushare`.`new_share` ADD COLUMN `funds` DECIMAL(18,4) DEFAULT NULL AFTER `limit_amount`',
    'SELECT 1'
);
PREPARE stmt_new_share_funds FROM @sql_new_share_funds;
EXECUTE stmt_new_share_funds;
DEALLOCATE PREPARE stmt_new_share_funds;

SET @has_new_share_ballot := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'new_share' AND column_name = 'ballot'
);
SET @sql_new_share_ballot := IF(
    @has_new_share_ballot = 0,
    'ALTER TABLE `tushare`.`new_share` ADD COLUMN `ballot` DECIMAL(18,4) DEFAULT NULL AFTER `funds`',
    'SELECT 1'
);
PREPARE stmt_new_share_ballot FROM @sql_new_share_ballot;
EXECUTE stmt_new_share_ballot;
DEALLOCATE PREPARE stmt_new_share_ballot;

CREATE TABLE IF NOT EXISTS `tushare`.`bak_daily` (
    `ts_code` VARCHAR(32) NOT NULL,
    `trade_date` DATE NOT NULL,
    `name` VARCHAR(255) DEFAULT NULL,
    `pct_change` DECIMAL(16,4) DEFAULT NULL,
    `close` DECIMAL(16,4) DEFAULT NULL,
    `change_amount` DECIMAL(16,4) DEFAULT NULL,
    `open` DECIMAL(16,4) DEFAULT NULL,
    `high` DECIMAL(16,4) DEFAULT NULL,
    `low` DECIMAL(16,4) DEFAULT NULL,
    `pre_close` DECIMAL(16,4) DEFAULT NULL,
    `vol_ratio` DECIMAL(16,4) DEFAULT NULL,
    `turn_over` DECIMAL(16,4) DEFAULT NULL,
    `swing` DECIMAL(16,4) DEFAULT NULL,
    `vol` BIGINT DEFAULT NULL,
    `amount` DECIMAL(20,2) DEFAULT NULL,
    `selling` BIGINT DEFAULT NULL,
    `buying` BIGINT DEFAULT NULL,
    `total_share` DECIMAL(20,2) DEFAULT NULL,
    `float_share` DECIMAL(20,2) DEFAULT NULL,
    `pe` DECIMAL(16,4) DEFAULT NULL,
    `industry` VARCHAR(128) DEFAULT NULL,
    `area` VARCHAR(64) DEFAULT NULL,
    `float_mv` DECIMAL(20,2) DEFAULT NULL,
    `total_mv` DECIMAL(20,2) DEFAULT NULL,
    `avg_price` DECIMAL(16,4) DEFAULT NULL,
    `strength` DECIMAL(16,4) DEFAULT NULL,
    `activity` BIGINT DEFAULT NULL,
    `avg_turnover` DECIMAL(16,4) DEFAULT NULL,
    `attack` DECIMAL(16,4) DEFAULT NULL,
    `interval_3` DECIMAL(16,4) DEFAULT NULL,
    `interval_6` DECIMAL(16,4) DEFAULT NULL,
    PRIMARY KEY (`ts_code`, `trade_date`),
    INDEX `idx_bak_daily_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tushare`.`suspend_d` (
    `ts_code` VARCHAR(32) NOT NULL,
    `trade_date` DATE NOT NULL,
    `suspend_timing` VARCHAR(64) DEFAULT NULL,
    `suspend_type` VARCHAR(32) DEFAULT NULL,
    PRIMARY KEY (`ts_code`, `trade_date`),
    INDEX `idx_suspend_d_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tushare`.`suspend` (
    `ts_code` VARCHAR(32) NOT NULL,
    `suspend_date` DATE NOT NULL,
    `resume_date` DATE DEFAULT NULL,
    `suspend_reason` VARCHAR(255) DEFAULT NULL,
    PRIMARY KEY (`ts_code`, `suspend_date`),
    INDEX `idx_suspend_resume_date` (`resume_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tushare`.`fina_indicator` (
    `ts_code` VARCHAR(20) NOT NULL,
    `ann_date` DATE DEFAULT NULL,
    `end_date` DATE NOT NULL,
    `eps` DECIMAL(18,6) DEFAULT NULL,
    `dt_eps` DECIMAL(18,6) DEFAULT NULL,
    `total_revenue_ps` DECIMAL(18,6) DEFAULT NULL,
    `revenue_ps` DECIMAL(18,6) DEFAULT NULL,
    `capital_rese_ps` DECIMAL(18,6) DEFAULT NULL,
    `surplus_rese_ps` DECIMAL(18,6) DEFAULT NULL,
    `undist_profit_ps` DECIMAL(18,6) DEFAULT NULL,
    `extra_item` DECIMAL(18,4) DEFAULT NULL,
    `profit_dedt` DECIMAL(18,4) DEFAULT NULL,
    `gross_margin` DECIMAL(20,6) DEFAULT NULL,
    `current_ratio` DECIMAL(20,6) DEFAULT NULL,
    `quick_ratio` DECIMAL(20,6) DEFAULT NULL,
    `cash_ratio` DECIMAL(20,6) DEFAULT NULL,
    `ar_turn` DECIMAL(20,6) DEFAULT NULL,
    `ca_turn` DECIMAL(20,6) DEFAULT NULL,
    `fa_turn` DECIMAL(20,6) DEFAULT NULL,
    `assets_turn` DECIMAL(20,6) DEFAULT NULL,
    `op_income` DECIMAL(18,4) DEFAULT NULL,
    `ebit` DECIMAL(18,4) DEFAULT NULL,
    `ebitda` DECIMAL(18,4) DEFAULT NULL,
    `fcff` DECIMAL(18,4) DEFAULT NULL,
    `fcfe` DECIMAL(18,4) DEFAULT NULL,
    `roe` DECIMAL(20,6) DEFAULT NULL,
    `roe_waa` DECIMAL(20,6) DEFAULT NULL,
    `roe_dt` DECIMAL(20,6) DEFAULT NULL,
    `roa` DECIMAL(20,6) DEFAULT NULL,
    `npta` DECIMAL(20,6) DEFAULT NULL,
    `debt_to_assets` DECIMAL(20,6) DEFAULT NULL,
    `netprofit_yoy` DECIMAL(20,6) DEFAULT NULL,
    `or_yoy` DECIMAL(20,6) DEFAULT NULL,
    `roe_yoy` DECIMAL(20,6) DEFAULT NULL,
    PRIMARY KEY (`ts_code`, `end_date`),
    INDEX `idx_end_date` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE `tushare`.`fina_indicator`
    MODIFY COLUMN `gross_margin` DECIMAL(20,6) DEFAULT NULL COMMENT '毛利率',
    MODIFY COLUMN `current_ratio` DECIMAL(20,6) DEFAULT NULL COMMENT '流动比率',
    MODIFY COLUMN `quick_ratio` DECIMAL(20,6) DEFAULT NULL COMMENT '速动比率',
    MODIFY COLUMN `cash_ratio` DECIMAL(20,6) DEFAULT NULL COMMENT '现金比率',
    MODIFY COLUMN `ar_turn` DECIMAL(20,6) DEFAULT NULL COMMENT '应收账款周转率',
    MODIFY COLUMN `ca_turn` DECIMAL(20,6) DEFAULT NULL COMMENT '流动资产周转率',
    MODIFY COLUMN `fa_turn` DECIMAL(20,6) DEFAULT NULL COMMENT '固定资产周转率',
    MODIFY COLUMN `assets_turn` DECIMAL(20,6) DEFAULT NULL COMMENT '总资产周转率',
    MODIFY COLUMN `roe` DECIMAL(20,6) DEFAULT NULL COMMENT '净资产收益率',
    MODIFY COLUMN `roe_waa` DECIMAL(20,6) DEFAULT NULL COMMENT '加权ROE',
    MODIFY COLUMN `roe_dt` DECIMAL(20,6) DEFAULT NULL COMMENT '扣非ROE',
    MODIFY COLUMN `roa` DECIMAL(20,6) DEFAULT NULL COMMENT '总资产报酬率',
    MODIFY COLUMN `npta` DECIMAL(20,6) DEFAULT NULL COMMENT '总资产净利率',
    MODIFY COLUMN `debt_to_assets` DECIMAL(20,6) DEFAULT NULL COMMENT '资产负债率',
    MODIFY COLUMN `netprofit_yoy` DECIMAL(20,6) DEFAULT NULL COMMENT '净利润同比增长',
    MODIFY COLUMN `or_yoy` DECIMAL(20,6) DEFAULT NULL COMMENT '营业收入同比增长',
    MODIFY COLUMN `roe_yoy` DECIMAL(20,6) DEFAULT NULL COMMENT 'ROE同比增长';

CREATE TABLE IF NOT EXISTS `tushare`.`balancesheet` (
    `ts_code` VARCHAR(20) NOT NULL,
    `ann_date` DATE DEFAULT NULL,
    `f_ann_date` DATE DEFAULT NULL,
    `end_date` DATE NOT NULL,
    `report_type` VARCHAR(5) DEFAULT NULL,
    `comp_type` VARCHAR(5) DEFAULT NULL,
    `data` JSON NOT NULL,
    PRIMARY KEY (`ts_code`, `end_date`),
    INDEX `idx_balancesheet_ann_date` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `tushare`.`cashflow` (
    `ts_code` VARCHAR(20) NOT NULL,
    `ann_date` DATE DEFAULT NULL,
    `f_ann_date` DATE DEFAULT NULL,
    `end_date` DATE NOT NULL,
    `report_type` VARCHAR(5) DEFAULT NULL,
    `comp_type` VARCHAR(5) DEFAULT NULL,
    `data` JSON NOT NULL,
    PRIMARY KEY (`ts_code`, `end_date`),
    INDEX `idx_cashflow_ann_date` (`ann_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
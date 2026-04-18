-- Migration 033: Align Tushare bootstrap schema with current init definitions
-- Date: 2026-04-16
-- Issue: N/A

-- Only bootstrap interfaces that are available at 120 points and below should
-- be created up front for new environments. Higher-permission Tushare tables
-- are created lazily when their interfaces are actually used.

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
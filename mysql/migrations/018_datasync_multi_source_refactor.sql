-- =============================================================================
-- Migration 018: DataSync Multi-Source Refactor
-- Converts hardcoded ENUM-based sync to dynamic multi-source plugin architecture
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Alter data_source_items: add target_database, target_table, table_created, sync_priority
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_source_items (
    id                  INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source              VARCHAR(20)  NOT NULL COMMENT 'tushare or akshare',
    item_key            VARCHAR(100) NOT NULL,
    item_name           VARCHAR(200) NOT NULL,
    enabled             TINYINT(1)   NOT NULL DEFAULT 1,
    description         TEXT         DEFAULT NULL,
    requires_permission VARCHAR(50)  DEFAULT NULL COMMENT 'Permission level required',
    updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_source_item (source, item_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE data_source_items
    ADD COLUMN IF NOT EXISTS target_database VARCHAR(50) NOT NULL DEFAULT '' AFTER requires_permission,
    ADD COLUMN IF NOT EXISTS target_table VARCHAR(100) NOT NULL DEFAULT '' AFTER target_database,
    ADD COLUMN IF NOT EXISTS table_created TINYINT(1) NOT NULL DEFAULT 0 AFTER target_table,
    ADD COLUMN IF NOT EXISTS sync_priority INT NOT NULL DEFAULT 100 AFTER table_created;

-- Backfill target_database and target_table for existing rows
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stock_basic',     sync_priority = 10  WHERE source = 'tushare' AND item_key = 'stock_basic';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stock_daily',     sync_priority = 20  WHERE source = 'tushare' AND item_key = 'stock_daily';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'adj_factor',      sync_priority = 30  WHERE source = 'tushare' AND item_key = 'adj_factor';
UPDATE data_source_items SET target_database = 'akshare', target_table = 'trade_cal',       sync_priority = 5   WHERE source = 'tushare' AND item_key = 'trade_cal';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stock_dividend',  sync_priority = 50  WHERE source = 'tushare' AND item_key = 'stock_dividend';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stock_weekly',    sync_priority = 25  WHERE source = 'tushare' AND item_key = 'stock_weekly';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stock_monthly',   sync_priority = 26  WHERE source = 'tushare' AND item_key = 'stock_monthly';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'index_weekly',    sync_priority = 28  WHERE source = 'tushare' AND item_key = 'index_weekly';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'index_daily',     sync_priority = 27  WHERE source = 'tushare' AND item_key = 'index_daily';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'moneyflow',       sync_priority = 60  WHERE source = 'tushare' AND item_key = 'money_flow';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stk_limit',       sync_priority = 70  WHERE source = 'tushare' AND item_key = 'stk_limit';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'margin',          sync_priority = 80  WHERE source = 'tushare' AND item_key = 'margin_detail';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'block_trade',     sync_priority = 90  WHERE source = 'tushare' AND item_key = 'block_trade';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stock_company',   sync_priority = 15  WHERE source = 'tushare' AND item_key = 'stock_company';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'fina_indicator',  sync_priority = 55  WHERE source = 'tushare' AND item_key = 'fina_indicator';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'stock_dividend',  sync_priority = 50  WHERE source = 'tushare' AND item_key = 'dividend';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'income',          sync_priority = 56  WHERE source = 'tushare' AND item_key = 'income';
UPDATE data_source_items SET target_database = 'tushare', target_table = 'top10_holders',   sync_priority = 57  WHERE source = 'tushare' AND item_key = 'top10_holders';

UPDATE data_source_items SET target_database = 'akshare', target_table = 'stock_zh_index_spot', sync_priority = 40  WHERE source = 'akshare' AND item_key = 'stock_zh_index';
UPDATE data_source_items SET target_database = 'akshare', target_table = 'stock_zh_index_spot', sync_priority = 40  WHERE source = 'akshare' AND item_key = 'stock_zh_index_spot';
UPDATE data_source_items SET target_database = 'akshare', target_table = 'fund_etf_daily',      sync_priority = 45  WHERE source = 'akshare' AND item_key = 'fund_etf_daily';

-- Mark all existing tables as created (they exist from init SQL)
UPDATE data_source_items SET table_created = 1 WHERE target_table != '';

-- -----------------------------------------------------------------------------
-- 2. Recreate data_sync_status with VARCHAR instead of ENUM
-- -----------------------------------------------------------------------------
-- Rename old table only when it exists.
SET @has_data_sync_status := (
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name = 'data_sync_status'
);
SET @rename_data_sync_status_sql := IF(
    @has_data_sync_status > 0,
    'RENAME TABLE data_sync_status TO data_sync_status_old',
    'SELECT 1'
);
PREPARE stmt_rename_data_sync_status FROM @rename_data_sync_status_sql;
EXECUTE stmt_rename_data_sync_status;
DEALLOCATE PREPARE stmt_rename_data_sync_status;

-- In case there was no legacy table, create an empty source table so the INSERT ... SELECT remains valid.
CREATE TABLE IF NOT EXISTS data_sync_status_old (
    sync_date DATE NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    rows_synced INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Create new table with dynamic columns
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

-- Migrate existing data: map step_name to source + interface_key
INSERT INTO data_sync_status (sync_date, source, interface_key, status, rows_synced, error_message, started_at, finished_at, created_at, updated_at)
SELECT
    sync_date,
    CASE
        WHEN step_name LIKE 'akshare_%' THEN 'akshare'
        WHEN step_name LIKE 'tushare_%' THEN 'tushare'
        WHEN step_name = 'vnpy_sync' THEN 'vnpy'
        ELSE 'unknown'
    END AS source,
    CASE
        WHEN step_name = 'akshare_index' THEN 'index_daily'
        WHEN step_name = 'tushare_stock_basic' THEN 'stock_basic'
        WHEN step_name = 'tushare_stock_daily' THEN 'stock_daily'
        WHEN step_name = 'tushare_adj_factor' THEN 'adj_factor'
        WHEN step_name = 'tushare_dividend' THEN 'dividend'
        WHEN step_name = 'tushare_top10_holders' THEN 'top10_holders'
        WHEN step_name = 'vnpy_sync' THEN 'vnpy_sync'
        WHEN step_name = 'tushare_stock_weekly' THEN 'stock_weekly'
        WHEN step_name = 'tushare_stock_monthly' THEN 'stock_monthly'
        WHEN step_name = 'tushare_index_daily' THEN 'index_daily'
        WHEN step_name = 'tushare_index_weekly' THEN 'index_weekly'
        ELSE step_name
    END AS interface_key,
    status,
    rows_synced,
    error_message,
    started_at,
    finished_at,
    created_at,
    updated_at
FROM data_sync_status_old;

-- Drop old table after migration
DROP TABLE data_sync_status_old;

-- -----------------------------------------------------------------------------
-- 3. Enhance data_source_configs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_source_configs (
    id                  INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source_type         VARCHAR(50)  NOT NULL,
    api_token_encrypted TEXT         DEFAULT NULL,
    rate_limit          INT          NOT NULL DEFAULT 60,
    is_enabled          TINYINT(1)   NOT NULL DEFAULT 1,
    updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ds_type (source_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Add missing columns if they don't exist
ALTER TABLE data_source_configs
    ADD COLUMN IF NOT EXISTS display_name VARCHAR(100) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS config_json JSON DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS requires_token TINYINT(1) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS source_type VARCHAR(50) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS is_enabled TINYINT(1) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS source_key VARCHAR(50) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS enabled TINYINT(1) NOT NULL DEFAULT 1;

-- Backfill new canonical columns from legacy columns.
UPDATE data_source_configs SET source_key = source_type WHERE source_key IS NULL OR source_key = '';
UPDATE data_source_configs SET enabled = is_enabled WHERE enabled IS NULL;
UPDATE data_source_configs SET source_type = source_key WHERE source_type IS NULL OR source_type = '';
UPDATE data_source_configs SET is_enabled = enabled WHERE is_enabled IS NULL;

-- Ensure source_key is non-null after backfill.
UPDATE data_source_configs SET source_key = 'unknown' WHERE source_key IS NULL OR source_key = '';

-- Seed/update config data
INSERT INTO data_source_configs (source_key, source_type, display_name, enabled, is_enabled, rate_limit, requires_token) VALUES
('tushare', 'tushare', 'Tushare Pro', 1, 1, 50, 1),
('akshare', 'akshare', 'AkShare', 1, 1, 30, 0)
ON DUPLICATE KEY UPDATE
    display_name = VALUES(display_name),
    enabled = VALUES(enabled),
    is_enabled = VALUES(is_enabled),
    requires_token = VALUES(requires_token);

-- -----------------------------------------------------------------------------
-- 4. Record migration
-- -----------------------------------------------------------------------------
INSERT IGNORE INTO schema_migrations (version, name) VALUES
    ('018', '018_datasync_multi_source_refactor.sql');

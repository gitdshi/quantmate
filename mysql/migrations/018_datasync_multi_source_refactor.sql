-- =============================================================================
-- Migration 018: DataSync Multi-Source Refactor
-- Converts hardcoded ENUM-based sync to dynamic multi-source plugin architecture
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Alter data_source_items: add target_database, target_table, table_created, sync_priority
-- -----------------------------------------------------------------------------
ALTER TABLE data_source_items
    ADD COLUMN target_database VARCHAR(50) NOT NULL DEFAULT '' AFTER requires_permission,
    ADD COLUMN target_table VARCHAR(100) NOT NULL DEFAULT '' AFTER target_database,
    ADD COLUMN table_created TINYINT(1) NOT NULL DEFAULT 0 AFTER target_table,
    ADD COLUMN sync_priority INT NOT NULL DEFAULT 100 AFTER table_created;

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
-- Rename old table
RENAME TABLE data_sync_status TO data_sync_status_old;

-- Create new table with dynamic columns
CREATE TABLE data_sync_status (
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
-- Add missing columns if they don't exist
ALTER TABLE data_source_configs
    ADD COLUMN display_name VARCHAR(100) NOT NULL DEFAULT '' AFTER source_type,
    ADD COLUMN config_json JSON DEFAULT NULL AFTER api_token_encrypted,
    ADD COLUMN requires_token TINYINT(1) DEFAULT 0 AFTER is_enabled;

-- Rename source_type to source_key for consistency
ALTER TABLE data_source_configs CHANGE COLUMN source_type source_key VARCHAR(50) NOT NULL;
ALTER TABLE data_source_configs CHANGE COLUMN is_enabled enabled TINYINT(1) NOT NULL DEFAULT 1;

-- Update unique key
ALTER TABLE data_source_configs DROP INDEX uq_ds_type;
ALTER TABLE data_source_configs ADD UNIQUE KEY uq_source_key (source_key);

-- Seed/update config data
INSERT INTO data_source_configs (source_key, display_name, enabled, rate_limit, requires_token) VALUES
('tushare', 'Tushare Pro', 1, 50, 1),
('akshare', 'AkShare', 1, 30, 0)
ON DUPLICATE KEY UPDATE
    display_name = VALUES(display_name),
    requires_token = VALUES(requires_token);

-- -----------------------------------------------------------------------------
-- 4. Record migration
-- -----------------------------------------------------------------------------
INSERT IGNORE INTO schema_migrations (version, name) VALUES
    ('018', '018_datasync_multi_source_refactor.sql');

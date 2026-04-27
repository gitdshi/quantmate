SET @has_col_sync_mode := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'data_source_items' AND column_name = 'sync_mode'
);
SET @sql_add_sync_mode := IF(
    @has_col_sync_mode = 0,
    'ALTER TABLE data_source_items ADD COLUMN sync_mode VARCHAR(20) NOT NULL DEFAULT ''backfill'' COMMENT ''backfill or latest_only'' AFTER sync_priority',
    'SELECT 1'
);
PREPARE stmt_add_sync_mode FROM @sql_add_sync_mode;
EXECUTE stmt_add_sync_mode;
DEALLOCATE PREPARE stmt_add_sync_mode;

UPDATE data_source_items
SET sync_mode = 'backfill'
WHERE sync_mode IS NULL OR TRIM(sync_mode) = '';

UPDATE data_source_items
SET sync_mode = 'latest_only'
WHERE (source = 'tushare' AND item_key IN (
    'opt_mins',
    'pledge_detail',
    'stock_basic',
    'stock_company',
    'us_basic'
))
   OR (source = 'akshare' AND item_key IN (
    'stock_zh_index_spot'
));
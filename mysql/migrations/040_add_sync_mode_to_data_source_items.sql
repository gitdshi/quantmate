ALTER TABLE data_source_items
    ADD COLUMN IF NOT EXISTS sync_mode VARCHAR(20) NOT NULL DEFAULT 'backfill'
    COMMENT 'backfill or latest_only'
    AFTER sync_priority;

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
-- Migration 031: Tushare catalog guardrails
-- Restores supported legacy items removed by migration 030 and prevents
-- unsupported catalog rows from being enabled by default.

-- Correct catalog mappings for legacy Tushare items.
UPDATE `quantmate`.`data_source_items`
SET target_database = 'tushare', target_table = 'trade_cal'
WHERE source = 'tushare' AND item_key = 'trade_cal';

UPDATE `quantmate`.`data_source_items`
SET target_database = 'tushare', target_table = 'money_flow'
WHERE source = 'tushare' AND item_key = 'money_flow';

UPDATE `quantmate`.`data_source_items`
SET target_database = 'tushare', target_table = 'margin_detail'
WHERE source = 'tushare' AND item_key = 'margin_detail';

-- Restore the supported legacy top10_holders item if migration 030 removed it.
INSERT INTO `quantmate`.`data_source_items`
  (
    source,
    item_key,
    item_name,
    enabled,
    description,
    category,
    sub_category,
    api_name,
    permission_points,
    rate_limit_note,
    requires_permission,
    target_database,
    target_table,
    table_created,
    sync_priority
  )
VALUES
  (
    'tushare',
    'top10_holders',
    '十大股东',
    0,
    '十大股东数据(需高级权限)',
    '股票数据',
    '特色数据',
    'top10_holders',
    NULL,
    '单次最大6000行',
    '1',
    'tushare',
    'top10_holders',
    0,
    57
  )
ON DUPLICATE KEY UPDATE
  item_name = VALUES(item_name),
  description = VALUES(description),
  category = VALUES(category),
  sub_category = VALUES(sub_category),
  api_name = VALUES(api_name),
  rate_limit_note = VALUES(rate_limit_note),
  requires_permission = VALUES(requires_permission),
  target_database = VALUES(target_database),
  target_table = VALUES(target_table),
  sync_priority = VALUES(sync_priority);

-- Disable catalog items that are visible in settings but do not yet have a
-- matching BaseIngestInterface implementation in the sync registry.
UPDATE `quantmate`.`data_source_items`
SET enabled = 0
WHERE source = 'tushare'
  AND item_key IN (
    'stock_company',
    'trade_cal',
    'money_flow',
    'margin_detail',
    'stk_limit',
    'fina_indicator',
    'block_trade'
  );
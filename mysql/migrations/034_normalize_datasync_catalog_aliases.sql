-- Migration 034: Normalize data_source_items aliases to canonical registry keys
-- Date: 2026-04-16

-- Keep supported registry keys canonical so dynamic enable/disable remains
-- aligned with the plugin registry.

-- Tushare moneyflow: replace legacy money_flow alias.
INSERT INTO `quantmate`.`data_source_items`
  (
    source,
    item_key,
    item_name,
    enabled,
    description,
    requires_permission,
    target_database,
    target_table,
    table_created,
    sync_priority
  )
SELECT
  'tushare',
  'moneyflow',
  '资金流向',
  1,
  '个股资金流向数据',
  '0',
  'tushare',
  'stock_moneyflow',
  COALESCE(MAX(table_created), 1),
  25
FROM `quantmate`.`data_source_items`
WHERE source = 'tushare' AND item_key = 'money_flow'
HAVING COUNT(*) > 0
ON DUPLICATE KEY UPDATE
  item_name = VALUES(item_name),
  description = VALUES(description),
  requires_permission = VALUES(requires_permission),
  target_database = VALUES(target_database),
  target_table = VALUES(target_table),
  sync_priority = VALUES(sync_priority);

UPDATE `quantmate`.`data_source_items`
SET item_name = '资金流向',
    description = '个股资金流向数据',
    requires_permission = '0',
    target_database = 'tushare',
    target_table = 'stock_moneyflow',
    sync_priority = 25
WHERE source = 'tushare' AND item_key = 'moneyflow';

DELETE FROM `quantmate`.`data_source_items`
WHERE source = 'tushare' AND item_key = 'money_flow';

-- Tushare dividend: replace legacy stock_dividend alias.
INSERT INTO `quantmate`.`data_source_items`
  (
    source,
    item_key,
    item_name,
    enabled,
    description,
    requires_permission,
    target_database,
    target_table,
    table_created,
    sync_priority
  )
SELECT
  'tushare',
  'dividend',
  '分红送股',
  0,
  '分红送股数据(需高级权限)',
  '1',
  'tushare',
  'stock_dividend',
  COALESCE(MAX(table_created), 1),
  50
FROM `quantmate`.`data_source_items`
WHERE source = 'tushare' AND item_key = 'stock_dividend'
HAVING COUNT(*) > 0
ON DUPLICATE KEY UPDATE
  item_name = VALUES(item_name),
  description = VALUES(description),
  requires_permission = VALUES(requires_permission),
  target_database = VALUES(target_database),
  target_table = VALUES(target_table),
  sync_priority = VALUES(sync_priority);

UPDATE `quantmate`.`data_source_items`
SET item_name = '分红送股',
    description = '分红送股数据(需高级权限)',
    requires_permission = '1',
    target_database = 'tushare',
    target_table = 'stock_dividend',
    sync_priority = 50
WHERE source = 'tushare' AND item_key = 'dividend';

DELETE FROM `quantmate`.`data_source_items`
WHERE source = 'tushare' AND item_key = 'stock_dividend';

-- AkShare index spot: replace legacy stock_zh_index alias.
INSERT INTO `quantmate`.`data_source_items`
  (
    source,
    item_key,
    item_name,
    enabled,
    description,
    requires_permission,
    target_database,
    target_table,
    table_created,
    sync_priority
  )
SELECT
  'akshare',
  'stock_zh_index_spot',
  '指数实时行情',
  1,
  'A股指数实时报价',
  NULL,
  'akshare',
  'stock_zh_index_spot',
  COALESCE(MAX(table_created), 1),
  40
FROM `quantmate`.`data_source_items`
WHERE source = 'akshare' AND item_key = 'stock_zh_index'
HAVING COUNT(*) > 0
ON DUPLICATE KEY UPDATE
  item_name = VALUES(item_name),
  description = VALUES(description),
  target_database = VALUES(target_database),
  target_table = VALUES(target_table),
  sync_priority = VALUES(sync_priority);

UPDATE `quantmate`.`data_source_items`
SET item_name = '指数实时行情',
    description = 'A股指数实时报价',
    target_database = 'akshare',
    target_table = 'stock_zh_index_spot',
    sync_priority = 40
WHERE source = 'akshare' AND item_key = 'stock_zh_index_spot';

DELETE FROM `quantmate`.`data_source_items`
WHERE source = 'akshare' AND item_key = 'stock_zh_index';
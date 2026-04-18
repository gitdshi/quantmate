-- Migration 036: Normalize Tushare permission metadata and explicit grants
-- Date: 2026-04-17

-- Canonical rule:
-- - requires_permission = '1' only for separately paid interfaces
-- - all other Tushare interfaces use requires_permission = '0'
-- - missing permission_points for legacy aliases are backfilled from the
--   official catalog or closest maintained equivalent

UPDATE `quantmate`.`data_source_items`
SET requires_permission = CASE
    WHEN source = 'tushare' AND COALESCE(permission_points, '') LIKE '%单独权限%' THEN '1'
    WHEN source = 'tushare' THEN '0'
    ELSE requires_permission
END
WHERE source = 'tushare';

UPDATE `quantmate`.`data_source_items`
SET api_name = 'suspend_d',
    permission_points = '120积分',
    category = '股票数据',
    sub_category = '行情数据',
    rate_limit_note = '单次最大6000行',
    requires_permission = '0'
WHERE source = 'tushare' AND item_key = 'suspend_d';

-- `suspend` is a legacy compatibility interface kept alongside `suspend_d`.
UPDATE `quantmate`.`data_source_items`
SET api_name = 'suspend',
    permission_points = '120积分',
    category = '股票数据',
    sub_category = '行情数据',
    rate_limit_note = '单次最大6000行',
    requires_permission = '0'
WHERE source = 'tushare' AND item_key = 'suspend';

UPDATE `quantmate`.`data_source_items`
SET api_name = 'moneyflow',
    permission_points = '2000积分',
    category = '股票数据',
    sub_category = '行情数据',
    rate_limit_note = '单次最大6000行',
    requires_permission = '0'
WHERE source = 'tushare' AND item_key = 'moneyflow';

UPDATE `quantmate`.`data_source_items`
SET api_name = 'top10_holders',
    permission_points = '5000积分',
    category = '股票数据',
    sub_category = '特色数据',
    rate_limit_note = '单次最大6000行',
    requires_permission = '0'
WHERE source = 'tushare' AND item_key = 'top10_holders';
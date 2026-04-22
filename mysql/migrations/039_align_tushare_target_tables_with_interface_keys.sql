-- Migration 039: align Tushare target_table names with interface keys
-- Date: 2026-04-22

UPDATE `quantmate`.`data_source_items`
SET target_database = 'tushare',
    target_table = 'moneyflow'
WHERE source = 'tushare' AND item_key = 'moneyflow';

UPDATE `quantmate`.`data_source_items`
SET target_database = 'tushare',
    target_table = 'dividend'
WHERE source = 'tushare' AND item_key = 'dividend';

UPDATE `quantmate`.`data_source_items`
SET target_database = 'tushare',
    target_table = 'margin_detail'
WHERE source = 'tushare' AND item_key = 'margin_detail';

UPDATE `quantmate`.`data_source_items`
SET target_database = 'tushare',
    target_table = 'margin'
WHERE source = 'tushare' AND item_key = 'margin';

SET @has_stock_moneyflow := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'stock_moneyflow'
);
SET @has_money_flow := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'money_flow'
);
SET @has_moneyflow := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'moneyflow'
);

SET @rename_moneyflow_sql := IF(
  @has_stock_moneyflow > 0 AND @has_moneyflow = 0,
  'RENAME TABLE `tushare`.`stock_moneyflow` TO `tushare`.`moneyflow`',
  IF(
    @has_money_flow > 0 AND @has_moneyflow = 0,
    'RENAME TABLE `tushare`.`money_flow` TO `tushare`.`moneyflow`',
    'SELECT 1'
  )
);
PREPARE stmt FROM @rename_moneyflow_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @has_stock_dividend := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'stock_dividend'
);
SET @has_dividend := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'dividend'
);

SET @rename_dividend_sql := IF(
  @has_stock_dividend > 0 AND @has_dividend = 0,
  'RENAME TABLE `tushare`.`stock_dividend` TO `tushare`.`dividend`',
  'SELECT 1'
);
PREPARE stmt FROM @rename_dividend_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @has_margin := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'margin'
);
SET @has_margin_detail := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'margin_detail'
);
SET @has_margin_summary := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = 'margin_summary'
);
SET @has_tmp_margin_detail := (
  SELECT COUNT(*)
  FROM information_schema.tables
  WHERE table_schema = 'tushare' AND table_name = '__tmp_align_margin_detail'
);

SET @rename_margin_sql := IF(
  @has_margin > 0 AND @has_margin_summary > 0 AND @has_margin_detail = 0 AND @has_tmp_margin_detail = 0,
  'RENAME TABLE `tushare`.`margin` TO `tushare`.`__tmp_align_margin_detail`, `tushare`.`margin_summary` TO `tushare`.`margin`, `tushare`.`__tmp_align_margin_detail` TO `tushare`.`margin_detail`',
  'SELECT 1'
);
PREPARE stmt FROM @rename_margin_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

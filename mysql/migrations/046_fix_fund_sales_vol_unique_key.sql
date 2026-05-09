-- Replace the auto-inferred single-column UNIQUE KEYs on fund_sales_vol with
-- the stable business key (year, quarter, inst_name).

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`fund_sales_vol` DROP INDEX `ux_fund_sales_vol_year`',
    'SELECT ''fund_sales_vol.ux_fund_sales_vol_year already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'fund_sales_vol' AND index_name = 'ux_fund_sales_vol_year'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`fund_sales_vol` DROP INDEX `ux_fund_sales_vol_quarter`',
    'SELECT ''fund_sales_vol.ux_fund_sales_vol_quarter already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'fund_sales_vol' AND index_name = 'ux_fund_sales_vol_quarter'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`fund_sales_vol` DROP INDEX `ux_fund_sales_vol_inst_name`',
    'SELECT ''fund_sales_vol.ux_fund_sales_vol_inst_name already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'fund_sales_vol' AND index_name = 'ux_fund_sales_vol_inst_name'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) = 0,
    'ALTER TABLE `tushare`.`fund_sales_vol` ADD UNIQUE KEY `ux_fund_sales_vol_year_quarter_inst_name` (`year`, `quarter`, `inst_name`)',
    'SELECT ''fund_sales_vol composite unique key already present'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare'
    AND table_name = 'fund_sales_vol'
    AND index_name = 'ux_fund_sales_vol_year_quarter_inst_name'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
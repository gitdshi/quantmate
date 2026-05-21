-- Drop stale timestamp-based UNIQUE KEYs on cn_pmi.
-- Older auto-inference runs could promote create_time/update_time to unique
-- keys, which later blocks re-ingest and schema reconciliation when duplicate
-- timestamp values already exist.

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_create_time`',
    'SELECT ''cn_pmi.ux_cn_pmi_create_time already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_create_time'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_update_time`',
    'SELECT ''cn_pmi.ux_cn_pmi_update_time already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_update_time'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
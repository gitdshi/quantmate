-- Drop overly-restrictive UNIQUE KEYs that were auto-inferred on numeric
-- value columns by infer_dynamic_table_schema.  These tables were created
-- with individual data columns promoted to key columns, producing spurious
-- duplicate-key errors on legitimate re-ingest (e.g. cb_rate 100001.SH,
-- cn_pmi PMI value 52.1 appearing in two different months).
-- Each DROP is guarded so the migration is idempotent on fresh installs.

-- cb_rate: unique key on ts_code alone is too narrow (a bond can have
-- multiple rate records for different announcement dates).
SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cb_rate` DROP INDEX `ux_cb_rate_ts_code`',
    'SELECT ''cb_rate.ux_cb_rate_ts_code already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cb_rate' AND index_name = 'ux_cb_rate_ts_code'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- cn_pmi: auto-inference created individual unique keys on most numeric
-- columns. Each key enforces uniqueness on a single value column, which
-- is incorrect for PMI data where values can repeat across months.
SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010801`',
    'SELECT ''cn_pmi index already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010801'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010600`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010600'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_id`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_id'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010402`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010402'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010403`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010403'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_month`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_month'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020601`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020601'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010501`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010501'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010503`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010503'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010401`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010401'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020401`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020401'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_update_by`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_update_by'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020301`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020301'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010502`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010502'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010703`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010703'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010702`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010702'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011600`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011600'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020202`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020202'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011700`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011700'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020501`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020501'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011800`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011800'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010603`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010603'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010802`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010802'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_create_by`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_create_by'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi030000`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi030000'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011900`',
    'SELECT 1') FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011900'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

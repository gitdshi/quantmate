-- Widen auto-inferred VARCHAR columns that were too narrow for real-world
-- Chinese business names (pledgor, holder_name, change_reason).
-- The _varchar_bucket function previously started at 16; the code fix in
-- commit bab7ce8 raises the minimum to 64. This migration aligns existing
-- tushare tables that were created by the old inference logic.
-- Each ALTER is guarded so the migration is idempotent across fresh installs
-- (where tables may not exist yet) and existing environments.

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`pledge_detail` MODIFY COLUMN `holder_name` VARCHAR(128) NOT NULL',
    'SELECT ''pledge_detail.holder_name already widened or missing'' AS info')
  FROM information_schema.columns
  WHERE table_schema = 'tushare' AND table_name = 'pledge_detail'
    AND column_name = 'holder_name' AND column_type = 'varchar(32)'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`pledge_detail` MODIFY COLUMN `pledgor` VARCHAR(128) NULL',
    'SELECT ''pledge_detail.pledgor already widened or missing'' AS info')
  FROM information_schema.columns
  WHERE table_schema = 'tushare' AND table_name = 'pledge_detail'
    AND column_name = 'pledgor' AND column_type = 'varchar(16)'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`forecast` MODIFY COLUMN `change_reason` TEXT NULL',
    'SELECT ''forecast.change_reason already TEXT or missing'' AS info')
  FROM information_schema.columns
  WHERE table_schema = 'tushare' AND table_name = 'forecast'
    AND column_name = 'change_reason' AND column_type NOT IN ('text','mediumtext','longtext')
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

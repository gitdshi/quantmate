-- 059_factor_evaluation_data_status.sql
-- Factor evaluation data completeness tracking.
--
-- Adds data_status / data_note to factor_evaluations so clients can tell
-- apart evaluations computed on real (fully synced) data from fallback stub
-- metrics, partial (some unsynced dates skipped) data, or failed runs.

SET @dbname = 'quantmate';
SET @tablename = 'factor_evaluations';

SET @colname = 'data_status';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` VARCHAR(16) DEFAULT NULL COMMENT ''real/partial/fallback/failed'' AFTER `long_short_ret`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

SET @colname = 'data_note';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` VARCHAR(255) DEFAULT NULL COMMENT ''Human-readable note on data coverage/sync'' AFTER `data_status`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;
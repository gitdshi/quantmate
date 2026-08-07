-- 057_factor_pipeline_schema.sql
-- TASK-004: Factor mining → screen → persist pipeline.
--
-- Adds the columns needed for the pipeline to store per-factor metrics in
-- factor_screening_details and to track screening run progress in
-- factor_screening_results. Also creates the factor_recommendations table
-- used by the optional auto-promote stage.

-- ─── 1. factor_screening_results: progress tracking ─────────────────

SET @dbname = 'quantmate';
SET @tablename = 'factor_screening_results';

SET @colname = 'passed_count';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` INT DEFAULT 0 COMMENT ''Number of factors that passed thresholds'' AFTER `result_count`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

SET @colname = 'completed_at';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` TIMESTAMP NULL DEFAULT NULL COMMENT ''When the pipeline finished'' AFTER `status`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;


-- ─── 2. factor_screening_details: pipeline columns ──────────────────

SET @tablename = 'factor_screening_details';

SET @colname = 'factor_source';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` VARCHAR(64) DEFAULT NULL COMMENT ''alpha158/custom/rdagent'' AFTER `factor_set`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

SET @colname = 'rank_ir';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` DECIMAL(10,4) DEFAULT NULL COMMENT ''Rank IC-based information ratio'' AFTER `ic_ir`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

SET @colname = 'passed';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` TINYINT(1) NOT NULL DEFAULT 0 COMMENT ''Whether the factor passed IC/IR thresholds'' AFTER `rank_ir`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;

SET @colname = 'metrics';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` JSON DEFAULT NULL COMMENT ''Full metrics dict from the pipeline'' AFTER `passed`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;


-- ─── 3. factor_recommendations ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS `quantmate`.`factor_recommendations` (
    `id`              INT AUTO_INCREMENT PRIMARY KEY,
    `factor_name`     VARCHAR(256) NOT NULL,
    `factor_source`   VARCHAR(64) DEFAULT NULL,
    `universe`        VARCHAR(64) DEFAULT NULL,
    `rank_ir`         DECIMAL(10,4) DEFAULT NULL,
    `recommended_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_fr_name_uni` (`factor_name`, `universe`),
    INDEX `idx_fr_universe` (`universe`),
    INDEX `idx_fr_rank` (`rank_ir` DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Auto-promoted top factors from the mining pipeline';

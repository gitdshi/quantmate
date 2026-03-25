-- Migration 019: Factor screening tables + factor_evaluations.ic_std column
-- Supports: Factor mining, batch screening, correlation deduplication

-- ─── 1. Add ic_std to factor_evaluations (quantmate DB) ─────────────

SET @dbname = 'quantmate';
SET @tablename = 'factor_evaluations';

-- ic_std column
SET @colname = 'ic_std';
SET @preparedStatement = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @dbname AND table_name = @tablename AND column_name = @colname) = 0,
    CONCAT('ALTER TABLE `', @dbname, '`.`', @tablename, '` ADD COLUMN `', @colname, '` DECIMAL(8,6) DEFAULT NULL COMMENT \'IC standard deviation\' AFTER `ic_mean`'),
    'SELECT 1'
));
PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;


-- ─── 2. Factor screening results (quantmate DB) ─────────────────────

CREATE TABLE IF NOT EXISTS `quantmate`.`factor_screening_results` (
  `id`            INT AUTO_INCREMENT PRIMARY KEY,
  `user_id`       INT NOT NULL,
  `run_label`     VARCHAR(200) NOT NULL COMMENT 'User-defined label for this screening run',
  `config`        JSON DEFAULT NULL COMMENT 'Screening configuration (thresholds, date range, etc.)',
  `result_count`  INT DEFAULT 0,
  `status`        VARCHAR(20) DEFAULT 'completed',
  `created_at`    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_user` (`user_id`),
  INDEX `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Factor screening run metadata';


CREATE TABLE IF NOT EXISTS `quantmate`.`factor_screening_details` (
  `id`              INT AUTO_INCREMENT PRIMARY KEY,
  `run_id`          INT NOT NULL,
  `rank_order`      INT NOT NULL COMMENT 'Rank within screening run (1 = best)',
  `factor_name`     VARCHAR(200) NOT NULL,
  `factor_set`      VARCHAR(30) DEFAULT 'custom' COMMENT 'Alpha158, Alpha360, custom',
  `expression`      TEXT DEFAULT NULL COMMENT 'Expression (for custom factors)',
  `ic_mean`         DECIMAL(10,6) DEFAULT NULL,
  `ic_std`          DECIMAL(10,6) DEFAULT NULL,
  `ic_ir`           DECIMAL(10,4) DEFAULT NULL,
  `turnover`        DECIMAL(10,4) DEFAULT NULL,
  `long_ret`        DECIMAL(10,6) DEFAULT NULL,
  `short_ret`       DECIMAL(10,6) DEFAULT NULL,
  `long_short_ret`  DECIMAL(10,6) DEFAULT NULL,
  INDEX `idx_run` (`run_id`),
  INDEX `idx_rank` (`run_id`, `rank_order`),
  CONSTRAINT `fk_screening_run` FOREIGN KEY (`run_id`)
    REFERENCES `factor_screening_results`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Individual factor results within a screening run';

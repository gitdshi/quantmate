-- Migration 020: Strategy ↔ Factor bridge table
-- Links strategies to their constituent factors (for multi-factor strategies)

CREATE TABLE IF NOT EXISTS `quantmate`.`strategy_factors` (
  `id`            INT AUTO_INCREMENT PRIMARY KEY,
  `strategy_id`   INT NOT NULL,
  `factor_id`     INT DEFAULT NULL COMMENT 'FK to factor_definitions (NULL if using raw factor_name)',
  `factor_name`   VARCHAR(200) NOT NULL COMMENT 'Factor name (from definitions or Qlib built-in)',
  `factor_set`    VARCHAR(30) DEFAULT 'custom' COMMENT 'Alpha158/Alpha360/custom',
  `weight`        DECIMAL(8,4) DEFAULT 1.0 COMMENT 'Factor weight in composite signal',
  `direction`     TINYINT DEFAULT 1 COMMENT '1=long higher values, -1=short higher values',
  `created_at`    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_strategy` (`strategy_id`),
  INDEX `idx_factor` (`factor_id`),
  CONSTRAINT `fk_sf_strategy` FOREIGN KEY (`strategy_id`)
    REFERENCES `strategies`(`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_sf_factor` FOREIGN KEY (`factor_id`)
    REFERENCES `factor_definitions`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Strategy-Factor relationship for multi-factor strategies';

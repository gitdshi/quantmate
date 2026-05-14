-- Paper execution ledger for runtime recovery, lot reconstruction, and analytics.

SET @has_col_orders_paper_deployment_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'orders' AND column_name = 'paper_deployment_id'
);
SET @sql_add_orders_paper_deployment_id := IF(
    @has_col_orders_paper_deployment_id = 0,
    'ALTER TABLE `quantmate`.`orders` ADD COLUMN `paper_deployment_id` INT DEFAULT NULL AFTER `paper_account_id`',
    'SELECT 1'
);
PREPARE stmt_add_orders_paper_deployment_id FROM @sql_add_orders_paper_deployment_id;
EXECUTE stmt_add_orders_paper_deployment_id;
DEALLOCATE PREPARE stmt_add_orders_paper_deployment_id;

SET @has_idx_orders_paper_deployment_id := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'orders' AND index_name = 'idx_orders_paper_deployment_id'
);
SET @sql_add_idx_orders_paper_deployment_id := IF(
    @has_idx_orders_paper_deployment_id = 0,
    'ALTER TABLE `quantmate`.`orders` ADD INDEX `idx_orders_paper_deployment_id` (`paper_deployment_id`)',
    'SELECT 1'
);
PREPARE stmt_add_idx_orders_paper_deployment_id FROM @sql_add_idx_orders_paper_deployment_id;
EXECUTE stmt_add_idx_orders_paper_deployment_id;
DEALLOCATE PREPARE stmt_add_idx_orders_paper_deployment_id;

CREATE TABLE IF NOT EXISTS `quantmate`.`paper_order_events` (
    `id`                INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `user_id`           INT NOT NULL,
    `paper_account_id`  INT DEFAULT NULL,
    `deployment_id`     INT DEFAULT NULL,
    `order_id`          INT DEFAULT NULL,
    `event_type`        ENUM('submitted','filled','trade','cancelled','rejected','checkpoint') NOT NULL,
    `symbol`            VARCHAR(20) NOT NULL,
    `direction`         ENUM('buy','sell') DEFAULT NULL,
    `quantity`          INT NOT NULL DEFAULT 0,
    `price`             DECIMAL(10,4) DEFAULT NULL,
    `fee`               DECIMAL(10,4) NOT NULL DEFAULT 0,
    `payload`           JSON DEFAULT NULL,
    `occurred_at`       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_poe_user_time` (`user_id`, `occurred_at`),
    INDEX `idx_poe_account_time` (`paper_account_id`, `occurred_at`),
    INDEX `idx_poe_deployment_time` (`deployment_id`, `occurred_at`),
    INDEX `idx_poe_order` (`order_id`),
    CONSTRAINT `fk_poe_user` FOREIGN KEY (`user_id`) REFERENCES `quantmate`.`users`(`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_poe_account` FOREIGN KEY (`paper_account_id`) REFERENCES `quantmate`.`paper_accounts`(`id`) ON DELETE SET NULL,
    CONSTRAINT `fk_poe_deployment` FOREIGN KEY (`deployment_id`) REFERENCES `quantmate`.`paper_deployments`(`id`) ON DELETE SET NULL,
    CONSTRAINT `fk_poe_order` FOREIGN KEY (`order_id`) REFERENCES `quantmate`.`orders`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `quantmate`.`paper_position_lots` (
    `id`                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `user_id`            INT NOT NULL,
    `paper_account_id`   INT NOT NULL,
    `deployment_id`      INT DEFAULT NULL,
    `symbol`             VARCHAR(20) NOT NULL,
    `side`               ENUM('long','short') NOT NULL,
    `open_quantity`      INT NOT NULL,
    `remaining_quantity` INT NOT NULL,
    `open_price`         DECIMAL(10,4) NOT NULL,
    `realized_pnl`       DECIMAL(16,4) NOT NULL DEFAULT 0,
    `source_order_id`    INT DEFAULT NULL,
    `status`             ENUM('open','closed') NOT NULL DEFAULT 'open',
    `opened_at`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `closed_at`          DATETIME DEFAULT NULL,
    INDEX `idx_ppl_account_symbol_status` (`paper_account_id`, `symbol`, `status`),
    INDEX `idx_ppl_deployment_status` (`deployment_id`, `status`),
    INDEX `idx_ppl_user_status` (`user_id`, `status`),
    CONSTRAINT `fk_ppl_user` FOREIGN KEY (`user_id`) REFERENCES `quantmate`.`users`(`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_ppl_account` FOREIGN KEY (`paper_account_id`) REFERENCES `quantmate`.`paper_accounts`(`id`) ON DELETE CASCADE,
    CONSTRAINT `fk_ppl_deployment` FOREIGN KEY (`deployment_id`) REFERENCES `quantmate`.`paper_deployments`(`id`) ON DELETE SET NULL,
    CONSTRAINT `fk_ppl_order` FOREIGN KEY (`source_order_id`) REFERENCES `quantmate`.`orders`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `quantmate`.`paper_runtime_checkpoints` (
    `id`             INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `deployment_id`  INT NOT NULL,
    `runtime_mode`   VARCHAR(64) NOT NULL,
    `strategy_kind`  VARCHAR(32) NOT NULL,
    `checkpoint_json` JSON NOT NULL,
    `updated_at`     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_paper_runtime_checkpoint_deployment` (`deployment_id`),
    CONSTRAINT `fk_prc_deployment` FOREIGN KEY (`deployment_id`) REFERENCES `quantmate`.`paper_deployments`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
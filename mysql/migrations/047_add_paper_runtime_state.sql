-- Paper runtime state and heartbeats.

SET @has_col_paper_deploy_desired_status := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'desired_status'
);
SET @sql_add_paper_deploy_desired_status := IF(
    @has_col_paper_deploy_desired_status = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `desired_status` ENUM(''running'',''stopped'') NOT NULL DEFAULT ''running'' AFTER `execution_mode`',
    'SELECT 1'
);
PREPARE stmt_add_paper_deploy_desired_status FROM @sql_add_paper_deploy_desired_status;
EXECUTE stmt_add_paper_deploy_desired_status;
DEALLOCATE PREPARE stmt_add_paper_deploy_desired_status;

SET @has_col_paper_deploy_runtime_status := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'runtime_status'
);
SET @sql_add_paper_deploy_runtime_status := IF(
    @has_col_paper_deploy_runtime_status = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `runtime_status` ENUM(''pending'',''running'',''stopping'',''stopped'',''error'') NOT NULL DEFAULT ''pending'' AFTER `desired_status`',
    'SELECT 1'
);
PREPARE stmt_add_paper_deploy_runtime_status FROM @sql_add_paper_deploy_runtime_status;
EXECUTE stmt_add_paper_deploy_runtime_status;
DEALLOCATE PREPARE stmt_add_paper_deploy_runtime_status;

SET @has_col_paper_deploy_runtime_worker_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'runtime_worker_id'
);
SET @sql_add_paper_deploy_runtime_worker_id := IF(
    @has_col_paper_deploy_runtime_worker_id = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `runtime_worker_id` VARCHAR(128) DEFAULT NULL AFTER `runtime_status`',
    'SELECT 1'
);
PREPARE stmt_add_paper_deploy_runtime_worker_id FROM @sql_add_paper_deploy_runtime_worker_id;
EXECUTE stmt_add_paper_deploy_runtime_worker_id;
DEALLOCATE PREPARE stmt_add_paper_deploy_runtime_worker_id;

SET @has_col_paper_deploy_runtime_heartbeat_at := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'runtime_heartbeat_at'
);
SET @sql_add_paper_deploy_runtime_heartbeat_at := IF(
    @has_col_paper_deploy_runtime_heartbeat_at = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `runtime_heartbeat_at` DATETIME DEFAULT NULL AFTER `runtime_worker_id`',
    'SELECT 1'
);
PREPARE stmt_add_paper_deploy_runtime_heartbeat_at FROM @sql_add_paper_deploy_runtime_heartbeat_at;
EXECUTE stmt_add_paper_deploy_runtime_heartbeat_at;
DEALLOCATE PREPARE stmt_add_paper_deploy_runtime_heartbeat_at;

SET @has_col_paper_deploy_runtime_error := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'runtime_error'
);
SET @sql_add_paper_deploy_runtime_error := IF(
    @has_col_paper_deploy_runtime_error = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `runtime_error` TEXT DEFAULT NULL AFTER `runtime_heartbeat_at`',
    'SELECT 1'
);
PREPARE stmt_add_paper_deploy_runtime_error FROM @sql_add_paper_deploy_runtime_error;
EXECUTE stmt_add_paper_deploy_runtime_error;
DEALLOCATE PREPARE stmt_add_paper_deploy_runtime_error;

SET @has_col_paper_deploy_runtime_warning := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'runtime_warning'
);
SET @sql_add_paper_deploy_runtime_warning := IF(
    @has_col_paper_deploy_runtime_warning = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `runtime_warning` TEXT DEFAULT NULL AFTER `runtime_error`',
    'SELECT 1'
);
PREPARE stmt_add_paper_deploy_runtime_warning FROM @sql_add_paper_deploy_runtime_warning;
EXECUTE stmt_add_paper_deploy_runtime_warning;
DEALLOCATE PREPARE stmt_add_paper_deploy_runtime_warning;

CREATE TABLE IF NOT EXISTS `quantmate`.`paper_runtime_heartbeats` (
    `id`                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `deployment_id`      INT NOT NULL,
    `worker_id`          VARCHAR(128) NOT NULL,
    `runtime_status`     VARCHAR(32) NOT NULL,
    `runtime_mode`       VARCHAR(64) NOT NULL,
    `strategy_kind`      VARCHAR(32) NOT NULL,
    `gateway_name`       VARCHAR(128) DEFAULT NULL,
    `message`            TEXT DEFAULT NULL,
    `heartbeat_at`       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `created_at`         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_paper_runtime_heartbeat_deployment` (`deployment_id`),
    INDEX `idx_paper_runtime_heartbeat_worker` (`worker_id`),
    INDEX `idx_paper_runtime_heartbeat_status` (`runtime_status`),
    CONSTRAINT `fk_paper_runtime_heartbeat_deployment`
        FOREIGN KEY (`deployment_id`) REFERENCES `quantmate`.`paper_deployments`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
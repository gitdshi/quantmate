-- Paper trading deployments table
-- Tracks strategy deployments to paper trading simulation

CREATE TABLE IF NOT EXISTS `quantmate`.`paper_deployments` (
    `id`              INT AUTO_INCREMENT PRIMARY KEY,
    `user_id`         INT NOT NULL,
    `strategy_id`     INT NOT NULL,
    `strategy_name`   VARCHAR(255) NOT NULL,
    `vt_symbol`       VARCHAR(50) NOT NULL,
    `parameters`      JSON DEFAULT NULL,
    `status`          ENUM('running', 'stopped', 'error') NOT NULL DEFAULT 'running',
    `started_at`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `stopped_at`      TIMESTAMP NULL DEFAULT NULL,
    `created_at`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_paper_deploy_user`     (`user_id`),
    INDEX `idx_paper_deploy_strategy` (`strategy_id`),
    INDEX `idx_paper_deploy_status`   (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

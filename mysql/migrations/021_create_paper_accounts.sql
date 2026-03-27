-- Paper Trading: Paper Accounts, Account Snapshots, Paper Signals
-- and extensions to existing tables for paper trading support.

-- Paper accounts — independent virtual capital accounts for simulation
CREATE TABLE IF NOT EXISTS `quantmate`.`paper_accounts` (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    name            VARCHAR(100) NOT NULL,
    initial_capital DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    balance         DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    frozen          DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    market_value    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    total_pnl       DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    currency        ENUM('CNY','HKD','USD') NOT NULL DEFAULT 'CNY',
    market          ENUM('CN','HK','US') NOT NULL DEFAULT 'CN',
    status          ENUM('active','closed') NOT NULL DEFAULT 'active',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_pa_user   (user_id),
    INDEX idx_pa_status (status),
    CONSTRAINT fk_pa_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Daily equity snapshots for paper accounts (used for equity curve)
CREATE TABLE IF NOT EXISTS `quantmate`.`paper_account_snapshots` (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    account_id      INT          NOT NULL,
    snapshot_date   DATE         NOT NULL,
    balance         DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    market_value    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    total_equity    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    daily_pnl       DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_snap_acct_date (account_id, snapshot_date),
    CONSTRAINT fk_snap_acct FOREIGN KEY (account_id) REFERENCES `quantmate`.`paper_accounts`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Paper signals — strategy signal notifications for semi-auto mode
CREATE TABLE IF NOT EXISTS `quantmate`.`paper_signals` (
    id               INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id          INT          NOT NULL,
    paper_account_id INT          NOT NULL,
    deployment_id    INT          NOT NULL,
    symbol           VARCHAR(20)  NOT NULL,
    direction        ENUM('buy','sell') NOT NULL,
    quantity         INT          NOT NULL,
    suggested_price  DECIMAL(10,4) DEFAULT NULL,
    reason           TEXT         DEFAULT NULL,
    status           ENUM('pending','confirmed','rejected','expired') NOT NULL DEFAULT 'pending',
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_at     DATETIME     DEFAULT NULL,
    INDEX idx_ps_user   (user_id),
    INDEX idx_ps_acct   (paper_account_id),
    INDEX idx_ps_status (status),
    CONSTRAINT fk_ps_user FOREIGN KEY (user_id)          REFERENCES `quantmate`.`users`(id)             ON DELETE CASCADE,
    CONSTRAINT fk_ps_acct FOREIGN KEY (paper_account_id) REFERENCES `quantmate`.`paper_accounts`(id)    ON DELETE CASCADE,
    CONSTRAINT fk_ps_depl FOREIGN KEY (deployment_id)    REFERENCES `quantmate`.`paper_deployments`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Extend existing orders table with paper_account_id and buy_date for T+1
SET @has_col_orders_paper_account_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'orders' AND column_name = 'paper_account_id'
);
SET @sql_add_orders_paper_account_id := IF(
    @has_col_orders_paper_account_id = 0,
    'ALTER TABLE `quantmate`.`orders` ADD COLUMN `paper_account_id` INT DEFAULT NULL AFTER `mode`',
    'SELECT 1'
);
PREPARE stmt_add_orders_paper_account_id FROM @sql_add_orders_paper_account_id;
EXECUTE stmt_add_orders_paper_account_id;
DEALLOCATE PREPARE stmt_add_orders_paper_account_id;

SET @has_col_orders_buy_date := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'orders' AND column_name = 'buy_date'
);
SET @sql_add_orders_buy_date := IF(
    @has_col_orders_buy_date = 0,
    'ALTER TABLE `quantmate`.`orders` ADD COLUMN `buy_date` DATE DEFAULT NULL AFTER `paper_account_id`',
    'SELECT 1'
);
PREPARE stmt_add_orders_buy_date FROM @sql_add_orders_buy_date;
EXECUTE stmt_add_orders_buy_date;
DEALLOCATE PREPARE stmt_add_orders_buy_date;

-- Extend paper_deployments with paper_account_id and execution_mode
SET @has_col_deploy_paper_account_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'paper_deployments' AND column_name = 'paper_account_id'
);
SET @sql_add_deploy_paper_account_id := IF(
    @has_col_deploy_paper_account_id = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `paper_account_id` INT DEFAULT NULL AFTER `user_id`',
    'SELECT 1'
);
PREPARE stmt_add_deploy_paper_account_id FROM @sql_add_deploy_paper_account_id;
EXECUTE stmt_add_deploy_paper_account_id;
DEALLOCATE PREPARE stmt_add_deploy_paper_account_id;

SET @has_col_deploy_execution_mode := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'quantmate' AND table_name = 'paper_deployments' AND column_name = 'execution_mode'
);
SET @sql_add_deploy_execution_mode := IF(
    @has_col_deploy_execution_mode = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `execution_mode` ENUM(''auto'',''semi_auto'') NOT NULL DEFAULT ''auto'' AFTER `status`',
    'SELECT 1'
);
PREPARE stmt_add_deploy_execution_mode FROM @sql_add_deploy_execution_mode;
EXECUTE stmt_add_deploy_execution_mode;
DEALLOCATE PREPARE stmt_add_deploy_execution_mode;

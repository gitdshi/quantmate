-- Paper Trading: Paper Accounts, Account Snapshots, Paper Signals
-- and extensions to existing tables for paper trading support.

-- Paper accounts — independent virtual capital accounts for simulation
CREATE TABLE IF NOT EXISTS paper_accounts (
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
    CONSTRAINT fk_pa_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Daily equity snapshots for paper accounts (used for equity curve)
CREATE TABLE IF NOT EXISTS paper_account_snapshots (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    account_id      INT          NOT NULL,
    snapshot_date   DATE         NOT NULL,
    balance         DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    market_value    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    total_equity    DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    daily_pnl       DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_snap_acct_date (account_id, snapshot_date),
    CONSTRAINT fk_snap_acct FOREIGN KEY (account_id) REFERENCES paper_accounts(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Paper signals — strategy signal notifications for semi-auto mode
CREATE TABLE IF NOT EXISTS paper_signals (
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
    CONSTRAINT fk_ps_user FOREIGN KEY (user_id)          REFERENCES users(id)             ON DELETE CASCADE,
    CONSTRAINT fk_ps_acct FOREIGN KEY (paper_account_id) REFERENCES paper_accounts(id)    ON DELETE CASCADE,
    CONSTRAINT fk_ps_depl FOREIGN KEY (deployment_id)    REFERENCES paper_deployments(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Extend existing orders table with paper_account_id and buy_date for T+1
ALTER TABLE orders ADD COLUMN paper_account_id INT DEFAULT NULL AFTER mode;
ALTER TABLE orders ADD COLUMN buy_date DATE DEFAULT NULL AFTER paper_account_id;

-- Extend paper_deployments with paper_account_id and execution_mode
ALTER TABLE paper_deployments ADD COLUMN paper_account_id INT DEFAULT NULL AFTER user_id;
ALTER TABLE paper_deployments ADD COLUMN execution_mode ENUM('auto','semi_auto') NOT NULL DEFAULT 'auto' AFTER status;

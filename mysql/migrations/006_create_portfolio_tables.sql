-- Issue: Portfolio tables
CREATE TABLE IF NOT EXISTS portfolios (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    name        VARCHAR(100) NOT NULL DEFAULT 'Default',
    mode        ENUM('paper','live') NOT NULL DEFAULT 'paper',
    initial_cash DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    cash        DECIMAL(16,2) NOT NULL DEFAULT 1000000.00,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_port_user (user_id),
    CONSTRAINT fk_port_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS portfolio_positions (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    portfolio_id INT          NOT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    quantity     INT          NOT NULL DEFAULT 0,
    avg_cost     DECIMAL(10,4) NOT NULL DEFAULT 0,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_port_sym (portfolio_id, symbol),
    CONSTRAINT fk_pos_port FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS portfolio_transactions (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    portfolio_id INT          NOT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    direction    ENUM('buy','sell') NOT NULL,
    quantity     INT          NOT NULL,
    price        DECIMAL(10,4) NOT NULL,
    fee          DECIMAL(10,4) NOT NULL DEFAULT 0,
    strategy_id  INT          DEFAULT NULL,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tx_port (portfolio_id),
    INDEX idx_tx_date (created_at),
    CONSTRAINT fk_tx_port FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    portfolio_id INT          NOT NULL,
    date         DATE         NOT NULL,
    nav          DECIMAL(16,4) NOT NULL,
    total_value  DECIMAL(16,4) NOT NULL,
    cash         DECIMAL(16,2) NOT NULL,
    positions_json JSON       DEFAULT NULL,
    returns_1d   DECIMAL(10,6) DEFAULT NULL,
    returns_5d   DECIMAL(10,6) DEFAULT NULL,
    returns_20d  DECIMAL(10,6) DEFAULT NULL,
    returns_ytd  DECIMAL(10,6) DEFAULT NULL,
    UNIQUE KEY uq_snap_date (portfolio_id, date),
    CONSTRAINT fk_snap_port FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

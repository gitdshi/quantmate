-- P2 Issue: Paper Trading, Order Management, Broker Config, Risk Rules

-- Orders (paper + live)
CREATE TABLE IF NOT EXISTS `quantmate`.`orders` (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    portfolio_id INT          DEFAULT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    direction    ENUM('buy','sell') NOT NULL,
    order_type   ENUM('market','limit','stop','stop_limit') NOT NULL DEFAULT 'market',
    quantity     INT          NOT NULL,
    price        DECIMAL(10,4) DEFAULT NULL,
    stop_price   DECIMAL(10,4) DEFAULT NULL,
    status       ENUM('created','submitted','partial','filled','cancelled','rejected','expired') NOT NULL DEFAULT 'created',
    filled_quantity INT       NOT NULL DEFAULT 0,
    avg_fill_price  DECIMAL(10,4) DEFAULT NULL,
    fee          DECIMAL(10,4) NOT NULL DEFAULT 0,
    strategy_id  INT          DEFAULT NULL,
    mode         ENUM('paper','live') NOT NULL DEFAULT 'paper',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ord_user (user_id),
    INDEX idx_ord_status (status),
    INDEX idx_ord_symbol (symbol),
    INDEX idx_ord_date (created_at),
    CONSTRAINT fk_ord_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Trades (fills from orders)
CREATE TABLE IF NOT EXISTS `quantmate`.`trades` (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id        INT          NOT NULL,
    filled_quantity INT          NOT NULL,
    filled_price    DECIMAL(10,4) NOT NULL,
    fee             DECIMAL(10,4) NOT NULL DEFAULT 0,
    filled_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_trade_order FOREIGN KEY (order_id) REFERENCES `quantmate`.`orders`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Risk rules
CREATE TABLE IF NOT EXISTS `quantmate`.`risk_rules` (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    name        VARCHAR(100) NOT NULL,
    rule_type   ENUM('position_limit','drawdown','concentration','frequency','custom') NOT NULL,
    condition_expr VARCHAR(500) DEFAULT NULL,
    threshold   DECIMAL(10,4) NOT NULL,
    action      ENUM('block','reduce','warn') NOT NULL DEFAULT 'warn',
    is_active   TINYINT(1)   NOT NULL DEFAULT 1,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_rr_user (user_id),
    CONSTRAINT fk_rr_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Broker configurations
CREATE TABLE IF NOT EXISTS `quantmate`.`broker_configs` (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    broker_type     VARCHAR(50)  NOT NULL,
    name            VARCHAR(100) NOT NULL,
    config_json_encrypted TEXT   NOT NULL,
    is_active       TINYINT(1)   NOT NULL DEFAULT 1,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_bc_user (user_id),
    CONSTRAINT fk_bc_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

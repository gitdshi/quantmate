-- Issue: Trade Audit Log table
CREATE TABLE IF NOT EXISTS `quantmate`.`trade_logs` (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    timestamp   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type  VARCHAR(50)  NOT NULL COMMENT 'signal|risk_check|order_submit|fill|settlement',
    symbol      VARCHAR(20)  NOT NULL,
    direction   VARCHAR(10)  DEFAULT NULL COMMENT 'buy|sell',
    quantity    INT          DEFAULT NULL,
    price       DECIMAL(10,4) DEFAULT NULL,
    strategy_id INT          DEFAULT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'created',
    notes       TEXT         DEFAULT NULL,
    INDEX idx_tl_time (timestamp),
    INDEX idx_tl_symbol (symbol),
    INDEX idx_tl_strategy (strategy_id),
    INDEX idx_tl_event (event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

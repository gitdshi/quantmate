-- Composite backtest results table
-- Stores results of composite strategy backtests (daily-frequency, multi-symbol)

CREATE TABLE IF NOT EXISTS `quantmate`.`composite_backtests` (
  id                    INT AUTO_INCREMENT PRIMARY KEY,
  job_id                VARCHAR(64) NOT NULL UNIQUE,
  user_id               INT NOT NULL,
  composite_strategy_id INT NOT NULL,
  start_date            DATE NOT NULL,
  end_date              DATE NOT NULL,
  initial_capital       DECIMAL(15,2) DEFAULT 1000000.00,
  benchmark             VARCHAR(30) DEFAULT '000300.SH',
  status                ENUM('queued','running','completed','failed') DEFAULT 'queued',
  result                JSON COMMENT 'Performance metrics + equity curve + trade log',
  attribution           JSON COMMENT 'Layer attribution analysis',
  error_message         TEXT,
  started_at            TIMESTAMP NULL,
  completed_at          TIMESTAMP NULL,
  created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user (user_id),
  INDEX idx_composite (composite_strategy_id),
  INDEX idx_status (status),
  FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE,
  FOREIGN KEY (composite_strategy_id) REFERENCES `quantmate`.`composite_strategies`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

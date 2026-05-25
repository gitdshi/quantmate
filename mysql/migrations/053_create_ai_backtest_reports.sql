-- Structured AI interpretation artifacts for completed backtests.

CREATE TABLE IF NOT EXISTS `quantmate`.`ai_backtest_reports` (
    id           INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT NOT NULL,
    job_id       VARCHAR(64) NOT NULL,
    status       ENUM('queued','running','completed','failed') NOT NULL DEFAULT 'completed',
    report_json  JSON NOT NULL,
    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME DEFAULT NULL,
    UNIQUE KEY uq_ai_backtest_reports_user_job (user_id, job_id),
    INDEX idx_ai_backtest_reports_job (job_id),
    CONSTRAINT fk_ai_backtest_reports_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
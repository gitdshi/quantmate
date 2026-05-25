-- Workbench workflow sessions and event history.

CREATE TABLE IF NOT EXISTS `quantmate`.`workbench_sessions` (
    id                   INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id              INT NOT NULL,
    name                 VARCHAR(100) NOT NULL DEFAULT 'New Workflow',
    current_stage        ENUM('factor','strategy','backtest','paper_trade') NOT NULL DEFAULT 'factor',
    status               ENUM('draft','running_backtest','paper_active','archived') NOT NULL DEFAULT 'draft',
    state_json           JSON NOT NULL,
    last_backtest_job_id VARCHAR(64) DEFAULT NULL,
    last_deployment_id   INT DEFAULT NULL,
    created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ws_user_updated (user_id, updated_at),
    INDEX idx_ws_status (status),
    CONSTRAINT fk_ws_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `quantmate`.`workbench_session_events` (
    id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    session_id  INT NOT NULL,
    event_type  VARCHAR(64) NOT NULL,
    payload     JSON NOT NULL,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_wse_session_created (session_id, created_at),
    CONSTRAINT fk_wse_session FOREIGN KEY (session_id) REFERENCES `quantmate`.`workbench_sessions`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
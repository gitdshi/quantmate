-- P2 Issue: Alert Engine, Notification Channels, Reports

-- Alert rules
CREATE TABLE IF NOT EXISTS alert_rules (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    name         VARCHAR(100) NOT NULL,
    metric       VARCHAR(100) NOT NULL,
    comparator   ENUM('gt','gte','lt','lte','eq','neq') NOT NULL,
    threshold    DECIMAL(16,4) NOT NULL,
    time_window  INT          DEFAULT NULL,
    level        ENUM('info','warning','severe') NOT NULL DEFAULT 'warning',
    is_active    TINYINT(1)   NOT NULL DEFAULT 1,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ar_user (user_id),
    CONSTRAINT fk_ar_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Alert history
CREATE TABLE IF NOT EXISTS alert_history (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    rule_id      INT          DEFAULT NULL,
    user_id      INT          NOT NULL,
    triggered_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level        ENUM('info','warning','severe') NOT NULL,
    message      TEXT         NOT NULL,
    status       ENUM('unread','read','acknowledged') NOT NULL DEFAULT 'unread',
    INDEX idx_ah_user (user_id),
    INDEX idx_ah_date (triggered_at),
    CONSTRAINT fk_ah_rule FOREIGN KEY (rule_id) REFERENCES alert_rules(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Notification channels
CREATE TABLE IF NOT EXISTS notification_channels (
    id            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    channel_type  ENUM('email','wechat','dingtalk','telegram','slack','webhook') NOT NULL,
    config_json   JSON         NOT NULL,
    is_active     TINYINT(1)   NOT NULL DEFAULT 1,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_nc_user (user_id),
    CONSTRAINT fk_nc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Reports
CREATE TABLE IF NOT EXISTS reports (
    id            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    report_type   ENUM('daily','weekly','monthly','custom') NOT NULL,
    period_start  DATE         NOT NULL,
    period_end    DATE         NOT NULL,
    content_json  JSON         DEFAULT NULL,
    pdf_path      VARCHAR(500) DEFAULT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rpt_user (user_id),
    INDEX idx_rpt_date (period_start, period_end),
    CONSTRAINT fk_rpt_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

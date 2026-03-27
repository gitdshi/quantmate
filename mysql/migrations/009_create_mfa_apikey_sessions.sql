-- P2 Issue: MFA, API Key Management, Session Management

-- MFA settings
CREATE TABLE IF NOT EXISTS `quantmate`.`mfa_settings` (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    mfa_type    ENUM('totp','email') NOT NULL DEFAULT 'totp',
    secret_encrypted VARCHAR(512) NOT NULL,
    is_enabled  TINYINT(1)   NOT NULL DEFAULT 0,
    recovery_codes_hash TEXT DEFAULT NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_mfa_user (user_id),
    CONSTRAINT fk_mfa_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- API Keys
CREATE TABLE IF NOT EXISTS `quantmate`.`api_keys` (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    key_id       VARCHAR(64)  NOT NULL,
    secret_hash  VARCHAR(255) NOT NULL,
    name         VARCHAR(100) NOT NULL,
    permissions  JSON         DEFAULT NULL,
    expires_at   DATETIME     DEFAULT NULL,
    ip_whitelist JSON         DEFAULT NULL,
    rate_limit   INT          DEFAULT 60,
    is_active    TINYINT(1)   NOT NULL DEFAULT 1,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at DATETIME     DEFAULT NULL,
    UNIQUE KEY uq_key_id (key_id),
    INDEX idx_apikey_user (user_id),
    CONSTRAINT fk_apikey_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- User sessions
CREATE TABLE IF NOT EXISTS `quantmate`.`user_sessions` (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT          NOT NULL,
    token_hash   VARCHAR(255) NOT NULL,
    device_info  VARCHAR(255) DEFAULT NULL,
    ip_address   VARCHAR(45)  DEFAULT NULL,
    login_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_active_at DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at   DATETIME     NOT NULL,
    INDEX idx_sess_user (user_id),
    INDEX idx_sess_token (token_hash),
    CONSTRAINT fk_sess_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

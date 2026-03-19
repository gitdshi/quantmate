-- Issue #9: KYC verification tables
CREATE TABLE IF NOT EXISTS kyc_submissions (
    id            INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    status        ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
    real_name     VARCHAR(100) NOT NULL,
    id_number     VARCHAR(100) NOT NULL COMMENT 'Encrypted',
    id_type       VARCHAR(20)  NOT NULL DEFAULT 'mainland_id' COMMENT 'mainland_id|passport|hk_pass',
    id_front_path VARCHAR(500) NOT NULL COMMENT 'Encrypted file path',
    id_back_path  VARCHAR(500) NOT NULL COMMENT 'Encrypted file path',
    reviewer_id   INT          DEFAULT NULL,
    review_notes  TEXT         DEFAULT NULL,
    reviewed_at   DATETIME     DEFAULT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_kyc_user (user_id),
    INDEX idx_kyc_status (status),
    CONSTRAINT fk_kyc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

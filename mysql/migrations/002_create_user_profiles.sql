-- Issue #8: User Profile table
CREATE TABLE IF NOT EXISTS `quantmate`.`user_profiles` (
    user_id       INT          NOT NULL PRIMARY KEY,
    display_name  VARCHAR(100) DEFAULT NULL,
    avatar_url    VARCHAR(500) DEFAULT NULL,
    phone         VARCHAR(30)  DEFAULT NULL,
    timezone      VARCHAR(50)  DEFAULT 'Asia/Shanghai',
    language      VARCHAR(10)  DEFAULT 'zh-CN',
    bio           TEXT         DEFAULT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_profile_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

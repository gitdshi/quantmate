-- Issue #6: Watchlist tables
CREATE TABLE IF NOT EXISTS `quantmate`.`watchlists` (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id     INT          NOT NULL,
    name        VARCHAR(100) NOT NULL,
    description VARCHAR(500) DEFAULT NULL,
    sort_order  INT          NOT NULL DEFAULT 0,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_wl_user (user_id),
    CONSTRAINT fk_wl_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `quantmate`.`watchlist_items` (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    watchlist_id INT          NOT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    added_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes        VARCHAR(500) DEFAULT NULL,
    INDEX idx_wli_wl (watchlist_id),
    UNIQUE KEY uq_wli_symbol (watchlist_id, symbol),
    CONSTRAINT fk_wli_wl FOREIGN KEY (watchlist_id) REFERENCES `quantmate`.`watchlists`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

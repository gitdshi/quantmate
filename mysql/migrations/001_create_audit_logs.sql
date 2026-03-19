-- =============================================================================
-- Migration: Create audit_logs table
-- Issue #2: Audit Logging System
-- =============================================================================

USE quantmate;

CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    user_id INT,
    username VARCHAR(50),
    operation_type VARCHAR(50) NOT NULL COMMENT 'e.g. AUTH_LOGIN, STRATEGY_CREATE, DATA_ACCESS',
    resource_type VARCHAR(50) COMMENT 'e.g. user, strategy, backtest',
    resource_id VARCHAR(100) COMMENT 'ID of the affected resource',
    details JSON COMMENT 'Operation-specific details',
    ip_address VARCHAR(45),
    user_agent VARCHAR(500),
    http_method VARCHAR(10),
    http_path VARCHAR(500),
    http_status INT,
    INDEX idx_timestamp (timestamp),
    INDEX idx_user_id (user_id),
    INDEX idx_operation_type (operation_type),
    INDEX idx_resource_type (resource_type),
    INDEX idx_user_timestamp (user_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Immutable audit log for all user operations';

-- Restrict DELETE/UPDATE on audit_logs for application user (optional security layer)
-- Note: Enforce via application-level controls; DB-level requires separate restricted user

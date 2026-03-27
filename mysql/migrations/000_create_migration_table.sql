-- =============================================================================
-- Migration tracking table
-- Tracks which SQL migration scripts have been applied
-- =============================================================================

CREATE TABLE IF NOT EXISTS `quantmate`.`schema_migrations` (
    version VARCHAR(14) NOT NULL PRIMARY KEY COMMENT 'Migration version (YYYYMMDDHHMMSS)',
    name VARCHAR(255) NOT NULL COMMENT 'Migration script name',
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    checksum VARCHAR(64) COMMENT 'SHA-256 of the migration file content'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tracks applied database migrations';

-- Mark all existing init scripts as applied (baseline)
INSERT IGNORE INTO `quantmate`.`schema_migrations` (version, name) VALUES
    ('00000000000000', 'baseline_init_quantmate');

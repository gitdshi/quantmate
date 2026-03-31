-- Migration 026: RBAC roles, permissions, and user-role assignments

CREATE TABLE IF NOT EXISTS `quantmate`.`roles` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(50) NOT NULL UNIQUE,
  `description` VARCHAR(255) DEFAULT NULL,
  `is_system` BOOLEAN NOT NULL DEFAULT TRUE,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `quantmate`.`permissions` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `resource` VARCHAR(50) NOT NULL,
  `action` VARCHAR(20) NOT NULL,
  `description` VARCHAR(255) DEFAULT NULL,
  `is_system` BOOLEAN NOT NULL DEFAULT TRUE,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uk_resource_action` (`resource`, `action`)
);

CREATE TABLE IF NOT EXISTS `quantmate`.`role_permissions` (
  `role_id` INT NOT NULL,
  `permission_id` INT NOT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`role_id`, `permission_id`),
  CONSTRAINT `fk_role_permissions_role` FOREIGN KEY (`role_id`) REFERENCES `quantmate`.`roles` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_role_permissions_permission` FOREIGN KEY (`permission_id`) REFERENCES `quantmate`.`permissions` (`id`) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS `quantmate`.`user_roles` (
  `user_id` INT NOT NULL,
  `role_id` INT NOT NULL,
  `assigned_by` INT DEFAULT NULL,
  `assigned_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `is_active` BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (`user_id`, `role_id`),
  KEY `idx_user_roles_user_id` (`user_id`),
  KEY `idx_user_roles_role_id` (`role_id`),
  CONSTRAINT `fk_user_roles_role` FOREIGN KEY (`role_id`) REFERENCES `quantmate`.`roles` (`id`) ON DELETE CASCADE
);

INSERT IGNORE INTO `quantmate`.`roles` (`name`, `description`, `is_system`) VALUES
  ('admin', 'Full system administrator', TRUE),
  ('trader', 'Trading operator with execution permissions', TRUE),
  ('researcher', 'Research-focused role for strategy analysis', TRUE),
  ('viewer', 'Read-only access', TRUE);

INSERT IGNORE INTO `quantmate`.`permissions` (`resource`, `action`, `description`, `is_system`) VALUES
  ('strategies', 'read', 'Read strategy records', TRUE),
  ('strategies', 'write', 'Create or update strategies', TRUE),
  ('strategies', 'manage', 'Manage all strategies', TRUE),
  ('backtests', 'read', 'Read backtest results', TRUE),
  ('backtests', 'write', 'Create or cancel backtests', TRUE),
  ('backtests', 'manage', 'Manage all backtests', TRUE),
  ('data', 'read', 'Read market and research data', TRUE),
  ('data', 'write', 'Manage data jobs and sources', TRUE),
  ('data', 'manage', 'Manage all data permissions', TRUE),
  ('portfolios', 'read', 'Read portfolio data', TRUE),
  ('portfolios', 'write', 'Manage portfolio operations', TRUE),
  ('portfolios', 'manage', 'Manage all portfolios', TRUE),
  ('alerts', 'read', 'Read alert rules and history', TRUE),
  ('alerts', 'write', 'Create or update alert rules', TRUE),
  ('alerts', 'manage', 'Manage notification channels', TRUE),
  ('trading', 'read', 'Read trading state', TRUE),
  ('trading', 'write', 'Create or cancel orders', TRUE),
  ('trading', 'manage', 'Manage all trading operations', TRUE),
  ('reports', 'read', 'Read reports', TRUE),
  ('reports', 'write', 'Create reports', TRUE),
  ('reports', 'manage', 'Manage all reports', TRUE),
  ('system', 'read', 'Read system status', TRUE),
  ('system', 'write', 'Update system settings', TRUE),
  ('system', 'manage', 'Full system management', TRUE),
  ('account', 'read', 'Read user account data', TRUE),
  ('account', 'write', 'Update user account data', TRUE),
  ('account', 'manage', 'Manage users, roles, and permissions', TRUE),
  ('templates', 'read', 'Read templates', TRUE),
  ('templates', 'write', 'Create or update templates', TRUE),
  ('templates', 'manage', 'Manage template publishing', TRUE),
  ('teams', 'read', 'Read team workspaces', TRUE),
  ('teams', 'write', 'Create or update teams', TRUE),
  ('teams', 'manage', 'Manage all teams', TRUE);

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'admin';

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'trader'
  AND (
    (p.resource IN ('strategies', 'backtests', 'data', 'portfolios', 'alerts', 'trading', 'reports', 'system', 'account', 'templates', 'teams') AND p.action = 'read')
    OR (p.resource IN ('strategies', 'backtests', 'portfolios', 'alerts', 'trading', 'templates', 'teams') AND p.action = 'write')
    OR (p.resource = 'alerts' AND p.action = 'manage')
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'researcher'
  AND (
    (p.resource IN ('strategies', 'backtests', 'data', 'portfolios', 'alerts', 'trading', 'reports', 'system', 'account', 'templates', 'teams') AND p.action = 'read')
    OR (p.resource IN ('strategies', 'backtests', 'data', 'reports', 'templates') AND p.action = 'write')
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'viewer'
  AND p.action = 'read';

INSERT INTO `quantmate`.`user_roles` (`user_id`, `role_id`, `assigned_by`, `is_active`)
SELECT u.id, r.id, NULL, TRUE
FROM `quantmate`.`users` u
JOIN `quantmate`.`roles` r ON r.name = 'admin'
WHERE u.username = 'admin'
ON DUPLICATE KEY UPDATE
  `assigned_by` = VALUES(`assigned_by`),
  `is_active` = VALUES(`is_active`);

INSERT INTO `quantmate`.`schema_migrations` (`version`, `name`, `applied_at`)
VALUES ('20260331000026', '026_create_rbac_tables', NOW());

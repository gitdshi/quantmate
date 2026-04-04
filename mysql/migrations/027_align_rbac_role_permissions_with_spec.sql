-- Migration 027: Align system role permissions with RBAC spec v1

DELETE rp
FROM `quantmate`.`role_permissions` rp
JOIN `quantmate`.`roles` r ON r.id = rp.role_id
WHERE r.name IN ('trader', 'researcher', 'viewer');

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'trader'
  AND (
    (p.resource = 'strategies' AND p.action IN ('read', 'write'))
    OR (p.resource = 'backtests' AND p.action IN ('read', 'write'))
    OR (p.resource = 'trading' AND p.action IN ('read', 'write'))
    OR (p.resource = 'portfolios' AND p.action IN ('read', 'write'))
    OR (p.resource = 'reports' AND p.action IN ('read', 'write'))
    OR (p.resource = 'data' AND p.action = 'read')
    OR (p.resource = 'alerts' AND p.action = 'read')
    OR (p.resource = 'account' AND p.action IN ('read', 'write'))
    OR (p.resource = 'templates' AND p.action IN ('read', 'write'))
    OR (p.resource = 'teams' AND p.action IN ('read', 'write'))
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'researcher'
  AND (
    (p.resource = 'strategies' AND p.action IN ('read', 'write'))
    OR (p.resource = 'backtests' AND p.action IN ('read', 'write'))
    OR (p.resource = 'portfolios' AND p.action = 'read')
    OR (p.resource = 'reports' AND p.action = 'read')
    OR (p.resource = 'data' AND p.action = 'read')
    OR (p.resource = 'alerts' AND p.action = 'read')
    OR (p.resource = 'account' AND p.action IN ('read', 'write'))
    OR (p.resource = 'templates' AND p.action IN ('read', 'write'))
    OR (p.resource = 'teams' AND p.action = 'read')
  );

INSERT IGNORE INTO `quantmate`.`role_permissions` (`role_id`, `permission_id`)
SELECT r.id, p.id
FROM `quantmate`.`roles` r
JOIN `quantmate`.`permissions` p
WHERE r.name = 'viewer'
  AND (
    (p.resource = 'reports' AND p.action = 'read')
    OR (p.resource = 'data' AND p.action = 'read')
    OR (p.resource = 'alerts' AND p.action = 'read')
    OR (p.resource = 'account' AND p.action = 'read')
  );

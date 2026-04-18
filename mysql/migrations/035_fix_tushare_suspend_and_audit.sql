-- Migration 035: Fix Tushare suspend alias and restore ingest audit support
-- Date: 2026-04-17

CREATE DATABASE IF NOT EXISTS `tushare` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `tushare`.`ingest_audit` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `api_name` VARCHAR(64) NOT NULL,
    `params` JSON,
    `status` VARCHAR(32) DEFAULT 'running',
    `fetched_rows` INT DEFAULT 0,
    `started_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `finished_at` TIMESTAMP NULL,
    INDEX `idx_audit_api` (`api_name`),
    INDEX `idx_audit_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `quantmate`.`data_source_items`
  (
    source,
    item_key,
    item_name,
    enabled,
    description,
    requires_permission,
    target_database,
    target_table,
    table_created,
    sync_priority
  )
SELECT
  'tushare',
  'suspend_d',
  '停复牌当日信息',
  COALESCE(MAX(enabled), 0),
  '停复牌当日状态数据',
  NULL,
  'tushare',
  'suspend_d',
  COALESCE(MAX(table_created), 1),
  23
FROM `quantmate`.`data_source_items`
WHERE source = 'tushare' AND item_key IN ('suspend_d', 'suspend_daily')
HAVING COUNT(*) > 0
ON DUPLICATE KEY UPDATE
  item_name = VALUES(item_name),
  description = VALUES(description),
  requires_permission = VALUES(requires_permission),
  target_database = VALUES(target_database),
  target_table = VALUES(target_table),
  sync_priority = VALUES(sync_priority);

UPDATE `quantmate`.`data_source_items`
SET item_name = '停复牌当日信息',
    description = '停复牌当日状态数据',
    requires_permission = NULL,
    target_database = 'tushare',
    target_table = 'suspend_d',
    sync_priority = 23
WHERE source = 'tushare' AND item_key = 'suspend_d';

DELETE FROM `quantmate`.`data_source_items`
WHERE source = 'tushare' AND item_key = 'suspend_daily';

DELETE FROM `quantmate`.`sync_status_init`
WHERE source = 'tushare' AND interface_key = 'suspend_daily';

DELETE FROM `quantmate`.`data_sync_status`
WHERE source = 'tushare' AND interface_key = 'suspend_daily';
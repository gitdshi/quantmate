-- Migration 037: Store permission_points as integer thresholds
-- Date: 2026-04-17

-- Canonical rule:
-- - permission_points stores only the numeric threshold
-- - separately paid interfaces use permission_points = 0 and requires_permission = '1'
-- - AkShare interfaces use permission_points = 0

UPDATE `quantmate`.`data_source_items`
SET requires_permission = CASE
    WHEN source = 'tushare' AND LOWER(TRIM(COALESCE(requires_permission, ''))) IN ('1', 'true', 'yes', 'paid')
      THEN '1'
    WHEN source IN ('tushare', 'akshare') THEN '0'
    ELSE requires_permission
END;

UPDATE `quantmate`.`data_source_items`
SET permission_points = CASE
    WHEN source = 'akshare' THEN '0'
    WHEN LOWER(TRIM(COALESCE(requires_permission, ''))) IN ('1', 'true', 'yes', 'paid') THEN '0'
    ELSE COALESCE(CAST(REGEXP_SUBSTR(COALESCE(permission_points, ''), '[0-9]+') AS UNSIGNED), 0)
END;

ALTER TABLE `quantmate`.`data_source_items`
  MODIFY COLUMN `permission_points` INT NOT NULL DEFAULT 0 COMMENT '权限积分门槛; 0 表示不依赖积分判断';
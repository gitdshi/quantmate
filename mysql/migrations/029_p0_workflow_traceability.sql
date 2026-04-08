-- P0 workflow traceability fields for backtests and paper deployments.

SET @has_backtest_source_col := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'source'
);
SET @add_backtest_source_sql := IF(
    @has_backtest_source_col = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `source` VARCHAR(50) DEFAULT NULL AFTER `strategy_version`',
    'SELECT 1'
);
PREPARE stmt_add_backtest_source FROM @add_backtest_source_sql;
EXECUTE stmt_add_backtest_source;
DEALLOCATE PREPARE stmt_add_backtest_source;

SET @has_pd_source_bt := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'source_backtest_job_id'
);
SET @add_pd_source_bt_sql := IF(
    @has_pd_source_bt = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `source_backtest_job_id` VARCHAR(36) DEFAULT NULL AFTER `execution_mode`',
    'SELECT 1'
);
PREPARE stmt_add_pd_source_bt FROM @add_pd_source_bt_sql;
EXECUTE stmt_add_pd_source_bt;
DEALLOCATE PREPARE stmt_add_pd_source_bt;

SET @has_pd_source_ver := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'source_version_id'
);
SET @add_pd_source_ver_sql := IF(
    @has_pd_source_ver = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `source_version_id` INT DEFAULT NULL AFTER `source_backtest_job_id`',
    'SELECT 1'
);
PREPARE stmt_add_pd_source_ver FROM @add_pd_source_ver_sql;
EXECUTE stmt_add_pd_source_ver;
DEALLOCATE PREPARE stmt_add_pd_source_ver;

SET @has_pd_risk_status := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'risk_check_status'
);
SET @add_pd_risk_status_sql := IF(
    @has_pd_risk_status = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `risk_check_status` VARCHAR(16) DEFAULT NULL AFTER `source_version_id`',
    'SELECT 1'
);
PREPARE stmt_add_pd_risk_status FROM @add_pd_risk_status_sql;
EXECUTE stmt_add_pd_risk_status;
DEALLOCATE PREPARE stmt_add_pd_risk_status;

SET @has_pd_risk_summary := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'paper_deployments' AND column_name = 'risk_check_summary'
);
SET @add_pd_risk_summary_sql := IF(
    @has_pd_risk_summary = 0,
    'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `risk_check_summary` JSON DEFAULT NULL AFTER `risk_check_status`',
    'SELECT 1'
);
PREPARE stmt_add_pd_risk_summary FROM @add_pd_risk_summary_sql;
EXECUTE stmt_add_pd_risk_summary;
DEALLOCATE PREPARE stmt_add_pd_risk_summary;

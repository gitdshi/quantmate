-- Unified backtest run fields for strategy/factor/composite convergence.

SET @has_subject_type := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'subject_type'
);
SET @add_subject_type_sql := IF(
    @has_subject_type = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `subject_type` VARCHAR(32) DEFAULT NULL AFTER `completed_at`',
    'SELECT 1'
);
PREPARE stmt_add_subject_type FROM @add_subject_type_sql;
EXECUTE stmt_add_subject_type;
DEALLOCATE PREPARE stmt_add_subject_type;

SET @has_subject_id := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'subject_id'
);
SET @add_subject_id_sql := IF(
    @has_subject_id = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `subject_id` INT DEFAULT NULL AFTER `subject_type`',
    'SELECT 1'
);
PREPARE stmt_add_subject_id FROM @add_subject_id_sql;
EXECUTE stmt_add_subject_id;
DEALLOCATE PREPARE stmt_add_subject_id;

SET @has_subject_name := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'subject_name'
);
SET @add_subject_name_sql := IF(
    @has_subject_name = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `subject_name` VARCHAR(255) DEFAULT NULL AFTER `subject_id`',
    'SELECT 1'
);
PREPARE stmt_add_subject_name FROM @add_subject_name_sql;
EXECUTE stmt_add_subject_name;
DEALLOCATE PREPARE stmt_add_subject_name;

SET @has_engine_type := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'engine_type'
);
SET @add_engine_type_sql := IF(
    @has_engine_type = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `engine_type` VARCHAR(32) DEFAULT NULL AFTER `subject_name`',
    'SELECT 1'
);
PREPARE stmt_add_engine_type FROM @add_engine_type_sql;
EXECUTE stmt_add_engine_type;
DEALLOCATE PREPARE stmt_add_engine_type;

SET @has_scope_type := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'scope_type'
);
SET @add_scope_type_sql := IF(
    @has_scope_type = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `scope_type` VARCHAR(32) DEFAULT NULL AFTER `engine_type`',
    'SELECT 1'
);
PREPARE stmt_add_scope_type FROM @add_scope_type_sql;
EXECUTE stmt_add_scope_type;
DEALLOCATE PREPARE stmt_add_scope_type;

SET @has_request_payload := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'request_payload'
);
SET @add_request_payload_sql := IF(
    @has_request_payload = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `request_payload` JSON DEFAULT NULL AFTER `scope_type`',
    'SELECT 1'
);
PREPARE stmt_add_request_payload FROM @add_request_payload_sql;
EXECUTE stmt_add_request_payload;
DEALLOCATE PREPARE stmt_add_request_payload;

SET @has_summary_json := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'summary_json'
);
SET @add_summary_json_sql := IF(
    @has_summary_json = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `summary_json` JSON DEFAULT NULL AFTER `request_payload`',
    'SELECT 1'
);
PREPARE stmt_add_summary_json FROM @add_summary_json_sql;
EXECUTE stmt_add_summary_json;
DEALLOCATE PREPARE stmt_add_summary_json;

SET @has_artifacts_json := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'artifacts_json'
);
SET @add_artifacts_json_sql := IF(
    @has_artifacts_json = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `artifacts_json` JSON DEFAULT NULL AFTER `summary_json`',
    'SELECT 1'
);
PREPARE stmt_add_artifacts_json FROM @add_artifacts_json_sql;
EXECUTE stmt_add_artifacts_json;
DEALLOCATE PREPARE stmt_add_artifacts_json;

SET @has_diagnostics_json := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'diagnostics_json'
);
SET @add_diagnostics_json_sql := IF(
    @has_diagnostics_json = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `diagnostics_json` JSON DEFAULT NULL AFTER `artifacts_json`',
    'SELECT 1'
);
PREPARE stmt_add_diagnostics_json FROM @add_diagnostics_json_sql;
EXECUTE stmt_add_diagnostics_json;
DEALLOCATE PREPARE stmt_add_diagnostics_json;

SET @has_extensions_json := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'extensions_json'
);
SET @add_extensions_json_sql := IF(
    @has_extensions_json = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `extensions_json` JSON DEFAULT NULL AFTER `diagnostics_json`',
    'SELECT 1'
);
PREPARE stmt_add_extensions_json FROM @add_extensions_json_sql;
EXECUTE stmt_add_extensions_json;
DEALLOCATE PREPARE stmt_add_extensions_json;

SET @has_result_schema_version := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND column_name = 'result_schema_version'
);
SET @add_result_schema_version_sql := IF(
    @has_result_schema_version = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD COLUMN `result_schema_version` INT NOT NULL DEFAULT 2 AFTER `extensions_json`',
    'SELECT 1'
);
PREPARE stmt_add_result_schema_version FROM @add_result_schema_version_sql;
EXECUTE stmt_add_result_schema_version;
DEALLOCATE PREPARE stmt_add_result_schema_version;

SET @has_idx_subject_type := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND index_name = 'idx_backtest_subject_type'
);
SET @add_idx_subject_type_sql := IF(
    @has_idx_subject_type = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD INDEX `idx_backtest_subject_type` (`subject_type`)',
    'SELECT 1'
);
PREPARE stmt_add_idx_subject_type FROM @add_idx_subject_type_sql;
EXECUTE stmt_add_idx_subject_type;
DEALLOCATE PREPARE stmt_add_idx_subject_type;

SET @has_idx_subject := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND index_name = 'idx_backtest_subject'
);
SET @add_idx_subject_sql := IF(
    @has_idx_subject = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD INDEX `idx_backtest_subject` (`subject_type`, `subject_id`)',
    'SELECT 1'
);
PREPARE stmt_add_idx_subject FROM @add_idx_subject_sql;
EXECUTE stmt_add_idx_subject;
DEALLOCATE PREPARE stmt_add_idx_subject;

SET @has_idx_engine_type := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND index_name = 'idx_backtest_engine_type'
);
SET @add_idx_engine_type_sql := IF(
    @has_idx_engine_type = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD INDEX `idx_backtest_engine_type` (`engine_type`)',
    'SELECT 1'
);
PREPARE stmt_add_idx_engine_type FROM @add_idx_engine_type_sql;
EXECUTE stmt_add_idx_engine_type;
DEALLOCATE PREPARE stmt_add_idx_engine_type;

SET @has_idx_user_created := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'backtest_history' AND index_name = 'idx_backtest_user_created'
);
SET @add_idx_user_created_sql := IF(
    @has_idx_user_created = 0,
    'ALTER TABLE `quantmate`.`backtest_history` ADD INDEX `idx_backtest_user_created` (`user_id`, `created_at`)',
    'SELECT 1'
);
PREPARE stmt_add_idx_user_created FROM @add_idx_user_created_sql;
EXECUTE stmt_add_idx_user_created;
DEALLOCATE PREPARE stmt_add_idx_user_created;
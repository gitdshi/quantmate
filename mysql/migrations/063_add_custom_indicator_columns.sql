-- Add custom-indicator columns expected by the /indicators API.
--
-- indicator_configs was created (init SQL / migration 012) with only
-- params_schema/calc_function, while routes and IndicatorConfigDao write
-- display_name/description/default_params/formula. Custom indicator creation
-- fails with "Unknown column 'display_name'" until these columns exist.
-- Idempotent guards follow migration 028's pattern.

SET @has_display_name := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'indicator_configs' AND column_name = 'display_name'
);
SET @add_display_name_sql := IF(
    @has_display_name = 0,
    'ALTER TABLE `quantmate`.`indicator_configs` ADD COLUMN `display_name` VARCHAR(100) NULL AFTER `name`',
    'SELECT 1'
);
PREPARE stmt_add_display_name FROM @add_display_name_sql;
EXECUTE stmt_add_display_name;
DEALLOCATE PREPARE stmt_add_display_name;

SET @has_description := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'indicator_configs' AND column_name = 'description'
);
SET @add_description_sql := IF(
    @has_description = 0,
    'ALTER TABLE `quantmate`.`indicator_configs` ADD COLUMN `description` VARCHAR(500) NULL AFTER `category`',
    'SELECT 1'
);
PREPARE stmt_add_description FROM @add_description_sql;
EXECUTE stmt_add_description;
DEALLOCATE PREPARE stmt_add_description;

SET @has_default_params := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'indicator_configs' AND column_name = 'default_params'
);
SET @add_default_params_sql := IF(
    @has_default_params = 0,
    'ALTER TABLE `quantmate`.`indicator_configs` ADD COLUMN `default_params` JSON NULL AFTER `params_schema`',
    'SELECT 1'
);
PREPARE stmt_add_default_params FROM @add_default_params_sql;
EXECUTE stmt_add_default_params;
DEALLOCATE PREPARE stmt_add_default_params;

SET @has_formula := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'indicator_configs' AND column_name = 'formula'
);
SET @add_formula_sql := IF(
    @has_formula = 0,
    'ALTER TABLE `quantmate`.`indicator_configs` ADD COLUMN `formula` TEXT NULL AFTER `calc_function`',
    'SELECT 1'
);
PREPARE stmt_add_formula FROM @add_formula_sql;
EXECUTE stmt_add_formula;
DEALLOCATE PREPARE stmt_add_formula;

SET @ddl := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'paper_deployments'
              AND column_name = 'strategy_id'
              AND is_nullable = 'NO'
        ),
        'ALTER TABLE `quantmate`.`paper_deployments` MODIFY COLUMN `strategy_id` INT DEFAULT NULL',
        'SELECT 1'
    )
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'paper_deployments'
              AND column_name = 'composite_strategy_id'
        ),
        'SELECT 1',
        'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `composite_strategy_id` INT DEFAULT NULL AFTER `strategy_id`'
    )
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'paper_deployments'
              AND column_name = 'strategy_source_type'
        ),
        'SELECT 1',
        'ALTER TABLE `quantmate`.`paper_deployments` ADD COLUMN `strategy_source_type` ENUM(''strategy'',''composite'') NOT NULL DEFAULT ''strategy'' AFTER `composite_strategy_id`'
    )
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'paper_deployments'
              AND column_name = 'vt_symbol'
              AND character_maximum_length >= 255
        ),
        'SELECT 1',
        'ALTER TABLE `quantmate`.`paper_deployments` MODIFY COLUMN `vt_symbol` VARCHAR(255) NOT NULL'
    )
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'paper_deployments'
              AND index_name = 'idx_paper_deploy_composite_strategy'
        ),
        'SELECT 1',
        'ALTER TABLE `quantmate`.`paper_deployments` ADD INDEX `idx_paper_deploy_composite_strategy` (`composite_strategy_id`)'
    )
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'paper_deployments'
              AND index_name = 'idx_paper_deploy_source_type'
        ),
        'SELECT 1',
        'ALTER TABLE `quantmate`.`paper_deployments` ADD INDEX `idx_paper_deploy_source_type` (`strategy_source_type`)'
    )
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
-- 061_widen_paper_deployments_vt_symbol.sql
-- Composite/portfolio paper deployments store the full comma-separated
-- universe symbol list in paper_deployments.vt_symbol (e.g. 300+ CSI300
-- constituents). This exceeds the previous VARCHAR(255) limit and caused
-- "(1406, Data too long for column 'vt_symbol')" during strategy deployment.
-- The paper runtime normalizes vt_symbol by splitting on commas, so a TEXT
-- column is sufficient.

SET @ddl := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'paper_deployments'
              AND column_name = 'vt_symbol'
              AND data_type = 'varchar'
        ),
        'ALTER TABLE `quantmate`.`paper_deployments` MODIFY COLUMN `vt_symbol` TEXT NOT NULL',
        'SELECT 1'
    )
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
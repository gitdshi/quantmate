-- Correct a stale inferred unique key on stk_managers.
-- The historical table shape only enforced (ann_date, ts_code), which blocks
-- multiple manager rows for the same company announcement date. The current
-- runtime key includes manager identity fields and begin_date.

SET @sql = (
  SELECT IF(COUNT(*) > 0,
    'ALTER TABLE `tushare`.`stk_managers` DROP INDEX `ux_stk_managers_ann_date_ts_code`',
    'SELECT ''stk_managers.ux_stk_managers_ann_date_ts_code already dropped or missing'' AS info')
  FROM information_schema.statistics
  WHERE table_schema = 'tushare' AND table_name = 'stk_managers' AND index_name = 'ux_stk_managers_ann_date_ts_code'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
  SELECT IF(COUNT(*) = 0,
    'ALTER TABLE `tushare`.`stk_managers` ADD UNIQUE KEY `ux_stk_mgr_ann_ts_name_title_lev_begin` (`ann_date`,`ts_code`,`name`,`title`,`lev`,`begin_date`)',
    'SELECT ''stk_managers correct composite unique key already exists'' AS info')
  FROM (
    SELECT index_name,
           MIN(non_unique) AS non_unique,
           GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS cols
    FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'stk_managers' AND index_name <> 'PRIMARY'
    GROUP BY index_name
  ) existing_indexes
  WHERE non_unique = 0 AND cols = 'ann_date,ts_code,name,title,lev,begin_date'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
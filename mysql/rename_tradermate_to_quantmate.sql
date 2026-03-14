-- Rename schema "tradermate" to "quantmate" without deleting data.
-- MySQL has no direct RENAME DATABASE command, so this script renames all
-- tables from source schema to destination schema.
--
-- Safety behavior:
-- 1) Aborts if source schema does not exist.
-- 2) Aborts if source schema has no tables.
-- 3) Aborts if destination schema already contains any tables.
-- 4) Uses one atomic RENAME TABLE statement for all tables.
--
-- Notes:
-- - Source schema itself is NOT dropped.
-- - Data is preserved because table files are renamed, not copied/deleted.

SELECT table_schema, COUNT(*) AS tables_count
FROM information_schema.tables
WHERE table_schema IN ('tradermate', 'quantmate')
GROUP BY table_schema;

CREATE DATABASE IF NOT EXISTS `quantmate` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `quantmate`;

DELIMITER //
CREATE PROCEDURE rename_schema(IN src VARCHAR(64), IN dst VARCHAR(64))
BEGIN
  DECLARE src_schema_count INT DEFAULT 0;
  DECLARE src_table_count INT DEFAULT 0;
  DECLARE dst_table_count INT DEFAULT 0;

  SELECT COUNT(*)
    INTO src_schema_count
  FROM information_schema.schemata
  WHERE schema_name = src;

  IF src_schema_count = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Source schema does not exist; aborting rename.';
  END IF;

  SELECT COUNT(*)
    INTO src_table_count
  FROM information_schema.tables
  WHERE table_schema = src
    AND table_type = 'BASE TABLE';

  IF src_table_count = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Source schema has no base tables; aborting rename.';
  END IF;

  SELECT COUNT(*)
    INTO dst_table_count
  FROM information_schema.tables
  WHERE table_schema = dst;

  IF dst_table_count > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Destination schema is not empty; aborting rename.';
  END IF;

  SET @create_sql = CONCAT(
    'CREATE DATABASE IF NOT EXISTS `',
    REPLACE(dst, '`', '``'),
    '` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'
  );
  PREPARE stmt_create FROM @create_sql;
  EXECUTE stmt_create;
  DEALLOCATE PREPARE stmt_create;

  SET SESSION group_concat_max_len = 1024 * 1024;

  SELECT GROUP_CONCAT(
           CONCAT(
             '`', REPLACE(src, '`', '``'), '`.`', REPLACE(table_name, '`', '``'),
             '` TO `', REPLACE(dst, '`', '``'), '`.`', REPLACE(table_name, '`', '``'), '`'
           )
           ORDER BY table_name
           SEPARATOR ', '
         )
    INTO @rename_pairs
  FROM information_schema.tables
  WHERE table_schema = src
    AND table_type = 'BASE TABLE';

  IF @rename_pairs IS NULL OR CHAR_LENGTH(@rename_pairs) = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'No tables found to rename; aborting.';
  END IF;

  SET @rename_sql = CONCAT('RENAME TABLE ', @rename_pairs);
  PREPARE stmt_rename FROM @rename_sql;
  EXECUTE stmt_rename;
  DEALLOCATE PREPARE stmt_rename;
END//
DELIMITER ;

CALL rename_schema('tradermate', 'quantmate');
DROP PROCEDURE rename_schema;

SELECT table_schema, COUNT(*) AS tables_count
FROM information_schema.tables
WHERE table_schema IN ('tradermate', 'quantmate')
GROUP BY table_schema;

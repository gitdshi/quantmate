-- ============================================================================
-- Migration: Deduplicate tables and add UNIQUE constraints
-- Database: tushare
-- Date: 2026-02-13
-- ============================================================================
-- Purpose:
--   1. Remove duplicate records from stock_dividend and top10_holders
--   2. Add UNIQUE constraints to prevent future duplicates
--   3. Ensure ON DUPLICATE KEY UPDATE works correctly in DAOs
-- ============================================================================

USE tushare;

-- ============================================================================
-- STEP 1: Create backup tables
-- ============================================================================

-- Backup stock_dividend (35,928 duplicate groups, ~201k extra rows)
CREATE TABLE IF NOT EXISTS stock_dividend_backup_20260213 AS SELECT * FROM stock_dividend;
SELECT COUNT(*) as stock_dividend_backup_count FROM stock_dividend_backup_20260213;

-- Backup top10_holders (12,625 duplicate groups)
CREATE TABLE IF NOT EXISTS top10_holders_backup_20260213 AS SELECT * FROM top10_holders;
SELECT COUNT(*) as top10_holders_backup_count FROM top10_holders_backup_20260213;

-- Note: stock_daily and adj_factor have no duplicates, no backup needed


-- ============================================================================
-- STEP 2: Remove duplicates (keep lowest id for each logical key)
-- ============================================================================

-- Dedupe stock_dividend (logical key: ts_code, ann_date)
-- Strategy: Keep row with MIN(id) for each (ts_code, ann_date) group
DELETE FROM stock_dividend
WHERE id NOT IN (
  SELECT * FROM (
    SELECT MIN(id)
    FROM stock_dividend
    GROUP BY ts_code, ann_date
  ) t
);

SELECT 
  COUNT(*) as stock_dividend_after_dedupe,
  (SELECT COUNT(*) FROM stock_dividend_backup_20260213) - COUNT(*) as rows_removed
FROM stock_dividend;


-- Dedupe top10_holders (logical key: ts_code, end_date, holder_name)
-- Strategy: Keep row with MIN(id) for each (ts_code, end_date, holder_name) group
DELETE FROM top10_holders
WHERE id NOT IN (
  SELECT * FROM (
    SELECT MIN(id)
    FROM top10_holders
    GROUP BY ts_code, end_date, holder_name
  ) t
);

SELECT 
  COUNT(*) as top10_holders_after_dedupe,
  (SELECT COUNT(*) FROM top10_holders_backup_20260213) - COUNT(*) as rows_removed
FROM top10_holders;


-- ============================================================================
-- STEP 3: Add UNIQUE constraints
-- ============================================================================

-- stock_daily: Already has PRIMARY KEY (ts_code, trade_date) ✓
-- Verify:
SHOW CREATE TABLE stock_daily;

-- adj_factor: Already has PRIMARY KEY (ts_code, trade_date) ✓
-- Verify:
SHOW CREATE TABLE adj_factor;

-- stock_dividend: Add UNIQUE constraint on (ts_code, ann_date)
-- NOTE: This will prevent duplicate inserts going forward
ALTER TABLE stock_dividend 
ADD UNIQUE INDEX ux_stock_dividend_ts_ann (ts_code, ann_date);

-- Verify:
SHOW CREATE TABLE stock_dividend;

-- top10_holders: Add UNIQUE constraint on (ts_code, end_date, holder_name)
-- NOTE: This will prevent duplicate inserts going forward
ALTER TABLE top10_holders 
ADD UNIQUE INDEX ux_top10_holders_ts_end_holder (ts_code, end_date, holder_name);

-- Verify:
SHOW CREATE TABLE top10_holders;


-- ============================================================================
-- STEP 4: Verification queries
-- ============================================================================

-- Check for any remaining duplicates (should return 0 for all)
SELECT 'stock_daily' as tbl, COUNT(*) as duplicate_groups
FROM (
  SELECT ts_code, trade_date, COUNT(*) as cnt
  FROM stock_daily
  GROUP BY ts_code, trade_date
  HAVING cnt > 1
) t
UNION ALL
SELECT 'adj_factor' as tbl, COUNT(*) as duplicate_groups
FROM (
  SELECT ts_code, trade_date, COUNT(*) as cnt
  FROM adj_factor
  GROUP BY ts_code, trade_date
  HAVING cnt > 1
) t
UNION ALL
SELECT 'stock_dividend' as tbl, COUNT(*) as duplicate_groups
FROM (
  SELECT ts_code, ann_date, COUNT(*) as cnt
  FROM stock_dividend
  GROUP BY ts_code, ann_date
  HAVING cnt > 1
) t
UNION ALL
SELECT 'top10_holders' as tbl, COUNT(*) as duplicate_groups
FROM (
  SELECT ts_code, end_date, holder_name, COUNT(*) as cnt
  FROM top10_holders
  GROUP BY ts_code, end_date, holder_name
  HAVING cnt > 1
) t;


-- ============================================================================
-- STEP 5: Test UNIQUE constraints (optional manual test)
-- ============================================================================

-- Test duplicate insert (should fail or update, not create duplicate)
-- INSERT INTO stock_dividend (ts_code, ann_date, div_cash) 
-- VALUES ('000001.SZ', '2024-01-01', 1.00)
-- ON DUPLICATE KEY UPDATE div_cash = VALUES(div_cash);


-- ============================================================================
-- ROLLBACK PROCEDURE (if needed)
-- ============================================================================

-- If something goes wrong, restore from backups:
-- DROP TABLE IF EXISTS stock_dividend;
-- CREATE TABLE stock_dividend AS SELECT * FROM stock_dividend_backup_20260213;
-- 
-- DROP TABLE IF EXISTS top10_holders;
-- CREATE TABLE top10_holders AS SELECT * FROM top10_holders_backup_20260213;
-- 
-- Then re-add original indexes/constraints from tushare.sql


-- ============================================================================
-- CLEANUP (after verification, optionally drop backups)
-- ============================================================================

-- Wait 7+ days after migration before dropping backups
-- DROP TABLE IF EXISTS stock_dividend_backup_20260213;
-- DROP TABLE IF EXISTS top10_holders_backup_20260213;


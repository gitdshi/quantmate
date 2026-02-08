-- =============================================================================
-- Migration: Add strategy version tracking
-- Run this on existing databases to add version columns
-- =============================================================================

USE tradermate;

-- Add version column to strategies table (default 1 for existing rows)
ALTER TABLE strategies ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1 AFTER code;

-- Add strategy_version column to backtest_history table
ALTER TABLE backtest_history ADD COLUMN IF NOT EXISTS strategy_version INT AFTER strategy_class;

-- 056_add_model_path_to_training_runs.sql
-- TASK-005: Backtest model reuse — persist trained model to disk so subsequent
-- backtests can load it instead of re-training every time.

ALTER TABLE `qlib`.`model_training_runs`
    ADD COLUMN `model_path` VARCHAR(512) DEFAULT NULL
        COMMENT 'Absolute path to pickled model file (TASK-005 model reuse)'
        AFTER `metrics`;

-- Also persist model_path on backtest rows so ad-hoc backtests (without a
-- pre-existing training_run_id) can be reused too.
ALTER TABLE `qlib`.`qlib_backtest_results`
    ADD COLUMN `model_path` VARCHAR(512) DEFAULT NULL
        COMMENT 'Path to pickled model file used for this backtest (TASK-005)'
        AFTER `training_run_id`;

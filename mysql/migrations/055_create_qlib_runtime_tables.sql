-- Migration 055: Create missing Qlib runtime tables
-- Implements TASK-002: 4 tables are missing in the `qlib` DB and are required
-- by qlib_model_service.py, qlib_tasks.py, and data_converter.py.
-- Note: factor_screening_results / factor_screening_details already live in
-- the `quantmate` DB (migration 019) and are intentionally NOT recreated here.

-- ─── 1. model_training_runs ────────────────────────────────────────
-- References:
--   app/domains/ai/qlib_model_service.py
--     _create_training_run  (INSERT: user_id, model_type, factor_set, universe,
--                            train_start, train_end, valid_start, valid_end,
--                            test_start, test_end, hyperparams, status='queued')
--     _update_training_status (UPDATE: status)
--     _complete_training_run  (UPDATE: status='completed', metrics, completed_at)
--     _fail_training_run      (UPDATE: status='failed', error_message, completed_at)
--     list_training_runs / get_training_run (SELECT *)

CREATE TABLE IF NOT EXISTS `qlib`.`model_training_runs` (
    `id`            BIGINT AUTO_INCREMENT PRIMARY KEY,
    `user_id`       BIGINT NOT NULL COMMENT 'Owner user id',
    `model_type`    VARCHAR(64)  NOT NULL COMMENT 'LightGBM / XGBoost / Linear / ...',
    `factor_set`    VARCHAR(64)  NOT NULL DEFAULT 'Alpha158' COMMENT 'Alpha158 / Alpha360 / custom',
    `universe`      VARCHAR(64)  NOT NULL DEFAULT 'csi300',
    `train_start`   DATE,
    `train_end`     DATE,
    `valid_start`   DATE,
    `valid_end`     DATE,
    `test_start`    DATE,
    `test_end`      DATE,
    `hyperparams`   JSON DEFAULT NULL COMMENT 'Hyperparameter overrides',
    `metrics`       JSON DEFAULT NULL COMMENT 'Training metrics (IC, ICIR, ...)',
    `status`        VARCHAR(32)  NOT NULL DEFAULT 'queued' COMMENT 'queued/running/completed/failed',
    `error_message` TEXT,
    `started_at`    DATETIME,
    `completed_at`  DATETIME,
    `created_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_mtr_user` (`user_id`),
    INDEX `idx_mtr_status` (`status`),
    INDEX `idx_mtr_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib model training runs';


-- ─── 2. model_predictions ──────────────────────────────────────────
-- References:
--   app/domains/ai/qlib_model_service.py
--     _save_predictions (INSERT: training_run_id, instrument, trade_date, score, rank_pct)
--     get_predictions   (SELECT: instrument, trade_date, score, rank_pct)

CREATE TABLE IF NOT EXISTS `qlib`.`model_predictions` (
    `id`                BIGINT AUTO_INCREMENT PRIMARY KEY,
    `training_run_id`   BIGINT NOT NULL COMMENT 'FK -> model_training_runs.id',
    `instrument`        VARCHAR(32) NOT NULL,
    `trade_date`        DATE NOT NULL,
    `score`             DOUBLE,
    `rank_pct`          DOUBLE COMMENT 'Cross-sectional rank percentile (0-1)',
    `created_at`        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_mp_run_date_inst` (`training_run_id`, `trade_date`, `instrument`),
    INDEX `idx_mp_run_date` (`training_run_id`, `trade_date`),
    INDEX `idx_mp_date_inst` (`trade_date`, `instrument`),
    CONSTRAINT `fk_mp_training_run` FOREIGN KEY (`training_run_id`)
        REFERENCES `qlib`.`model_training_runs`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib model predictions';


-- ─── 3. qlib_backtest_results ──────────────────────────────────────
-- References:
--   app/worker/service/qlib_tasks.py
--     _create_qlib_backtest_record  (INSERT: user_id, job_id, training_run_id,
--                                    strategy_type, topk, n_drop, universe,
--                                    start_date, end_date, benchmark, status='queued')
--     _update_qlib_backtest_status  (UPDATE: status, error_message)
--     _complete_qlib_backtest       (UPDATE: status='completed', statistics,
--                                    portfolio_analysis, completed_at)

CREATE TABLE IF NOT EXISTS `qlib`.`qlib_backtest_results` (
    `id`                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    `user_id`             BIGINT NOT NULL,
    `job_id`              VARCHAR(64) NOT NULL UNIQUE COMMENT 'Business id (UUID)',
    `training_run_id`     BIGINT DEFAULT NULL COMMENT 'FK -> model_training_runs.id',
    `strategy_type`       VARCHAR(64) NOT NULL DEFAULT 'TopkDropout',
    `topk`                INT NOT NULL DEFAULT 50,
    `n_drop`              INT NOT NULL DEFAULT 5,
    `universe`            VARCHAR(64) NOT NULL DEFAULT 'csi300',
    `start_date`          DATE,
    `end_date`            DATE,
    `benchmark`           VARCHAR(32) DEFAULT 'SH000300',
    `statistics`          JSON DEFAULT NULL COMMENT 'Backtest statistics (annualized_return, sharpe, max_drawdown, ...)',
    `portfolio_analysis`  JSON DEFAULT NULL,
    `status`              VARCHAR(32) NOT NULL DEFAULT 'queued',
    `error_message`       TEXT,
    `started_at`          DATETIME,
    `completed_at`        DATETIME,
    `created_at`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_qbt_user` (`user_id`),
    INDEX `idx_qbt_status` (`status`),
    INDEX `idx_qbt_run` (`training_run_id`),
    INDEX `idx_qbt_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib backtest results';


-- ─── 4. data_conversion_log ────────────────────────────────────────
-- References:
--   app/infrastructure/qlib/data_converter.py
--     _log_conversion (INSERT: source_db, source_table, instrument_count,
--                      date_range_start, date_range_end, status='completed',
--                      completed_at)

CREATE TABLE IF NOT EXISTS `qlib`.`data_conversion_log` (
    `id`                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    `source_db`           VARCHAR(64) NOT NULL COMMENT 'tushare / akshare',
    `source_table`        VARCHAR(128) NOT NULL,
    `instrument_count`    INT,
    `date_range_start`    DATE,
    `date_range_end`      DATE,
    `status`              VARCHAR(32) NOT NULL DEFAULT 'pending',
    `error_message`       TEXT,
    `started_at`          DATETIME,
    `completed_at`        DATETIME,
    `created_at`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_dcl_status` (`status`),
    INDEX `idx_dcl_source` (`source_db`),
    INDEX `idx_dcl_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib data conversion log';

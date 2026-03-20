-- =============================================================================
-- QuantMate Qlib Database
-- Database: qlib - stores Qlib alpha factors, model results, and metadata
-- Data source: tushare + akshare (NOT vnpy)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS qlib CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE qlib;

-- =============================================================================
-- SECTION 1: Alpha Factor Storage
-- =============================================================================

-- Computed alpha factor values per instrument per date
CREATE TABLE IF NOT EXISTS alpha_factor_values (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    instrument   VARCHAR(20)  NOT NULL COMMENT 'e.g. SH600000',
    trade_date   DATE         NOT NULL,
    factor_set   VARCHAR(30)  NOT NULL COMMENT 'Alpha158, Alpha360, custom',
    factor_name  VARCHAR(100) NOT NULL,
    factor_value DOUBLE       DEFAULT NULL,
    INDEX idx_instrument_date (instrument, trade_date),
    INDEX idx_factor_set_date (factor_set, trade_date),
    UNIQUE KEY uq_inst_date_factor (instrument, trade_date, factor_set, factor_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib alpha factor values';

-- Factor set metadata (Alpha158, Alpha360, custom user factors)
CREATE TABLE IF NOT EXISTS alpha_factor_sets (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(30)  NOT NULL UNIQUE COMMENT 'Alpha158, Alpha360, custom',
    description  TEXT         DEFAULT NULL,
    factor_count INT          DEFAULT 0,
    source       VARCHAR(20)  DEFAULT 'qlib' COMMENT 'qlib or custom',
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Factor set definitions';

INSERT IGNORE INTO alpha_factor_sets (name, description, factor_count, source) VALUES
('Alpha158', 'Qlib built-in 158-factor set covering price/volume/volatility patterns', 158, 'qlib'),
('Alpha360', 'Qlib built-in 360-factor set with extended features', 360, 'qlib');

-- =============================================================================
-- SECTION 2: Model Training & Prediction
-- =============================================================================

-- Training run records
CREATE TABLE IF NOT EXISTS model_training_runs (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    model_type      VARCHAR(50)  NOT NULL COMMENT 'LightGBM, LSTM, Transformer, HIST, etc.',
    factor_set      VARCHAR(30)  NOT NULL DEFAULT 'Alpha158',
    universe        VARCHAR(50)  DEFAULT 'csi300' COMMENT 'csi300, csi500, all_a',
    train_start     DATE         NOT NULL,
    train_end       DATE         NOT NULL,
    valid_start     DATE         DEFAULT NULL,
    valid_end       DATE         DEFAULT NULL,
    test_start      DATE         DEFAULT NULL,
    test_end        DATE         DEFAULT NULL,
    hyperparams     JSON         DEFAULT NULL,
    metrics         JSON         DEFAULT NULL COMMENT '{"ic": 0.05, "icir": 0.4, "rank_ic": 0.06, ...}',
    status          ENUM('queued','running','completed','failed') NOT NULL DEFAULT 'queued',
    error_message   TEXT         DEFAULT NULL,
    model_path      VARCHAR(500) DEFAULT NULL COMMENT 'Path to saved model artifact',
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMP    NULL,
    INDEX idx_user (user_id),
    INDEX idx_status (status),
    INDEX idx_model_type (model_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib model training runs';

-- Model predictions (signal scores per instrument)
CREATE TABLE IF NOT EXISTS model_predictions (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    training_run_id INT          NOT NULL,
    instrument      VARCHAR(20)  NOT NULL,
    trade_date      DATE         NOT NULL,
    score           DOUBLE       NOT NULL COMMENT 'Predicted return score',
    rank_pct        DOUBLE       DEFAULT NULL COMMENT 'Cross-sectional percentile rank',
    INDEX idx_run_date (training_run_id, trade_date),
    INDEX idx_instrument_date (instrument, trade_date),
    CONSTRAINT fk_pred_run FOREIGN KEY (training_run_id) REFERENCES model_training_runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib model prediction scores';

-- =============================================================================
-- SECTION 3: Qlib Backtest Results
-- =============================================================================

CREATE TABLE IF NOT EXISTS qlib_backtest_results (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    job_id          VARCHAR(36)  NOT NULL UNIQUE,
    training_run_id INT          DEFAULT NULL,
    strategy_type   VARCHAR(50)  DEFAULT 'TopkDropout' COMMENT 'TopkDropout, WeightedAvg, etc.',
    topk            INT          DEFAULT 50,
    n_drop          INT          DEFAULT 5,
    universe        VARCHAR(50)  DEFAULT 'csi300',
    start_date      DATE         NOT NULL,
    end_date        DATE         NOT NULL,
    benchmark       VARCHAR(50)  DEFAULT 'SH000300',
    statistics      JSON         DEFAULT NULL COMMENT '{"annualized_return":..., "max_drawdown":..., "sharpe":...}',
    portfolio_analysis JSON      DEFAULT NULL COMMENT 'Long/short analysis, turnover, IC',
    status          ENUM('queued','running','completed','failed') NOT NULL DEFAULT 'queued',
    error_message   TEXT         DEFAULT NULL,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMP    NULL,
    INDEX idx_user (user_id),
    INDEX idx_job (job_id),
    INDEX idx_status (status),
    CONSTRAINT fk_qbt_run FOREIGN KEY (training_run_id) REFERENCES model_training_runs(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Qlib-based backtest results';

-- =============================================================================
-- SECTION 4: Data Sync Tracking
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_conversion_log (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    source_db       VARCHAR(20)  NOT NULL COMMENT 'tushare or akshare',
    source_table    VARCHAR(50)  NOT NULL,
    instrument_count INT         DEFAULT 0,
    date_range_start DATE        DEFAULT NULL,
    date_range_end   DATE        DEFAULT NULL,
    status          ENUM('running','completed','failed') NOT NULL DEFAULT 'running',
    error_message   TEXT         DEFAULT NULL,
    started_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMP    NULL,
    INDEX idx_source (source_db, source_table),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tracks tushare/akshare to Qlib data conversions';

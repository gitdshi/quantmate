-- P2 Issue: System Config, Parameter Optimization, Indicator Library

-- System configuration
CREATE TABLE IF NOT EXISTS `quantmate`.`system_configs` (
    id                 INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    config_key         VARCHAR(100) NOT NULL,
    config_value       TEXT         NOT NULL,
    category           VARCHAR(50)  NOT NULL DEFAULT 'general',
    description        VARCHAR(500) DEFAULT NULL,
    is_user_overridable TINYINT(1)  NOT NULL DEFAULT 0,
    updated_by         INT          DEFAULT NULL,
    updated_at         DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_config_key (config_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Data source configurations (token/rate limits)
CREATE TABLE IF NOT EXISTS `quantmate`.`data_source_configs` (
    id                INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source_type       VARCHAR(50)  NOT NULL,
    api_token_encrypted TEXT       DEFAULT NULL,
    rate_limit        INT          NOT NULL DEFAULT 60,
    is_enabled        TINYINT(1)   NOT NULL DEFAULT 1,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ds_type (source_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Optimization tasks
CREATE TABLE IF NOT EXISTS `quantmate`.`optimization_tasks` (
    id             INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id        INT          NOT NULL,
    strategy_id    INT          NOT NULL,
    search_method  ENUM('grid','random','bayesian') NOT NULL DEFAULT 'random',
    param_ranges   JSON         NOT NULL,
    objective      VARCHAR(50)  NOT NULL DEFAULT 'sharpe_ratio',
    max_iterations INT          NOT NULL DEFAULT 100,
    status         ENUM('pending','running','completed','failed','cancelled') NOT NULL DEFAULT 'pending',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at   DATETIME     DEFAULT NULL,
    INDEX idx_ot_user (user_id),
    CONSTRAINT fk_ot_user FOREIGN KEY (user_id) REFERENCES `quantmate`.`users`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Optimization results
CREATE TABLE IF NOT EXISTS `quantmate`.`optimization_task_results` (
    id          INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id     INT          NOT NULL,
    params      JSON         NOT NULL,
    metrics     JSON         NOT NULL,
    rank_num    INT          DEFAULT NULL,
    CONSTRAINT fk_otr_task FOREIGN KEY (task_id) REFERENCES `quantmate`.`optimization_tasks`(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indicator configurations
CREATE TABLE IF NOT EXISTS `quantmate`.`indicator_configs` (
    id           INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(50)  NOT NULL,
    category     ENUM('trend','oscillator','volume','volatility','custom') NOT NULL,
    params_schema JSON        DEFAULT NULL,
    calc_function TEXT        DEFAULT NULL,
    user_id      INT          DEFAULT NULL,
    is_builtin   TINYINT(1)   NOT NULL DEFAULT 0,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ind_name_user (name, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert built-in indicators
INSERT IGNORE INTO `quantmate`.`indicator_configs` (name, category, params_schema, is_builtin) VALUES
('SMA', 'trend', '{"period": {"type": "int", "default": 20, "min": 2, "max": 500}}', 1),
('EMA', 'trend', '{"period": {"type": "int", "default": 20, "min": 2, "max": 500}}', 1),
('BOLL', 'trend', '{"period": {"type": "int", "default": 20}, "std_dev": {"type": "float", "default": 2.0}}', 1),
('SAR', 'trend', '{"af": {"type": "float", "default": 0.02}, "max_af": {"type": "float", "default": 0.2}}', 1),
('ADX', 'trend', '{"period": {"type": "int", "default": 14}}', 1),
('RSI', 'oscillator', '{"period": {"type": "int", "default": 14, "min": 2, "max": 100}}', 1),
('MACD', 'oscillator', '{"fast": {"type": "int", "default": 12}, "slow": {"type": "int", "default": 26}, "signal": {"type": "int", "default": 9}}', 1),
('KDJ', 'oscillator', '{"n": {"type": "int", "default": 9}, "m1": {"type": "int", "default": 3}, "m2": {"type": "int", "default": 3}}', 1),
('CCI', 'oscillator', '{"period": {"type": "int", "default": 14}}', 1),
('WR', 'oscillator', '{"period": {"type": "int", "default": 14}}', 1),
('ROC', 'oscillator', '{"period": {"type": "int", "default": 12}}', 1),
('OBV', 'volume', '{}', 1),
('VWAP', 'volume', '{}', 1),
('MFI', 'volume', '{"period": {"type": "int", "default": 14}}', 1),
('ATR', 'volatility', '{"period": {"type": "int", "default": 14}}', 1),
('HV', 'volatility', '{"period": {"type": "int", "default": 20}}', 1);

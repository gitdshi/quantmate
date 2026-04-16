-- Migration 032: RD-Agent autonomous factor mining tables
-- Supports the Auto Pilot feature for LLM-driven factor discovery
-- Tables are created in the quantmate database (application user has full access)

-- Mining run records
CREATE TABLE IF NOT EXISTS `qlib`.`rdagent_runs` (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id          VARCHAR(36)  NOT NULL UNIQUE,
    user_id         BIGINT       NOT NULL,
    scenario        VARCHAR(32)  NOT NULL DEFAULT 'fin_factor',
    config          JSON,
    status          VARCHAR(20)  NOT NULL DEFAULT 'queued',
    current_iteration INT        DEFAULT 0,
    total_iterations  INT        DEFAULT 0,
    error_message   TEXT,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at      DATETIME,
    completed_at    DATETIME,
    INDEX idx_rdagent_runs_user (user_id),
    INDEX idx_rdagent_runs_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Iteration-level results within a run
CREATE TABLE IF NOT EXISTS `qlib`.`rdagent_iterations` (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id           VARCHAR(36) NOT NULL,
    iteration_number INT         NOT NULL,
    hypothesis       TEXT,
    experiment_code  MEDIUMTEXT,
    metrics          JSON,
    feedback         TEXT,
    status           VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at       DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rdagent_iter_run (run_id),
    CONSTRAINT fk_rdagent_iter_run FOREIGN KEY (run_id) REFERENCES `rdagent_runs`(run_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Factors discovered by RD-Agent mining
CREATE TABLE IF NOT EXISTS `qlib`.`rdagent_discovered_factors` (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id       VARCHAR(36)  NOT NULL,
    factor_name  VARCHAR(128) NOT NULL,
    expression   TEXT         NOT NULL,
    description  TEXT,
    ic_mean      DOUBLE,
    icir         DOUBLE,
    sharpe       DOUBLE,
    status       VARCHAR(20)  NOT NULL DEFAULT 'discovered',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rdagent_factors_run (run_id),
    CONSTRAINT fk_rdagent_factors_run FOREIGN KEY (run_id) REFERENCES `qlib`.`rdagent_runs`(run_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Data catalog cache (optional — stores last scan results)
CREATE TABLE IF NOT EXISTS `qlib`.`data_catalog` (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    source       VARCHAR(32)  NOT NULL,
    table_name   VARCHAR(128) NOT NULL,
    column_name  VARCHAR(128) NOT NULL,
    data_type    VARCHAR(64)  NOT NULL,
    category     VARCHAR(32),
    is_numeric   TINYINT(1)   NOT NULL DEFAULT 0,
    scanned_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_catalog_col (source, table_name, column_name),
    INDEX idx_catalog_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
-- 060_create_autopilot_tables.sql
-- Autopilot closed-loop runtime state, stage tracking, and decisions.
--
-- Three tables in `quantmate`:
--   autopilot_runs       one row per business-day loop run
--   autopilot_stages     one row per stage execution (idempotent per run+stage)
--   autopilot_decisions  decision-engine outputs (auditable)

CREATE TABLE IF NOT EXISTS `quantmate`.`autopilot_runs` (
    `id`            BIGINT AUTO_INCREMENT PRIMARY KEY,
    `run_id`        VARCHAR(64) NOT NULL,
    `business_date` DATE NOT NULL,
    `market`        VARCHAR(32) NOT NULL DEFAULT 'CN',
    `status`        VARCHAR(20) NOT NULL DEFAULT 'pending',
    `started_at`    TIMESTAMP NULL DEFAULT NULL,
    `ended_at`      TIMESTAMP NULL DEFAULT NULL,
    `created_at`    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_autopilot_runs_run_id` (`run_id`),
    INDEX `idx_autopilot_runs_date` (`business_date`),
    INDEX `idx_autopilot_runs_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Autopilot closed-loop run (one per business day)';

CREATE TABLE IF NOT EXISTS `quantmate`.`autopilot_stages` (
    `id`         BIGINT AUTO_INCREMENT PRIMARY KEY,
    `run_id`     VARCHAR(64) NOT NULL,
    `stage`      VARCHAR(50) NOT NULL,
    `status`     VARCHAR(20) NOT NULL DEFAULT 'pending',
    `attempt`    INT NOT NULL DEFAULT 0,
    `params`     JSON DEFAULT NULL,
    `result`     JSON DEFAULT NULL,
    `error`      TEXT DEFAULT NULL,
    `started_at` TIMESTAMP NULL DEFAULT NULL,
    `ended_at`   TIMESTAMP NULL DEFAULT NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_autopilot_stage` (`run_id`, `stage`),
    INDEX `idx_autopilot_stages_status` (`stage`, `status`),
    INDEX `idx_autopilot_stages_run` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Autopilot per-stage execution records';

CREATE TABLE IF NOT EXISTS `quantmate`.`autopilot_decisions` (
    `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
    `run_id`          VARCHAR(64) NOT NULL,
    `decision_type`   VARCHAR(50) NOT NULL,
    `action`          VARCHAR(50) NOT NULL,
    `subject_type`    VARCHAR(50) DEFAULT NULL,
    `subject_id`      VARCHAR(128) DEFAULT NULL,
    `input_summary`   JSON DEFAULT NULL,
    `reason`          TEXT DEFAULT NULL,
    `approval_status` VARCHAR(20) NOT NULL DEFAULT 'auto',
    `created_at`      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_autopilot_decisions_run` (`run_id`),
    INDEX `idx_autopilot_decisions_type` (`decision_type`),
    INDEX `idx_autopilot_decisions_approval` (`approval_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Autopilot decision-engine outputs';
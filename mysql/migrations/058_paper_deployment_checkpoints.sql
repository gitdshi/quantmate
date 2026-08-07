-- 058_paper_deployment_checkpoints.sql
-- TASK-011: Paper deployment checkpoint table.
--
-- Replaces the prior pickle-based checkpoint mechanism with a JSON column
-- so that schema/dependency changes don't break recovery. The
-- CheckpointManager uses UPSERT to guarantee atomic writes.

CREATE TABLE IF NOT EXISTS `quantmate`.`paper_deployment_checkpoints` (
    `deployment_id`  VARCHAR(64) NOT NULL COMMENT 'paper_deployments.id (as string)',
    `state_json`     JSON NOT NULL COMMENT 'Serialized runtime state (positions, cash, last_trade_date, ...)',
    `updated_at`     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`deployment_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='TASK-011: durable paper deployment runtime state';

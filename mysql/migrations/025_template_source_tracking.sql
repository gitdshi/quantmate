-- Migration 025: Add source tracking columns to strategy_templates
-- Enables distinguishing marketplace-cloned templates from user-created ones

ALTER TABLE `quantmate`.`strategy_templates`
  ADD COLUMN source_template_id INT DEFAULT NULL AFTER author_id,
  ADD COLUMN source ENUM('marketplace','personal') NOT NULL DEFAULT 'personal' AFTER source_template_id;

CREATE INDEX idx_source_template ON `quantmate`.`strategy_templates`(source_template_id);
CREATE INDEX idx_source ON `quantmate`.`strategy_templates`(source);

-- Record migration
INSERT INTO `quantmate`.`schema_migrations` (version, name, applied_at)
VALUES ('20260330000025', '025_template_source_tracking', NOW());

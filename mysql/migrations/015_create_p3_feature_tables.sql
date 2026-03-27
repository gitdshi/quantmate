-- Migration 015: P3 Feature Tables (AI, Factor Lab, Templates, Teams)

-- AI conversation history
CREATE TABLE IF NOT EXISTS `quantmate`.`ai_conversations` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `user_id`     INT NOT NULL,
  `session_id`  VARCHAR(64) NOT NULL COMMENT 'UUID grouping a conversation',
  `title`       VARCHAR(200) DEFAULT NULL,
  `model_used`  VARCHAR(50) DEFAULT NULL,
  `total_tokens` INT DEFAULT 0,
  `status`      VARCHAR(20) DEFAULT 'active' COMMENT 'active/archived',
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_user` (`user_id`),
  INDEX `idx_session` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- AI conversation messages
CREATE TABLE IF NOT EXISTS `quantmate`.`ai_messages` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `conversation_id` INT NOT NULL,
  `role`        VARCHAR(20) NOT NULL COMMENT 'user/assistant/system',
  `content`     TEXT NOT NULL,
  `tokens`      INT DEFAULT 0,
  `metadata`    JSON DEFAULT NULL COMMENT 'tool_calls, citations, etc.',
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_conversation` (`conversation_id`),
  CONSTRAINT `fk_msg_conversation` FOREIGN KEY (`conversation_id`) REFERENCES `quantmate`.`ai_conversations`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- AI model configurations
CREATE TABLE IF NOT EXISTS `quantmate`.`ai_model_configs` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `model_name`  VARCHAR(50) NOT NULL UNIQUE,
  `provider`    VARCHAR(30) NOT NULL COMMENT 'openai/anthropic/local/deepseek',
  `endpoint`    VARCHAR(500) DEFAULT NULL,
  `api_key_ref` VARCHAR(100) DEFAULT NULL COMMENT 'Reference to secrets manager key',
  `temperature` DECIMAL(3,2) DEFAULT 0.70,
  `max_tokens`  INT DEFAULT 4096,
  `enabled`     BOOLEAN DEFAULT TRUE,
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Factor definitions (Factor Lab)
CREATE TABLE IF NOT EXISTS `quantmate`.`factor_definitions` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `user_id`     INT NOT NULL,
  `name`        VARCHAR(100) NOT NULL,
  `category`    VARCHAR(50) DEFAULT NULL COMMENT 'momentum/value/quality/volatility/custom',
  `expression`  TEXT NOT NULL COMMENT 'Factor formula or code',
  `description` TEXT DEFAULT NULL,
  `params`      JSON DEFAULT NULL COMMENT 'Configurable parameters',
  `status`      VARCHAR(20) DEFAULT 'draft' COMMENT 'draft/backtesting/validated/published',
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_user` (`user_id`),
  INDEX `idx_category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Factor evaluation results
CREATE TABLE IF NOT EXISTS `quantmate`.`factor_evaluations` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `factor_id`   INT NOT NULL,
  `start_date`  DATE NOT NULL,
  `end_date`    DATE NOT NULL,
  `ic_mean`     DECIMAL(8,6) DEFAULT NULL COMMENT 'Information Coefficient mean',
  `ic_ir`       DECIMAL(8,6) DEFAULT NULL COMMENT 'IC Information Ratio',
  `turnover`    DECIMAL(8,6) DEFAULT NULL,
  `long_ret`    DECIMAL(10,6) DEFAULT NULL COMMENT 'Long portfolio return',
  `short_ret`   DECIMAL(10,6) DEFAULT NULL COMMENT 'Short portfolio return',
  `long_short_ret` DECIMAL(10,6) DEFAULT NULL,
  `metrics`     JSON DEFAULT NULL COMMENT 'Full evaluation metrics',
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_factor` (`factor_id`),
  CONSTRAINT `fk_eval_factor` FOREIGN KEY (`factor_id`) REFERENCES `quantmate`.`factor_definitions`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Strategy templates
CREATE TABLE IF NOT EXISTS `quantmate`.`strategy_templates` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `author_id`   INT NOT NULL,
  `name`        VARCHAR(100) NOT NULL,
  `category`    VARCHAR(50) DEFAULT NULL COMMENT 'trend/mean_revert/arbitrage/ml/multi_factor',
  `description` TEXT DEFAULT NULL,
  `code`        MEDIUMTEXT NOT NULL,
  `params_schema` JSON DEFAULT NULL COMMENT 'JSON Schema for parameters',
  `default_params` JSON DEFAULT NULL,
  `version`     VARCHAR(20) DEFAULT '1.0.0',
  `visibility`  VARCHAR(20) DEFAULT 'private' COMMENT 'private/team/public',
  `downloads`   INT DEFAULT 0,
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_author` (`author_id`),
  INDEX `idx_visibility` (`visibility`),
  INDEX `idx_category` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Strategy shares (team-level sharing)
CREATE TABLE IF NOT EXISTS `quantmate`.`strategy_shares` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `strategy_id` INT NOT NULL,
  `shared_by`   INT NOT NULL,
  `shared_with_user_id` INT DEFAULT NULL,
  `shared_with_team_id` INT DEFAULT NULL,
  `permission`  VARCHAR(20) DEFAULT 'view' COMMENT 'view/clone/edit',
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_strategy` (`strategy_id`),
  INDEX `idx_shared_user` (`shared_with_user_id`),
  INDEX `idx_shared_team` (`shared_with_team_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Team workspaces
CREATE TABLE IF NOT EXISTS `quantmate`.`team_workspaces` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `name`        VARCHAR(100) NOT NULL,
  `description` TEXT DEFAULT NULL,
  `owner_id`    INT NOT NULL,
  `avatar_url`  VARCHAR(500) DEFAULT NULL,
  `max_members` INT DEFAULT 10,
  `status`      VARCHAR(20) DEFAULT 'active' COMMENT 'active/archived',
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_owner` (`owner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Team workspace members
CREATE TABLE IF NOT EXISTS `quantmate`.`workspace_members` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `workspace_id` INT NOT NULL,
  `user_id`     INT NOT NULL,
  `role`        VARCHAR(20) DEFAULT 'member' COMMENT 'owner/admin/member/viewer',
  `joined_at`   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `uq_workspace_user` (`workspace_id`, `user_id`),
  INDEX `idx_user` (`user_id`),
  CONSTRAINT `fk_member_workspace` FOREIGN KEY (`workspace_id`) REFERENCES `quantmate`.`team_workspaces`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- P4: Strategy comments (marketplace)
CREATE TABLE IF NOT EXISTS `quantmate`.`strategy_comments` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `template_id` INT NOT NULL,
  `user_id`     INT NOT NULL,
  `content`     TEXT NOT NULL,
  `parent_id`   INT DEFAULT NULL,
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_template` (`template_id`),
  INDEX `idx_parent` (`parent_id`),
  CONSTRAINT `fk_comment_template` FOREIGN KEY (`template_id`) REFERENCES `quantmate`.`strategy_templates`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- P4: Strategy ratings
CREATE TABLE IF NOT EXISTS `quantmate`.`strategy_ratings` (
  `id`          INT AUTO_INCREMENT PRIMARY KEY,
  `template_id` INT NOT NULL,
  `user_id`     INT NOT NULL,
  `rating`      TINYINT NOT NULL COMMENT '1-5',
  `review`      TEXT DEFAULT NULL,
  `created_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `uq_template_user` (`template_id`, `user_id`),
  INDEX `idx_template` (`template_id`),
  CONSTRAINT `fk_rating_template` FOREIGN KEY (`template_id`) REFERENCES `quantmate`.`strategy_templates`(`id`) ON DELETE CASCADE,
  CONSTRAINT `chk_rating` CHECK (`rating` BETWEEN 1 AND 5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

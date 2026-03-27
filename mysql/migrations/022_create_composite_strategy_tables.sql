-- Composite Strategy System: strategy_components, composite_strategies, composite_component_bindings
-- Implements the three-layer architecture: Universe → Trading → Risk

-- Strategy components — individual reusable building blocks for composite strategies
CREATE TABLE IF NOT EXISTS strategy_components (
    id              INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    name            VARCHAR(100) NOT NULL,
    layer           ENUM('universe','trading','risk') NOT NULL,
    sub_type        VARCHAR(50)  NOT NULL COMMENT 'Sub-type: factor/technical/trend/grid/mean_revert/stop_loss/position_sizing/var_constraint/...',
    description     TEXT,
    code            MEDIUMTEXT   DEFAULT NULL COMMENT 'Executable Python source (mainly for trading layer)',
    config          JSON         DEFAULT NULL COMMENT 'Declarative config (factor DSL / rule params / filter criteria)',
    parameters      JSON         DEFAULT NULL,
    version         INT          NOT NULL DEFAULT 1,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_sc_user_layer (user_id, layer),
    INDEX idx_sc_sub_type (sub_type),
    CONSTRAINT fk_sc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Composite strategies — composed from multiple strategy components
CREATE TABLE IF NOT EXISTS composite_strategies (
    id                INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id           INT          NOT NULL,
    name              VARCHAR(100) NOT NULL,
    description       TEXT,
    portfolio_config  JSON         DEFAULT NULL COMMENT 'Weight allocation / rebalance config',
    market_constraints JSON        DEFAULT NULL COMMENT 'T+1 / price limit / lot size constraints',
    execution_mode    ENUM('backtest','paper','live') NOT NULL DEFAULT 'backtest',
    is_active         BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_cs_user (user_id),
    CONSTRAINT fk_cs_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Composite-component bindings — many-to-many with ordering and weights
CREATE TABLE IF NOT EXISTS composite_component_bindings (
    id                     INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
    composite_strategy_id  INT            NOT NULL,
    component_id           INT            NOT NULL,
    layer                  ENUM('universe','trading','risk') NOT NULL,
    ordinal                INT            NOT NULL DEFAULT 0   COMMENT 'Order within same layer (priority / chain sequence)',
    weight                 DECIMAL(5,4)   NOT NULL DEFAULT 1.0 COMMENT 'Weight for multi-component voting / merging',
    config_override        JSON           DEFAULT NULL          COMMENT 'Per-binding parameter overrides',
    FOREIGN KEY (composite_strategy_id) REFERENCES composite_strategies(id) ON DELETE CASCADE,
    FOREIGN KEY (component_id) REFERENCES strategy_components(id) ON DELETE RESTRICT,
    INDEX idx_ccb_composite_layer (composite_strategy_id, layer),
    UNIQUE KEY uq_ccb_composite_component (composite_strategy_id, component_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

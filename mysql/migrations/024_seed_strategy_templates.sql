-- Migration 024: Add template_type system + seed 32 strategy templates
-- Adds template_type/layer/sub_type/composite_config columns to strategy_templates
-- and seeds 5 standalone + 23 component + 4 composite templates.

-- ─────────────────────────────────────────────────────────
-- 2.1  ALTER TABLE — add new columns
-- ─────────────────────────────────────────────────────────

ALTER TABLE `quantmate`.`strategy_templates`
  ADD COLUMN template_type ENUM('standalone','component','composite') NOT NULL DEFAULT 'standalone'
    COMMENT 'standalone = VNPy CTA, component = pipeline layer, composite = pipeline blueprint'
    AFTER category,
  ADD COLUMN layer ENUM('universe','trading','risk') DEFAULT NULL
    COMMENT 'Applicable only when template_type = component'
    AFTER template_type,
  ADD COLUMN sub_type VARCHAR(50) DEFAULT NULL
    COMMENT 'Finer subclass label for component templates'
    AFTER layer,
  ADD COLUMN composite_config JSON DEFAULT NULL
    COMMENT 'Composite-only: bindings blueprint referencing sub_type values'
    AFTER sub_type;

ALTER TABLE `quantmate`.`strategy_templates`
  ADD INDEX idx_template_type (template_type),
  ADD INDEX idx_layer (layer);

-- ─────────────────────────────────────────────────────────
-- 2.2  Seed 5 standalone templates (VNPy CtaTemplate)
-- ─────────────────────────────────────────────────────────

INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, 'MACD交叉策略', 'cta', 'standalone', NULL, NULL,
   'MACD histogram flip + zero-line cross entry/exit',
   '-- see strategies/macd_strategy.py',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":12},"slow_period":{"type":"integer","default":26},"signal_period":{"type":"integer","default":9},"fixed_size":{"type":"integer","default":1}}}',
   '{"fast_period":12,"slow_period":26,"signal_period":9,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '三均线趋势策略', 'cta', 'standalone', NULL, NULL,
   'Triple moving-average trend-following strategy',
   '-- see strategies/triple_ma_strategy.py',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":5},"mid_period":{"type":"integer","default":10},"slow_period":{"type":"integer","default":20},"fixed_size":{"type":"integer","default":1}}}',
   '{"fast_period":5,"mid_period":10,"slow_period":20,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '海龟交易策略', 'cta', 'standalone', NULL, NULL,
   'Turtle-trading Donchian breakout with ATR stops',
   '-- see strategies/turtle_trading.py',
   '{"type":"object","properties":{"entry_window":{"type":"integer","default":20},"exit_window":{"type":"integer","default":10},"atr_window":{"type":"integer","default":20},"fixed_size":{"type":"integer","default":1}}}',
   '{"entry_window":20,"exit_window":10,"atr_window":20,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '布林带突破策略', 'cta', 'standalone', NULL, NULL,
   'Bollinger Band breakout with bandwidth confirmation',
   '-- see strategies/bollinger_breakout.py',
   '{"type":"object","properties":{"bb_period":{"type":"integer","default":20},"bb_std":{"type":"number","default":2.0},"bandwidth_threshold":{"type":"number","default":0.04},"fixed_size":{"type":"integer","default":1}}}',
   '{"bb_period":20,"bb_std":2.0,"bandwidth_threshold":0.04,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, 'ATR通道策略', 'cta', 'standalone', NULL, NULL,
   'ATR channel breakout / reversion strategy',
   '-- see strategies/atr_channel.py',
   '{"type":"object","properties":{"ma_period":{"type":"integer","default":20},"atr_period":{"type":"integer","default":14},"atr_multiplier":{"type":"number","default":2.0},"fixed_size":{"type":"integer","default":1}}}',
   '{"ma_period":20,"atr_period":14,"atr_multiplier":2.0,"fixed_size":1}',
   '1.0.0', 'public');

-- ─────────────────────────────────────────────────────────
-- 2.3  Seed 23 component templates
-- ─────────────────────────────────────────────────────────

-- Universe components (6)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '市值过滤', 'cta', 'component', 'universe', 'market_cap_filter',
   'Filter by market cap range',
   '-- see strategies/universe/market_cap_filter.py',
   '{"type":"object","properties":{"min_market_cap":{"type":"number","default":5000000000},"max_market_cap":{"type":"number","default":1000000000000}}}',
   '{"min_market_cap":5000000000,"max_market_cap":1000000000000}',
   '1.0.0', 'public'),

  (1, '流动性过滤', 'cta', 'component', 'universe', 'liquidity_filter',
   'Filter by average volume and turnover rate',
   '-- see strategies/universe/liquidity_filter.py',
   '{"type":"object","properties":{"min_avg_volume":{"type":"number","default":1000000},"min_turnover_rate":{"type":"number","default":0.005}}}',
   '{"min_avg_volume":1000000,"min_turnover_rate":0.005}',
   '1.0.0', 'public'),

  (1, '行业轮动选股', 'cta', 'component', 'universe', 'sector_rotation',
   'Select top-momentum sectors',
   '-- see strategies/universe/sector_rotation.py',
   '{"type":"object","properties":{"top_sectors":{"type":"integer","default":3},"momentum_key":{"type":"string","default":"sector_momentum_20d"}}}',
   '{"top_sectors":3,"momentum_key":"sector_momentum_20d"}',
   '1.0.0', 'public'),

  (1, '指数成分股', 'cta', 'component', 'universe', 'index_constituents',
   'Filter to major index constituents (CSI 300/500)',
   '-- see strategies/universe/index_constituents.py',
   '{"type":"object","properties":{"index":{"type":"string","default":"csi300"}}}',
   '{"index":"csi300"}',
   '1.0.0', 'public'),

  (1, '基本面筛选', 'alpha', 'component', 'universe', 'fundamental_screen',
   'PE/PB/ROE/revenue growth screen',
   '-- see strategies/universe/fundamental_screen.py',
   '{"type":"object","properties":{"max_pe":{"type":"number","default":40},"max_pb":{"type":"number","default":8},"min_roe":{"type":"number","default":0.08},"min_revenue_growth":{"type":"number","default":0}}}',
   '{"max_pe":40,"max_pb":8,"min_roe":0.08,"min_revenue_growth":0}',
   '1.0.0', 'public'),

  (1, 'ST/停牌过滤', 'cta', 'component', 'universe', 'st_halt_filter',
   'Exclude ST, suspended and newly-listed stocks',
   '-- see strategies/universe/st_halt_filter.py',
   '{"type":"object","properties":{"min_list_days":{"type":"integer","default":60}}}',
   '{"min_list_days":60}',
   '1.0.0', 'public');

-- Trading components (11)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '双均线交叉信号', 'cta', 'component', 'trading', 'dual_ma_signal',
   'Fast/slow MA crossover signals',
   '-- see strategies/trading/dual_ma_signal.py',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":5},"slow_period":{"type":"integer","default":20}}}',
   '{"fast_period":5,"slow_period":20}',
   '1.0.0', 'public'),

  (1, '唐奇安突破信号', 'cta', 'component', 'trading', 'donchian_breakout',
   'Donchian channel breakout entry/exit',
   '-- see strategies/trading/donchian_breakout.py',
   '{"type":"object","properties":{"entry_period":{"type":"integer","default":20},"exit_period":{"type":"integer","default":10}}}',
   '{"entry_period":20,"exit_period":10}',
   '1.0.0', 'public'),

  (1, 'MACD信号', 'cta', 'component', 'trading', 'macd_signal',
   'MACD histogram flip + zero-line cross',
   '-- see strategies/trading/macd_signal.py',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":12},"slow_period":{"type":"integer","default":26},"signal_period":{"type":"integer","default":9}}}',
   '{"fast_period":12,"slow_period":26,"signal_period":9}',
   '1.0.0', 'public'),

  (1, '布林带回归信号', 'cta', 'component', 'trading', 'bollinger_reversion',
   'Mean-reversion at Bollinger extremes',
   '-- see strategies/trading/bollinger_reversion.py',
   '{"type":"object","properties":{"bb_period":{"type":"integer","default":20},"bb_std":{"type":"number","default":2.0}}}',
   '{"bb_period":20,"bb_std":2.0}',
   '1.0.0', 'public'),

  (1, '多因子Alpha信号', 'alpha', 'component', 'trading', 'multi_factor_alpha',
   'Value + momentum + quality composite z-score',
   '-- see strategies/trading/multi_factor_alpha.py',
   '{"type":"object","properties":{"weight_value":{"type":"number","default":0.4},"weight_momentum":{"type":"number","default":0.3},"weight_quality":{"type":"number","default":0.3},"top_k":{"type":"integer","default":10},"alpha_threshold":{"type":"number","default":0.5}}}',
   '{"weight_value":0.4,"weight_momentum":0.3,"weight_quality":0.3,"top_k":10,"alpha_threshold":0.5}',
   '1.0.0', 'public'),

  (1, '动量信号', 'alpha', 'component', 'trading', 'momentum_signal',
   'Cross-sectional momentum long/short',
   '-- see strategies/trading/momentum_signal.py',
   '{"type":"object","properties":{"momentum_days":{"type":"integer","default":20},"long_pct":{"type":"number","default":0.1},"short_pct":{"type":"number","default":0.1}}}',
   '{"momentum_days":20,"long_pct":0.1,"short_pct":0.1}',
   '1.0.0', 'public'),

  (1, '均值回归Alpha', 'alpha', 'component', 'trading', 'mean_reversion_alpha',
   'Z-score reversion at extended deviations',
   '-- see strategies/trading/mean_reversion_alpha.py',
   '{"type":"object","properties":{"lookback":{"type":"integer","default":20},"entry_z_threshold":{"type":"number","default":2.0}}}',
   '{"lookback":20,"entry_z_threshold":2.0}',
   '1.0.0', 'public'),

  (1, '固定网格信号', 'grid', 'component', 'trading', 'fixed_grid',
   'Fixed-percentage grid entry levels',
   '-- see strategies/trading/fixed_grid.py',
   '{"type":"object","properties":{"grid_pct":{"type":"number","default":0.02},"max_layers":{"type":"integer","default":5}}}',
   '{"grid_pct":0.02,"max_layers":5}',
   '1.0.0', 'public'),

  (1, '动态网格信号', 'grid', 'component', 'trading', 'dynamic_grid',
   'ATR-adaptive grid spacing',
   '-- see strategies/trading/dynamic_grid.py',
   '{"type":"object","properties":{"atr_multiplier":{"type":"number","default":1.0},"max_layers":{"type":"integer","default":5},"atr_period":{"type":"integer","default":14}}}',
   '{"atr_multiplier":1.0,"max_layers":5,"atr_period":14}',
   '1.0.0', 'public'),

  (1, '配对交易信号', 'arbitrage', 'component', 'trading', 'pair_trading_signal',
   'Co-integrated pair spread-reversion',
   '-- see strategies/trading/pair_trading_signal.py',
   '{"type":"object","properties":{"entry_z":{"type":"number","default":2.0},"exit_z":{"type":"number","default":0.5},"pairs":{"type":"array","items":{"type":"object","properties":{"leg_a":{"type":"string"},"leg_b":{"type":"string"}}}}}}',
   '{"entry_z":2.0,"exit_z":0.5,"pairs":[]}',
   '1.0.0', 'public'),

  (1, 'ETF套利信号', 'arbitrage', 'component', 'trading', 'etf_arbitrage',
   'ETF premium/discount arbitrage',
   '-- see strategies/trading/etf_arbitrage.py',
   '{"type":"object","properties":{"premium_threshold":{"type":"number","default":0.005},"discount_threshold":{"type":"number","default":-0.005}}}',
   '{"premium_threshold":0.005,"discount_threshold":-0.005}',
   '1.0.0', 'public');

-- Risk components (6)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '等权配置', 'cta', 'component', 'risk', 'equal_weight',
   'Equal-weight capital allocation',
   '-- see strategies/risk/equal_weight.py',
   '{"type":"object","properties":{"max_positions":{"type":"integer","default":10}}}',
   '{"max_positions":10}',
   '1.0.0', 'public'),

  (1, '波动率平价', 'alpha', 'component', 'risk', 'volatility_parity',
   'Inverse-volatility position sizing',
   '-- see strategies/risk/volatility_parity.py',
   '{"type":"object","properties":{"max_positions":{"type":"integer","default":10},"target_portfolio_vol":{"type":"number","default":0.15}}}',
   '{"max_positions":10,"target_portfolio_vol":0.15}',
   '1.0.0', 'public'),

  (1, '固定止损', 'cta', 'component', 'risk', 'fixed_stop_loss',
   'Fixed percentage stop-loss with risk-per-trade sizing',
   '-- see strategies/risk/fixed_stop_loss.py',
   '{"type":"object","properties":{"stop_pct":{"type":"number","default":0.05},"risk_per_trade_pct":{"type":"number","default":0.02},"max_positions":{"type":"integer","default":20}}}',
   '{"stop_pct":0.05,"risk_per_trade_pct":0.02,"max_positions":20}',
   '1.0.0', 'public'),

  (1, '追踪止损', 'cta', 'component', 'risk', 'trailing_stop',
   'Trailing stop-loss that ratchets with price',
   '-- see strategies/risk/trailing_stop.py',
   '{"type":"object","properties":{"trail_pct":{"type":"number","default":0.03},"max_positions":{"type":"integer","default":20},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"trail_pct":0.03,"max_positions":20,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public'),

  (1, '回撤控制', 'cta', 'component', 'risk', 'drawdown_control',
   'Throttle new entries when portfolio drawdown exceeds threshold',
   '-- see strategies/risk/drawdown_control.py',
   '{"type":"object","properties":{"max_drawdown":{"type":"number","default":0.15},"reduce_at_drawdown":{"type":"number","default":0.10},"reduce_scale":{"type":"number","default":0.5},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"max_drawdown":0.15,"reduce_at_drawdown":0.10,"reduce_scale":0.5,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public'),

  (1, '持仓限制', 'cta', 'component', 'risk', 'position_limits',
   'Per-symbol and portfolio position limit enforcement',
   '-- see strategies/risk/position_limits.py',
   '{"type":"object","properties":{"max_single_position_pct":{"type":"number","default":0.10},"max_total_positions":{"type":"integer","default":20},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"max_single_position_pct":0.10,"max_total_positions":20,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public');

-- ─────────────────────────────────────────────────────────
-- 2.4  Seed 4 composite templates
-- ─────────────────────────────────────────────────────────

INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, composite_config, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, 'CTA趋势跟踪组合', 'cta', 'composite', NULL, NULL,
   '{"bindings":{"universe":["market_cap_filter","liquidity_filter","st_halt_filter"],"trading":["dual_ma_signal"],"risk":["fixed_stop_loss","drawdown_control"]}}',
   'Classic CTA trend-following composite: cap+liquidity+ST filter → dual MA signals → fixed stop + drawdown control',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, 'Alpha多因子组合', 'alpha', 'composite', NULL, NULL,
   '{"bindings":{"universe":["index_constituents","fundamental_screen","st_halt_filter"],"trading":["multi_factor_alpha"],"risk":["volatility_parity","position_limits"]}}',
   'Multi-factor alpha composite: index+fundamentals → alpha z-score → vol-parity sizing + position caps',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, '网格震荡组合', 'grid', 'composite', NULL, NULL,
   '{"bindings":{"universe":["liquidity_filter","st_halt_filter"],"trading":["dynamic_grid"],"risk":["equal_weight","trailing_stop"]}}',
   'Grid-trading composite: liquidity screen → dynamic ATR grid → equal weight + trailing stop',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, '统计套利组合', 'arbitrage', 'composite', NULL, NULL,
   '{"bindings":{"universe":["liquidity_filter","index_constituents"],"trading":["pair_trading_signal"],"risk":["equal_weight","drawdown_control"]}}',
   'Statistical arbitrage composite: liquidity+index filter → pair spread signals → equal weight + drawdown control',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public');

-- Migration 013: Tushare Extended Data Tables
-- Additional Tushare endpoints: money_flow, stk_limit, margin_detail,
-- block_trade, stock_company, fina_indicator, dividend, income, top10_holders

-- 资金流向
CREATE TABLE IF NOT EXISTS `tushare`.`money_flow` (
  `ts_code`      VARCHAR(20) NOT NULL,
  `trade_date`   DATE NOT NULL,
  `buy_sm_vol`   BIGINT DEFAULT NULL COMMENT '小单买入量',
  `buy_sm_amount` DECIMAL(18,4) DEFAULT NULL,
  `sell_sm_vol`  BIGINT DEFAULT NULL,
  `sell_sm_amount` DECIMAL(18,4) DEFAULT NULL,
  `buy_md_vol`   BIGINT DEFAULT NULL COMMENT '中单买入量',
  `buy_md_amount` DECIMAL(18,4) DEFAULT NULL,
  `sell_md_vol`  BIGINT DEFAULT NULL,
  `sell_md_amount` DECIMAL(18,4) DEFAULT NULL,
  `buy_lg_vol`   BIGINT DEFAULT NULL COMMENT '大单买入量',
  `buy_lg_amount` DECIMAL(18,4) DEFAULT NULL,
  `sell_lg_vol`  BIGINT DEFAULT NULL,
  `sell_lg_amount` DECIMAL(18,4) DEFAULT NULL,
  `buy_elg_vol`  BIGINT DEFAULT NULL COMMENT '特大单买入量',
  `buy_elg_amount` DECIMAL(18,4) DEFAULT NULL,
  `sell_elg_vol` BIGINT DEFAULT NULL,
  `sell_elg_amount` DECIMAL(18,4) DEFAULT NULL,
  `net_mf_vol`   BIGINT DEFAULT NULL COMMENT '净流入量',
  `net_mf_amount` DECIMAL(18,4) DEFAULT NULL COMMENT '净流入额',
  PRIMARY KEY (`ts_code`, `trade_date`),
  INDEX `idx_trade_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 涨跌停统计
CREATE TABLE IF NOT EXISTS `tushare`.`stk_limit` (
  `ts_code`      VARCHAR(20) NOT NULL,
  `trade_date`   DATE NOT NULL,
  `name`         VARCHAR(50) DEFAULT NULL,
  `close`        DECIMAL(10,4) DEFAULT NULL,
  `pct_chg`      DECIMAL(10,4) DEFAULT NULL,
  `amp`          DECIMAL(10,4) DEFAULT NULL,
  `fc_ratio`     DECIMAL(10,4) DEFAULT NULL COMMENT '封成比',
  `fl_ratio`     DECIMAL(10,4) DEFAULT NULL COMMENT '封流比',
  `fd_amount`    DECIMAL(18,4) DEFAULT NULL COMMENT '封单金额',
  `first_time`   VARCHAR(20) DEFAULT NULL COMMENT '首次封板时间',
  `last_time`    VARCHAR(20) DEFAULT NULL COMMENT '最后封板时间',
  `open_times`   INT DEFAULT NULL COMMENT '打开次数',
  `strth`        DECIMAL(10,4) DEFAULT NULL COMMENT '涨跌停强度',
  `limit_type`   VARCHAR(2) DEFAULT NULL COMMENT 'U涨停/D跌停',
  PRIMARY KEY (`ts_code`, `trade_date`),
  INDEX `idx_trade_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 融资融券
CREATE TABLE IF NOT EXISTS `tushare`.`margin_detail` (
  `ts_code`      VARCHAR(20) NOT NULL,
  `trade_date`   DATE NOT NULL,
  `rzye`         DECIMAL(18,4) DEFAULT NULL COMMENT '融资余额',
  `rqye`         DECIMAL(18,4) DEFAULT NULL COMMENT '融券余额',
  `rzmre`        DECIMAL(18,4) DEFAULT NULL COMMENT '融资买入额',
  `rqyl`         DECIMAL(18,4) DEFAULT NULL COMMENT '融券余量',
  `rzche`        DECIMAL(18,4) DEFAULT NULL COMMENT '融资偿还额',
  `rqchl`        DECIMAL(18,4) DEFAULT NULL COMMENT '融券偿还量',
  `rqmcl`        DECIMAL(18,4) DEFAULT NULL COMMENT '融券卖出量',
  `rzrqye`       DECIMAL(18,4) DEFAULT NULL COMMENT '融资融券余额',
  PRIMARY KEY (`ts_code`, `trade_date`),
  INDEX `idx_trade_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 大宗交易
CREATE TABLE IF NOT EXISTS `tushare`.`block_trade` (
  `ts_code`      VARCHAR(20) NOT NULL,
  `trade_date`   DATE NOT NULL,
  `price`        DECIMAL(10,4) DEFAULT NULL,
  `vol`          DECIMAL(18,4) DEFAULT NULL,
  `amount`       DECIMAL(18,4) DEFAULT NULL,
  `buyer`        VARCHAR(100) DEFAULT NULL COMMENT '买方营业部',
  `seller`       VARCHAR(100) DEFAULT NULL COMMENT '卖方营业部',
  INDEX `idx_ts_code_date` (`ts_code`, `trade_date`),
  INDEX `idx_trade_date` (`trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 公司基本面
CREATE TABLE IF NOT EXISTS `tushare`.`stock_company` (
  `ts_code`        VARCHAR(20) NOT NULL PRIMARY KEY,
  `exchange`       VARCHAR(10) DEFAULT NULL,
  `chairman`       VARCHAR(50) DEFAULT NULL,
  `manager`        VARCHAR(50) DEFAULT NULL,
  `secretary`      VARCHAR(50) DEFAULT NULL,
  `reg_capital`    DECIMAL(18,4) DEFAULT NULL COMMENT '注册资本(万)',
  `setup_date`     DATE DEFAULT NULL,
  `province`       VARCHAR(20) DEFAULT NULL,
  `city`           VARCHAR(30) DEFAULT NULL,
  `introduction`   TEXT DEFAULT NULL,
  `website`        VARCHAR(200) DEFAULT NULL,
  `email`          VARCHAR(100) DEFAULT NULL,
  `office`         VARCHAR(200) DEFAULT NULL,
  `employees`      INT DEFAULT NULL,
  `main_business`  TEXT DEFAULT NULL,
  `business_scope` TEXT DEFAULT NULL,
  `updated_at`     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 财务指标
CREATE TABLE IF NOT EXISTS `tushare`.`fina_indicator` (
  `ts_code`        VARCHAR(20) NOT NULL,
  `ann_date`       DATE DEFAULT NULL,
  `end_date`       DATE NOT NULL,
  `eps`            DECIMAL(18,6) DEFAULT NULL COMMENT '每股收益',
  `dt_eps`         DECIMAL(18,6) DEFAULT NULL COMMENT '稀释每股收益',
  `total_revenue_ps` DECIMAL(18,6) DEFAULT NULL COMMENT '每股营业收入',
  `revenue_ps`     DECIMAL(18,6) DEFAULT NULL COMMENT '每股营业收入',
  `capital_rese_ps` DECIMAL(18,6) DEFAULT NULL COMMENT '每股资本公积',
  `surplus_rese_ps` DECIMAL(18,6) DEFAULT NULL COMMENT '每股盈余公积',
  `undist_profit_ps` DECIMAL(18,6) DEFAULT NULL COMMENT '每股未分配利润',
  `extra_item`     DECIMAL(18,4) DEFAULT NULL,
  `profit_dedt`    DECIMAL(18,4) DEFAULT NULL COMMENT '扣非净利润',
  `gross_margin`   DECIMAL(10,6) DEFAULT NULL COMMENT '毛利率',
  `current_ratio`  DECIMAL(10,6) DEFAULT NULL COMMENT '流动比率',
  `quick_ratio`    DECIMAL(10,6) DEFAULT NULL COMMENT '速动比率',
  `cash_ratio`     DECIMAL(10,6) DEFAULT NULL COMMENT '现金比率',
  `ar_turn`        DECIMAL(10,6) DEFAULT NULL COMMENT '应收账款周转率',
  `ca_turn`        DECIMAL(10,6) DEFAULT NULL COMMENT '流动资产周转率',
  `fa_turn`        DECIMAL(10,6) DEFAULT NULL COMMENT '固定资产周转率',
  `assets_turn`    DECIMAL(10,6) DEFAULT NULL COMMENT '总资产周转率',
  `op_income`      DECIMAL(18,4) DEFAULT NULL COMMENT '经营活动净现金流',
  `ebit`           DECIMAL(18,4) DEFAULT NULL,
  `ebitda`         DECIMAL(18,4) DEFAULT NULL,
  `fcff`           DECIMAL(18,4) DEFAULT NULL COMMENT '企业自由现金流量',
  `fcfe`           DECIMAL(18,4) DEFAULT NULL COMMENT '股权自由现金流量',
  `roe`            DECIMAL(10,6) DEFAULT NULL COMMENT '净资产收益率',
  `roe_waa`        DECIMAL(10,6) DEFAULT NULL COMMENT '加权ROE',
  `roe_dt`         DECIMAL(10,6) DEFAULT NULL COMMENT '扣非ROE',
  `roa`            DECIMAL(10,6) DEFAULT NULL COMMENT '总资产报酬率',
  `npta`           DECIMAL(10,6) DEFAULT NULL COMMENT '总资产净利率',
  `debt_to_assets` DECIMAL(10,6) DEFAULT NULL COMMENT '资产负债率',
  `netprofit_yoy`  DECIMAL(10,6) DEFAULT NULL COMMENT '净利润同比增长',
  `or_yoy`         DECIMAL(10,6) DEFAULT NULL COMMENT '营业收入同比增长',
  `roe_yoy`        DECIMAL(10,6) DEFAULT NULL COMMENT 'ROE同比增长',
  PRIMARY KEY (`ts_code`, `end_date`),
  INDEX `idx_end_date` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 分红送股
CREATE TABLE IF NOT EXISTS `tushare`.`dividend` (
  `ts_code`      VARCHAR(20) NOT NULL,
  `end_date`     DATE NOT NULL,
  `ann_date`     DATE DEFAULT NULL,
  `div_proc`     VARCHAR(20) DEFAULT NULL COMMENT '实施进度',
  `stk_div`      DECIMAL(10,6) DEFAULT NULL COMMENT '每股送转',
  `stk_bo_rate`  DECIMAL(10,6) DEFAULT NULL COMMENT '每股送股比例',
  `stk_co_rate`  DECIMAL(10,6) DEFAULT NULL COMMENT '每股转增比例',
  `cash_div`     DECIMAL(10,6) DEFAULT NULL COMMENT '每股分红(税前)',
  `cash_div_tax` DECIMAL(10,6) DEFAULT NULL COMMENT '每股分红(税后)',
  `record_date`  DATE DEFAULT NULL,
  `ex_date`      DATE DEFAULT NULL COMMENT '除权除息日',
  `pay_date`     DATE DEFAULT NULL COMMENT '派息日',
  `div_listdate` DATE DEFAULT NULL COMMENT '红股上市日',
  `imp_ann_date` DATE DEFAULT NULL COMMENT '实施公告日',
  INDEX `idx_ts_code_end` (`ts_code`, `end_date`),
  INDEX `idx_ex_date` (`ex_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 利润表
CREATE TABLE IF NOT EXISTS `tushare`.`income` (
  `ts_code`        VARCHAR(20) NOT NULL,
  `ann_date`       DATE DEFAULT NULL,
  `f_ann_date`     DATE DEFAULT NULL,
  `end_date`       DATE NOT NULL,
  `report_type`    VARCHAR(5) DEFAULT NULL COMMENT '报告类型',
  `comp_type`      VARCHAR(5) DEFAULT NULL COMMENT '公司类型',
  `total_revenue`  DECIMAL(18,4) DEFAULT NULL COMMENT '营业总收入',
  `revenue`        DECIMAL(18,4) DEFAULT NULL COMMENT '营业收入',
  `total_cogs`     DECIMAL(18,4) DEFAULT NULL COMMENT '营业总成本',
  `oper_cost`      DECIMAL(18,4) DEFAULT NULL COMMENT '营业成本',
  `sell_exp`       DECIMAL(18,4) DEFAULT NULL COMMENT '销售费用',
  `admin_exp`      DECIMAL(18,4) DEFAULT NULL COMMENT '管理费用',
  `fin_exp`        DECIMAL(18,4) DEFAULT NULL COMMENT '财务费用',
  `operate_profit` DECIMAL(18,4) DEFAULT NULL COMMENT '营业利润',
  `total_profit`   DECIMAL(18,4) DEFAULT NULL COMMENT '利润总额',
  `income_tax`     DECIMAL(18,4) DEFAULT NULL COMMENT '所得税费用',
  `n_income`       DECIMAL(18,4) DEFAULT NULL COMMENT '净利润',
  `n_income_attr_p` DECIMAL(18,4) DEFAULT NULL COMMENT '归属母公司净利润',
  `minority_gain`  DECIMAL(18,4) DEFAULT NULL COMMENT '少数股东损益',
  `basic_eps`      DECIMAL(10,6) DEFAULT NULL COMMENT '基本每股收益',
  `diluted_eps`    DECIMAL(10,6) DEFAULT NULL COMMENT '稀释每股收益',
  PRIMARY KEY (`ts_code`, `end_date`),
  INDEX `idx_end_date` (`end_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 十大股东
CREATE TABLE IF NOT EXISTS `tushare`.`top10_holders` (
  `ts_code`       VARCHAR(20) NOT NULL,
  `ann_date`      DATE DEFAULT NULL,
  `end_date`      DATE NOT NULL,
  `holder_name`   VARCHAR(200) NOT NULL,
  `hold_amount`   DECIMAL(18,4) DEFAULT NULL COMMENT '持有数量(万股)',
  `hold_ratio`    DECIMAL(10,6) DEFAULT NULL COMMENT '持有比例',
  `hold_float_ratio` DECIMAL(10,6) DEFAULT NULL COMMENT '流通股持有比例',
  `hold_change`   DECIMAL(18,4) DEFAULT NULL COMMENT '变动数量',
  `holder_type`   VARCHAR(20) DEFAULT NULL COMMENT '股东性质',
  INDEX `idx_ts_code_end` (`ts_code`, `end_date`),
  INDEX `idx_end_date` (`end_date`),
  INDEX `idx_holder` (`holder_name`(50))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Register as data_source_items (defaults based on permission level)
INSERT IGNORE INTO `quantmate`.`data_source_items` (`source`, `item_key`, `item_name`, `enabled`, `description`, `requires_permission`) VALUES
  ('tushare', 'money_flow',     '资金流向',   1, '个股资金流向数据(大中小单)', 0),
  ('tushare', 'stk_limit',      '涨跌停统计', 1, '涨跌停数据(封单/强度)',     0),
  ('tushare', 'margin_detail',  '融资融券',   1, '融资融券余额明细',          0),
  ('tushare', 'block_trade',    '大宗交易',   1, '大宗交易数据',              0),
  ('tushare', 'stock_company',  '公司基本面', 1, '上市公司基本信息',          0),
  ('tushare', 'fina_indicator', '财务指标',   1, '主要财务指标数据',          0),
  ('tushare', 'dividend',       '分红送股',   0, '分红送股数据(需高级权限)',    1),
  ('tushare', 'income',         '利润表',     0, '利润表数据(需高级权限)',      1),
  ('tushare', 'top10_holders',  '十大股东',   0, '十大股东数据(需高级权限)',    1);

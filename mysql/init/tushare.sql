-- =============================================================================
-- QuantMate Tushare Database (Merged Init)
-- Database: tushare - Raw Tushare Data Ingestion
-- Includes tables from migrations 008, 013, 014, 016
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tushare CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE tushare;

-- =============================================================================
-- SECTION 1: Stock Metadata
-- =============================================================================

-- Stock basic information (tushare: stock_basic)
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code VARCHAR(32) NOT NULL,
    symbol VARCHAR(16),
    name VARCHAR(255),
    area VARCHAR(64),
    industry VARCHAR(128),
    fullname VARCHAR(255),
    enname VARCHAR(255),
    market VARCHAR(32),
    exchange VARCHAR(16),
    list_status VARCHAR(16),
    list_date DATE,
    delist_date DATE,
    is_hs VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_stock_basic_symbol ON stock_basic(symbol);
CREATE INDEX idx_stock_basic_exchange ON stock_basic(exchange);

-- Company information (migration 013 version - extended fields)
CREATE TABLE IF NOT EXISTS stock_company (
    ts_code        VARCHAR(20) NOT NULL PRIMARY KEY,
    exchange       VARCHAR(10) DEFAULT NULL,
    chairman       VARCHAR(50) DEFAULT NULL,
    manager        VARCHAR(50) DEFAULT NULL,
    secretary      VARCHAR(50) DEFAULT NULL,
    reg_capital    DECIMAL(18,4) DEFAULT NULL COMMENT '注册资本(万)',
    setup_date     DATE DEFAULT NULL,
    province       VARCHAR(20) DEFAULT NULL,
    city           VARCHAR(30) DEFAULT NULL,
    introduction   TEXT DEFAULT NULL,
    website        VARCHAR(200) DEFAULT NULL,
    email          VARCHAR(100) DEFAULT NULL,
    office         VARCHAR(200) DEFAULT NULL,
    employees      INT DEFAULT NULL,
    main_business  TEXT DEFAULT NULL,
    business_scope TEXT DEFAULT NULL,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Trading calendar (tushare: trade_cal)
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange VARCHAR(16) NOT NULL,
    cal_date DATE NOT NULL,
    is_open TINYINT NOT NULL DEFAULT 0,
    pretrade_date DATE,
    PRIMARY KEY (exchange, cal_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- HS (沪深股通) constituents (tushare: hs_const)
CREATE TABLE IF NOT EXISTS hs_const (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    in_date DATE,
    out_date DATE,
    market VARCHAR(32),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hs_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Stock name change history (tushare: namechange)
CREATE TABLE IF NOT EXISTS stock_name_change (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    INDEX idx_namechange_ts (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- New share / IPO list (tushare: new_share)
CREATE TABLE IF NOT EXISTS new_share (
    ts_code VARCHAR(32) NOT NULL,
    name VARCHAR(255),
    ipo_date DATE,
    issue_date DATE,
    issue_price DECIMAL(12,2),
    amount BIGINT,
    market VARCHAR(32),
    PRIMARY KEY (ts_code, ipo_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- SECTION 2: Market / Price / Time-Series Data
-- =============================================================================

-- Daily OHLC data (tushare: daily)
CREATE TABLE IF NOT EXISTS stock_daily (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,2),
    high DECIMAL(16,2),
    low DECIMAL(16,2),
    close DECIMAL(16,2),
    pre_close DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change DECIMAL(10,2),
    vol BIGINT,
    amount DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_daily_ts ON stock_daily(ts_code);
CREATE INDEX idx_daily_date ON stock_daily(trade_date);
CREATE INDEX idx_daily_ts_date ON stock_daily(ts_code, trade_date);

-- Weekly K-line (migration 008)
CREATE TABLE IF NOT EXISTS stock_weekly (
    ts_code       VARCHAR(32)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_weekly_ts ON stock_weekly(ts_code);
CREATE INDEX idx_weekly_date ON stock_weekly(trade_date);

-- Monthly K-line (migration 008)
CREATE TABLE IF NOT EXISTS stock_monthly (
    ts_code       VARCHAR(32)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX idx_monthly_ts ON stock_monthly(ts_code);
CREATE INDEX idx_monthly_date ON stock_monthly(trade_date);

-- Adjustment factor (tushare: adj_factor)
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DECIMAL(24,12),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Daily derived indicators (tushare: daily_basic)
CREATE TABLE IF NOT EXISTS daily_basic (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    turnover_rate DECIMAL(10,2),
    turnover_rate_f DECIMAL(10,2),
    volume_ratio DECIMAL(10,2),
    pe DECIMAL(12,2),
    pe_ttm DECIMAL(12,2),
    pb DECIMAL(12,2),
    ps DECIMAL(12,2),
    ps_ttm DECIMAL(12,2),
    total_mv DECIMAL(20,2),
    circ_mv DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Money flow / capital movement (original tushare table)
CREATE TABLE IF NOT EXISTS stock_moneyflow (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    net_mf DECIMAL(20,2),
    buy_small DECIMAL(20,2),
    sell_small DECIMAL(20,2),
    buy_medium DECIMAL(20,2),
    sell_medium DECIMAL(20,2),
    buy_large DECIMAL(20,2),
    sell_large DECIMAL(20,2),
    buy_huge DECIMAL(20,2),
    sell_huge DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Money flow - extended (migration 013)
CREATE TABLE IF NOT EXISTS money_flow (
    ts_code      VARCHAR(20) NOT NULL,
    trade_date   DATE NOT NULL,
    buy_sm_vol   BIGINT DEFAULT NULL COMMENT '小单买入量',
    buy_sm_amount DECIMAL(18,4) DEFAULT NULL,
    sell_sm_vol  BIGINT DEFAULT NULL,
    sell_sm_amount DECIMAL(18,4) DEFAULT NULL,
    buy_md_vol   BIGINT DEFAULT NULL COMMENT '中单买入量',
    buy_md_amount DECIMAL(18,4) DEFAULT NULL,
    sell_md_vol  BIGINT DEFAULT NULL,
    sell_md_amount DECIMAL(18,4) DEFAULT NULL,
    buy_lg_vol   BIGINT DEFAULT NULL COMMENT '大单买入量',
    buy_lg_amount DECIMAL(18,4) DEFAULT NULL,
    sell_lg_vol  BIGINT DEFAULT NULL,
    sell_lg_amount DECIMAL(18,4) DEFAULT NULL,
    buy_elg_vol  BIGINT DEFAULT NULL COMMENT '特大单买入量',
    buy_elg_amount DECIMAL(18,4) DEFAULT NULL,
    sell_elg_vol BIGINT DEFAULT NULL,
    sell_elg_amount DECIMAL(18,4) DEFAULT NULL,
    net_mf_vol   BIGINT DEFAULT NULL COMMENT '净流入量',
    net_mf_amount DECIMAL(18,4) DEFAULT NULL COMMENT '净流入额',
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Minute-level bar data (migration 014 version - includes period column)
CREATE TABLE IF NOT EXISTS stock_minute (
    ts_code    VARCHAR(20) NOT NULL,
    trade_time DATETIME NOT NULL,
    period     VARCHAR(5) NOT NULL COMMENT '1min/5min/15min/30min/60min',
    open       DECIMAL(10,4) DEFAULT NULL,
    high       DECIMAL(10,4) DEFAULT NULL,
    low        DECIMAL(10,4) DEFAULT NULL,
    close      DECIMAL(10,4) DEFAULT NULL,
    volume     BIGINT DEFAULT NULL,
    amount     DECIMAL(18,4) DEFAULT NULL,
    PRIMARY KEY (ts_code, trade_time, period),
    INDEX idx_trade_time (trade_time),
    INDEX idx_period (period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tick-level (tushare: tick)
CREATE TABLE IF NOT EXISTS stock_tick (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_time DATETIME NOT NULL,
    price DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    volume BIGINT,
    amount DECIMAL(20,2),
    type VARCHAR(16),
    INDEX idx_tick_ts_time (ts_code, trade_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Suspension / resumption (tushare: suspend)
CREATE TABLE IF NOT EXISTS stock_suspend (
    ts_code VARCHAR(32) NOT NULL,
    suspend_date DATE,
    resume_date DATE,
    reason TEXT,
    PRIMARY KEY (ts_code, suspend_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Corporate actions: dividends (original tushare table)
CREATE TABLE IF NOT EXISTS stock_dividend (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    ann_date DATE,
    imp_ann_date DATE,
    record_date DATE,
    ex_date DATE,
    pay_date DATE,
    div_cash DECIMAL(20,2),
    div_stock DECIMAL(20,2),
    bonus_ratio DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX ux_stock_dividend_ts_ann (ts_code, ann_date),
    INDEX idx_div_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dividend - extended (migration 013)
CREATE TABLE IF NOT EXISTS dividend (
    ts_code      VARCHAR(20) NOT NULL,
    end_date     DATE NOT NULL,
    ann_date     DATE DEFAULT NULL,
    div_proc     VARCHAR(20) DEFAULT NULL COMMENT '实施进度',
    stk_div      DECIMAL(10,6) DEFAULT NULL COMMENT '每股送转',
    stk_bo_rate  DECIMAL(10,6) DEFAULT NULL COMMENT '每股送股比例',
    stk_co_rate  DECIMAL(10,6) DEFAULT NULL COMMENT '每股转增比例',
    cash_div     DECIMAL(10,6) DEFAULT NULL COMMENT '每股分红(税前)',
    cash_div_tax DECIMAL(10,6) DEFAULT NULL COMMENT '每股分红(税后)',
    record_date  DATE DEFAULT NULL,
    ex_date      DATE DEFAULT NULL COMMENT '除权除息日',
    pay_date     DATE DEFAULT NULL COMMENT '派息日',
    div_listdate DATE DEFAULT NULL COMMENT '红股上市日',
    imp_ann_date DATE DEFAULT NULL COMMENT '实施公告日',
    INDEX idx_ts_code_end (ts_code, end_date),
    INDEX idx_ex_date (ex_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Top-10 holders (migration 013 version - extended fields)
CREATE TABLE IF NOT EXISTS top10_holders (
    ts_code       VARCHAR(20) NOT NULL,
    ann_date      DATE DEFAULT NULL,
    end_date      DATE NOT NULL,
    holder_name   VARCHAR(200) NOT NULL,
    hold_amount   DECIMAL(18,4) DEFAULT NULL COMMENT '持有数量(万股)',
    hold_ratio    DECIMAL(10,6) DEFAULT NULL COMMENT '持有比例',
    hold_float_ratio DECIMAL(10,6) DEFAULT NULL COMMENT '流通股持有比例',
    hold_change   DECIMAL(18,4) DEFAULT NULL COMMENT '变动数量',
    holder_type   VARCHAR(20) DEFAULT NULL COMMENT '股东性质',
    INDEX idx_ts_code_end (ts_code, end_date),
    INDEX idx_end_date (end_date),
    INDEX idx_holder (holder_name(50))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- SECTION 3: Financial Data
-- =============================================================================

-- Financial statements stored as JSON payloads
CREATE TABLE IF NOT EXISTS financial_statement (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    statement_type VARCHAR(32) NOT NULL,
    ann_date DATE,
    end_date DATE,
    report_date DATE,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fin_ts_end (ts_code, end_date),
    INDEX idx_fin_ts_ann (ts_code, ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Financial indicators / audit / forecasts (generic storage)
CREATE TABLE IF NOT EXISTS financial_meta (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    api_name VARCHAR(64) NOT NULL,
    ann_date DATE,
    end_date DATE,
    data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fm_ts_api (ts_code, api_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Financial indicators - structured (migration 013)
CREATE TABLE IF NOT EXISTS fina_indicator (
    ts_code        VARCHAR(20) NOT NULL,
    ann_date       DATE DEFAULT NULL,
    end_date       DATE NOT NULL,
    eps            DECIMAL(18,6) DEFAULT NULL COMMENT '每股收益',
    dt_eps         DECIMAL(18,6) DEFAULT NULL COMMENT '稀释每股收益',
    total_revenue_ps DECIMAL(18,6) DEFAULT NULL COMMENT '每股营业收入',
    revenue_ps     DECIMAL(18,6) DEFAULT NULL COMMENT '每股营业收入',
    capital_rese_ps DECIMAL(18,6) DEFAULT NULL COMMENT '每股资本公积',
    surplus_rese_ps DECIMAL(18,6) DEFAULT NULL COMMENT '每股盈余公积',
    undist_profit_ps DECIMAL(18,6) DEFAULT NULL COMMENT '每股未分配利润',
    extra_item     DECIMAL(18,4) DEFAULT NULL,
    profit_dedt    DECIMAL(18,4) DEFAULT NULL COMMENT '扣非净利润',
    gross_margin   DECIMAL(10,6) DEFAULT NULL COMMENT '毛利率',
    current_ratio  DECIMAL(10,6) DEFAULT NULL COMMENT '流动比率',
    quick_ratio    DECIMAL(10,6) DEFAULT NULL COMMENT '速动比率',
    cash_ratio     DECIMAL(10,6) DEFAULT NULL COMMENT '现金比率',
    ar_turn        DECIMAL(10,6) DEFAULT NULL COMMENT '应收账款周转率',
    ca_turn        DECIMAL(10,6) DEFAULT NULL COMMENT '流动资产周转率',
    fa_turn        DECIMAL(10,6) DEFAULT NULL COMMENT '固定资产周转率',
    assets_turn    DECIMAL(10,6) DEFAULT NULL COMMENT '总资产周转率',
    op_income      DECIMAL(18,4) DEFAULT NULL COMMENT '经营活动净现金流',
    ebit           DECIMAL(18,4) DEFAULT NULL,
    ebitda         DECIMAL(18,4) DEFAULT NULL,
    fcff           DECIMAL(18,4) DEFAULT NULL COMMENT '企业自由现金流量',
    fcfe           DECIMAL(18,4) DEFAULT NULL COMMENT '股权自由现金流量',
    roe            DECIMAL(10,6) DEFAULT NULL COMMENT '净资产收益率',
    roe_waa        DECIMAL(10,6) DEFAULT NULL COMMENT '加权ROE',
    roe_dt         DECIMAL(10,6) DEFAULT NULL COMMENT '扣非ROE',
    roa            DECIMAL(10,6) DEFAULT NULL COMMENT '总资产报酬率',
    npta           DECIMAL(10,6) DEFAULT NULL COMMENT '总资产净利率',
    debt_to_assets DECIMAL(10,6) DEFAULT NULL COMMENT '资产负债率',
    netprofit_yoy  DECIMAL(10,6) DEFAULT NULL COMMENT '净利润同比增长',
    or_yoy         DECIMAL(10,6) DEFAULT NULL COMMENT '营业收入同比增长',
    roe_yoy        DECIMAL(10,6) DEFAULT NULL COMMENT 'ROE同比增长',
    PRIMARY KEY (ts_code, end_date),
    INDEX idx_end_date (end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Income statement (migration 013)
CREATE TABLE IF NOT EXISTS income (
    ts_code        VARCHAR(20) NOT NULL,
    ann_date       DATE DEFAULT NULL,
    f_ann_date     DATE DEFAULT NULL,
    end_date       DATE NOT NULL,
    report_type    VARCHAR(5) DEFAULT NULL COMMENT '报告类型',
    comp_type      VARCHAR(5) DEFAULT NULL COMMENT '公司类型',
    total_revenue  DECIMAL(18,4) DEFAULT NULL COMMENT '营业总收入',
    revenue        DECIMAL(18,4) DEFAULT NULL COMMENT '营业收入',
    total_cogs     DECIMAL(18,4) DEFAULT NULL COMMENT '营业总成本',
    oper_cost      DECIMAL(18,4) DEFAULT NULL COMMENT '营业成本',
    sell_exp       DECIMAL(18,4) DEFAULT NULL COMMENT '销售费用',
    admin_exp      DECIMAL(18,4) DEFAULT NULL COMMENT '管理费用',
    fin_exp        DECIMAL(18,4) DEFAULT NULL COMMENT '财务费用',
    operate_profit DECIMAL(18,4) DEFAULT NULL COMMENT '营业利润',
    total_profit   DECIMAL(18,4) DEFAULT NULL COMMENT '利润总额',
    income_tax     DECIMAL(18,4) DEFAULT NULL COMMENT '所得税费用',
    n_income       DECIMAL(18,4) DEFAULT NULL COMMENT '净利润',
    n_income_attr_p DECIMAL(18,4) DEFAULT NULL COMMENT '归属母公司净利润',
    minority_gain  DECIMAL(18,4) DEFAULT NULL COMMENT '少数股东损益',
    basic_eps      DECIMAL(10,6) DEFAULT NULL COMMENT '基本每股收益',
    diluted_eps    DECIMAL(10,6) DEFAULT NULL COMMENT '稀释每股收益',
    PRIMARY KEY (ts_code, end_date),
    INDEX idx_end_date (end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- SECTION 4: Market Structure & Events
-- =============================================================================

-- Raw responses capture (for debugging/backfill)
CREATE TABLE IF NOT EXISTS raw_response (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    interface_name VARCHAR(128) NOT NULL,
    params JSON,
    ts_code VARCHAR(32),
    data JSON,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_raw_iface (interface_name),
    INDEX idx_raw_ts (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Limit-up / limit-down list (original)
CREATE TABLE IF NOT EXISTS stock_limit_list (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    limit_type VARCHAR(32),
    limit_reason TEXT,
    INDEX idx_limit_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 涨跌停统计 - structured (migration 013)
CREATE TABLE IF NOT EXISTS stk_limit (
    ts_code      VARCHAR(20) NOT NULL,
    trade_date   DATE NOT NULL,
    name         VARCHAR(50) DEFAULT NULL,
    close        DECIMAL(10,4) DEFAULT NULL,
    pct_chg      DECIMAL(10,4) DEFAULT NULL,
    amp          DECIMAL(10,4) DEFAULT NULL,
    fc_ratio     DECIMAL(10,4) DEFAULT NULL COMMENT '封成比',
    fl_ratio     DECIMAL(10,4) DEFAULT NULL COMMENT '封流比',
    fd_amount    DECIMAL(18,4) DEFAULT NULL COMMENT '封单金额',
    first_time   VARCHAR(20) DEFAULT NULL COMMENT '首次封板时间',
    last_time    VARCHAR(20) DEFAULT NULL COMMENT '最后封板时间',
    open_times   INT DEFAULT NULL COMMENT '打开次数',
    strth        DECIMAL(10,4) DEFAULT NULL COMMENT '涨跌停强度',
    limit_type   VARCHAR(2) DEFAULT NULL COMMENT 'U涨停/D跌停',
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Margin detail - structured (migration 013)
CREATE TABLE IF NOT EXISTS margin_detail (
    ts_code      VARCHAR(20) NOT NULL,
    trade_date   DATE NOT NULL,
    rzye         DECIMAL(18,4) DEFAULT NULL COMMENT '融资余额',
    rqye         DECIMAL(18,4) DEFAULT NULL COMMENT '融券余额',
    rzmre        DECIMAL(18,4) DEFAULT NULL COMMENT '融资买入额',
    rqyl         DECIMAL(18,4) DEFAULT NULL COMMENT '融券余量',
    rzche        DECIMAL(18,4) DEFAULT NULL COMMENT '融资偿还额',
    rqchl        DECIMAL(18,4) DEFAULT NULL COMMENT '融券偿还量',
    rqmcl        DECIMAL(18,4) DEFAULT NULL COMMENT '融券卖出量',
    rzrqye       DECIMAL(18,4) DEFAULT NULL COMMENT '融资融券余额',
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Top institutional holdings
CREATE TABLE IF NOT EXISTS stock_top_list (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    change_amount DECIMAL(20,2),
    change_rate DECIMAL(10,2),
    reason TEXT,
    INDEX idx_toplist_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Top institutional holdings by investor
CREATE TABLE IF NOT EXISTS stock_top_inst (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    end_date DATE,
    inst_name VARCHAR(255),
    hold_amount DECIMAL(20,2),
    hold_ratio DECIMAL(8,2),
    INDEX idx_topinst_ts_end (ts_code, end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Stock incentives / rewards
CREATE TABLE IF NOT EXISTS stock_stk_rewards (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    reward_date DATE,
    holder VARCHAR(255),
    change_amount DECIMAL(20,2),
    change_ratio DECIMAL(8,2),
    note TEXT,
    INDEX idx_rewards_ts_date (ts_code, reward_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Holder changes
CREATE TABLE IF NOT EXISTS holder_changes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    change_date DATE,
    holder_name VARCHAR(255),
    before_amount DECIMAL(20,2),
    after_amount DECIMAL(20,2),
    change_amount DECIMAL(20,2),
    INDEX idx_holderchg_ts_date (ts_code, change_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Pledge statistics
CREATE TABLE IF NOT EXISTS stock_pledge (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    pledge_date DATE,
    pledge_amount DECIMAL(20,2),
    pledge_ratio DECIMAL(8,2),
    pledge_holder VARCHAR(255),
    detail JSON,
    INDEX idx_pledge_ts_date (ts_code, pledge_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Margin financing / securities lending (original)
CREATE TABLE IF NOT EXISTS stock_margin (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    financing_balance DECIMAL(20,2),
    financing_buy DECIMAL(20,2),
    financing_repay DECIMAL(20,2),
    securities_lend_balance DECIMAL(20,2),
    INDEX idx_margin_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_margin_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    detail JSON,
    INDEX idx_margindet_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Block trades (migration 013 version - includes buyer/seller)
CREATE TABLE IF NOT EXISTS block_trade (
    ts_code      VARCHAR(20) NOT NULL,
    trade_date   DATE NOT NULL,
    price        DECIMAL(10,4) DEFAULT NULL,
    vol          DECIMAL(18,4) DEFAULT NULL,
    amount       DECIMAL(18,4) DEFAULT NULL,
    buyer        VARCHAR(100) DEFAULT NULL COMMENT '买方营业部',
    seller       VARCHAR(100) DEFAULT NULL COMMENT '卖方营业部',
    INDEX idx_ts_code_date (ts_code, trade_date),
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Short sell / borrow related data
CREATE TABLE IF NOT EXISTS short_sell (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE,
    short_volume BIGINT,
    short_amount DECIMAL(20,2),
    INDEX idx_short_ts_date (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- SECTION 5: Index Data
-- =============================================================================

-- Index basics and constituents
CREATE TABLE IF NOT EXISTS index_basic (
    index_code VARCHAR(32) NOT NULL PRIMARY KEY,
    name VARCHAR(255),
    market VARCHAR(32),
    publisher VARCHAR(128),
    category VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS index_member (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    index_code VARCHAR(32) NOT NULL,
    ts_code VARCHAR(32) NOT NULL,
    in_date DATE,
    out_date DATE,
    weight DECIMAL(12,8),
    INDEX idx_index_member_code (index_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS index_daily (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,2),
    high DECIMAL(16,2),
    low DECIMAL(16,2),
    close DECIMAL(16,2),
    vol BIGINT,
    amount DECIMAL(20,2),
    PRIMARY KEY (index_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Index weekly K-line (migration 008)
CREATE TABLE IF NOT EXISTS index_weekly (
    index_code    VARCHAR(32)    NOT NULL,
    trade_date    DATE           NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (index_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- SECTION 6: Concept / Classification
-- =============================================================================

CREATE TABLE IF NOT EXISTS stock_concept (
    concept_code VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255),
    description TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS concept_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    concept_code VARCHAR(64) NOT NULL,
    ts_code VARCHAR(32) NOT NULL,
    in_date DATE,
    out_date DATE,
    INDEX idx_concept_ts (concept_code, ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_classification (
    class_code VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255),
    parent_code VARCHAR(64)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Repo / funding market data
CREATE TABLE IF NOT EXISTS repo (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    repo_date DATE,
    instrument VARCHAR(64),
    rate DECIMAL(12,2),
    amount DECIMAL(20,2),
    INDEX idx_repo_date (repo_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- SECTION 7: Multi-Market (HK / US) - Migration 016
-- =============================================================================

-- Hong Kong market (HKEX)
CREATE TABLE IF NOT EXISTS hk_stock_basic (
    ts_code       VARCHAR(20)     NOT NULL    COMMENT 'Tushare code e.g. 00700.HK',
    name          VARCHAR(100)    DEFAULT NULL,
    enname        VARCHAR(200)    DEFAULT NULL,
    industry      VARCHAR(100)    DEFAULT NULL,
    area          VARCHAR(50)     DEFAULT 'HK',
    market        VARCHAR(20)     DEFAULT 'MAIN'  COMMENT 'MAIN, GEM',
    list_date     VARCHAR(8)      DEFAULT NULL,
    delist_date   VARCHAR(8)      DEFAULT NULL,
    list_status   VARCHAR(2)      DEFAULT 'L'     COMMENT 'L=listed, D=delisted',
    curr_type     VARCHAR(5)      DEFAULT 'HKD',
    updated_at    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code),
    KEY idx_hksb_industry (industry),
    KEY idx_hksb_status   (list_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hk_stock_daily (
    ts_code       VARCHAR(20)     NOT NULL,
    trade_date    VARCHAR(8)      NOT NULL,
    open          DECIMAL(12,4)   DEFAULT NULL,
    high          DECIMAL(12,4)   DEFAULT NULL,
    low           DECIMAL(12,4)   DEFAULT NULL,
    close         DECIMAL(12,4)   DEFAULT NULL,
    vol           DECIMAL(18,2)   DEFAULT NULL    COMMENT 'Volume in shares',
    amount        DECIMAL(18,4)   DEFAULT NULL    COMMENT 'Turnover in HKD',
    pct_chg       DECIMAL(10,4)   DEFAULT NULL,
    PRIMARY KEY (ts_code, trade_date),
    KEY idx_hksd_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- US market (NYSE / NASDAQ)
CREATE TABLE IF NOT EXISTS us_stock_basic (
    ts_code       VARCHAR(20)     NOT NULL    COMMENT 'e.g. AAPL, TSLA',
    name          VARCHAR(200)    DEFAULT NULL,
    enname        VARCHAR(200)    DEFAULT NULL,
    industry      VARCHAR(100)    DEFAULT NULL,
    exchange      VARCHAR(20)     DEFAULT NULL    COMMENT 'NYSE, NASDAQ, AMEX',
    area          VARCHAR(50)     DEFAULT 'US',
    market_cap    DECIMAL(18,2)   DEFAULT NULL,
    list_date     VARCHAR(8)      DEFAULT NULL,
    delist_date   VARCHAR(8)      DEFAULT NULL,
    list_status   VARCHAR(2)      DEFAULT 'L',
    curr_type     VARCHAR(5)      DEFAULT 'USD',
    updated_at    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code),
    KEY idx_ussb_exchange (exchange),
    KEY idx_ussb_status   (list_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS us_stock_daily (
    ts_code       VARCHAR(20)     NOT NULL,
    trade_date    VARCHAR(8)      NOT NULL,
    open          DECIMAL(12,4)   DEFAULT NULL,
    high          DECIMAL(12,4)   DEFAULT NULL,
    low           DECIMAL(12,4)   DEFAULT NULL,
    close         DECIMAL(12,4)   DEFAULT NULL,
    vol           DECIMAL(18,2)   DEFAULT NULL,
    amount        DECIMAL(18,4)   DEFAULT NULL,
    pct_chg       DECIMAL(10,4)   DEFAULT NULL,
    PRIMARY KEY (ts_code, trade_date),
    KEY idx_ussd_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- SECTION 8: Audit & Sync
-- =============================================================================

-- Ingestion audit table
CREATE TABLE IF NOT EXISTS ingest_audit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    api_name VARCHAR(128) NOT NULL,
    params JSON,
    status VARCHAR(32),
    fetched_rows INT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    INDEX idx_ingest_api (api_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Daily sync log
CREATE TABLE IF NOT EXISTS sync_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sync_date DATE NOT NULL,
    endpoint VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    rows_synced INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    UNIQUE KEY uq_sync_date_endpoint (sync_date, endpoint),
    INDEX idx_sync_date (sync_date),
    INDEX idx_sync_endpoint (endpoint)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

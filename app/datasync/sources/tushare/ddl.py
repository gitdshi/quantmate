"""DDL definitions for all Tushare tables."""

STOCK_BASIC_DDL = """
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
    PRIMARY KEY (ts_code),
    INDEX idx_stock_basic_symbol (symbol),
    INDEX idx_stock_basic_exchange (exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_COMPANY_DDL = """
CREATE TABLE IF NOT EXISTS stock_company (
    ts_code VARCHAR(20) NOT NULL PRIMARY KEY,
    com_name VARCHAR(255),
    com_id VARCHAR(64),
    exchange VARCHAR(10),
    chairman VARCHAR(50),
    manager VARCHAR(50),
    secretary VARCHAR(50),
    reg_capital DECIMAL(18,4),
    setup_date DATE,
    province VARCHAR(20),
    city VARCHAR(30),
    introduction TEXT,
    website VARCHAR(200),
    email VARCHAR(100),
    office VARCHAR(200),
    employees INT,
    main_business TEXT,
    business_scope TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

NEW_SHARE_DDL = """
CREATE TABLE IF NOT EXISTS new_share (
    ts_code VARCHAR(32) NOT NULL,
    sub_code VARCHAR(32),
    name VARCHAR(255),
    ipo_date DATE,
    issue_date DATE,
    market_amount DECIMAL(18,4),
    issue_price DECIMAL(12,2),
    pe DECIMAL(12,2),
    limit_amount DECIMAL(18,4),
    funds DECIMAL(18,4),
    ballot DECIMAL(18,4),
    amount BIGINT,
    market VARCHAR(32),
    PRIMARY KEY (ts_code, ipo_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

TRADE_CAL_DDL = """
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange VARCHAR(16) NOT NULL DEFAULT 'SSE',
    cal_date DATE NOT NULL,
    is_open TINYINT NOT NULL DEFAULT 0,
    pretrade_date DATE DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, cal_date),
    INDEX idx_trade_cal_date (cal_date),
    INDEX idx_trade_cal_is_open (is_open)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_DAILY_DDL = """
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
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_daily_ts (ts_code),
    INDEX idx_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

BAK_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS bak_daily (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    name VARCHAR(255),
    pct_change DECIMAL(16,4),
    close DECIMAL(16,4),
    change_amount DECIMAL(16,4),
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    pre_close DECIMAL(16,4),
    vol_ratio DECIMAL(16,4),
    turn_over DECIMAL(16,4),
    swing DECIMAL(16,4),
    vol BIGINT,
    amount DECIMAL(20,2),
    selling BIGINT,
    buying BIGINT,
    total_share DECIMAL(20,2),
    float_share DECIMAL(20,2),
    pe DECIMAL(16,4),
    industry VARCHAR(128),
    area VARCHAR(64),
    float_mv DECIMAL(20,2),
    total_mv DECIMAL(20,2),
    avg_price DECIMAL(16,4),
    strength DECIMAL(16,4),
    activity BIGINT,
    avg_turnover DECIMAL(16,4),
    attack DECIMAL(16,4),
    interval_3 DECIMAL(16,4),
    interval_6 DECIMAL(16,4),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_bak_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_MONEYFLOW_DDL = """
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
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_stock_moneyflow_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SUSPEND_D_DDL = """
CREATE TABLE IF NOT EXISTS suspend_d (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    suspend_timing VARCHAR(64),
    suspend_type VARCHAR(32),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_suspend_d_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SUSPEND_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS `suspend` (
    ts_code VARCHAR(32) NOT NULL,
    suspend_date DATE NOT NULL,
    resume_date DATE,
    suspend_reason VARCHAR(255),
    PRIMARY KEY (ts_code, suspend_date),
    INDEX idx_suspend_resume_date (resume_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

ADJ_FACTOR_DDL = """
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DECIMAL(24,12),
    PRIMARY KEY (ts_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

FINA_INDICATOR_DDL = """
CREATE TABLE IF NOT EXISTS fina_indicator (
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE DEFAULT NULL,
    end_date DATE NOT NULL,
    eps DECIMAL(18,6) DEFAULT NULL,
    dt_eps DECIMAL(18,6) DEFAULT NULL,
    total_revenue_ps DECIMAL(18,6) DEFAULT NULL,
    revenue_ps DECIMAL(18,6) DEFAULT NULL,
    capital_rese_ps DECIMAL(18,6) DEFAULT NULL,
    surplus_rese_ps DECIMAL(18,6) DEFAULT NULL,
    undist_profit_ps DECIMAL(18,6) DEFAULT NULL,
    extra_item DECIMAL(18,4) DEFAULT NULL,
    profit_dedt DECIMAL(18,4) DEFAULT NULL,
    gross_margin DECIMAL(20,6) DEFAULT NULL,
    current_ratio DECIMAL(20,6) DEFAULT NULL,
    quick_ratio DECIMAL(20,6) DEFAULT NULL,
    cash_ratio DECIMAL(20,6) DEFAULT NULL,
    ar_turn DECIMAL(20,6) DEFAULT NULL,
    ca_turn DECIMAL(20,6) DEFAULT NULL,
    fa_turn DECIMAL(20,6) DEFAULT NULL,
    assets_turn DECIMAL(20,6) DEFAULT NULL,
    op_income DECIMAL(18,4) DEFAULT NULL,
    ebit DECIMAL(18,4) DEFAULT NULL,
    ebitda DECIMAL(18,4) DEFAULT NULL,
    fcff DECIMAL(18,4) DEFAULT NULL,
    fcfe DECIMAL(18,4) DEFAULT NULL,
    roe DECIMAL(20,6) DEFAULT NULL,
    roe_waa DECIMAL(20,6) DEFAULT NULL,
    roe_dt DECIMAL(20,6) DEFAULT NULL,
    roa DECIMAL(20,6) DEFAULT NULL,
    npta DECIMAL(20,6) DEFAULT NULL,
    debt_to_assets DECIMAL(20,6) DEFAULT NULL,
    netprofit_yoy DECIMAL(20,6) DEFAULT NULL,
    or_yoy DECIMAL(20,6) DEFAULT NULL,
    roe_yoy DECIMAL(20,6) DEFAULT NULL,
    PRIMARY KEY (ts_code, end_date),
    INDEX idx_fina_indicator_end_date (end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

INCOME_DDL = """
CREATE TABLE IF NOT EXISTS income (
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE DEFAULT NULL,
    f_ann_date DATE DEFAULT NULL,
    end_date DATE NOT NULL,
    report_type VARCHAR(5) DEFAULT NULL,
    comp_type VARCHAR(5) DEFAULT NULL,
    total_revenue DECIMAL(18,4) DEFAULT NULL,
    revenue DECIMAL(18,4) DEFAULT NULL,
    total_cogs DECIMAL(18,4) DEFAULT NULL,
    oper_cost DECIMAL(18,4) DEFAULT NULL,
    sell_exp DECIMAL(18,4) DEFAULT NULL,
    admin_exp DECIMAL(18,4) DEFAULT NULL,
    fin_exp DECIMAL(18,4) DEFAULT NULL,
    operate_profit DECIMAL(18,4) DEFAULT NULL,
    total_profit DECIMAL(18,4) DEFAULT NULL,
    income_tax DECIMAL(18,4) DEFAULT NULL,
    n_income DECIMAL(18,4) DEFAULT NULL,
    n_income_attr_p DECIMAL(18,4) DEFAULT NULL,
    minority_gain DECIMAL(18,4) DEFAULT NULL,
    basic_eps DECIMAL(10,6) DEFAULT NULL,
    diluted_eps DECIMAL(10,6) DEFAULT NULL,
    PRIMARY KEY (ts_code, end_date),
    INDEX idx_income_end_date (end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

BALANCESHEET_DDL = """
CREATE TABLE IF NOT EXISTS balancesheet (
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE DEFAULT NULL,
    f_ann_date DATE DEFAULT NULL,
    end_date DATE NOT NULL,
    report_type VARCHAR(5) DEFAULT NULL,
    comp_type VARCHAR(5) DEFAULT NULL,
    data JSON NOT NULL,
    PRIMARY KEY (ts_code, end_date),
    INDEX idx_balancesheet_ann_date (ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CASHFLOW_DDL = """
CREATE TABLE IF NOT EXISTS cashflow (
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE DEFAULT NULL,
    f_ann_date DATE DEFAULT NULL,
    end_date DATE NOT NULL,
    report_type VARCHAR(5) DEFAULT NULL,
    comp_type VARCHAR(5) DEFAULT NULL,
    data JSON NOT NULL,
    PRIMARY KEY (ts_code, end_date),
    INDEX idx_cashflow_ann_date (ann_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_DIVIDEND_DDL = """
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

TOP10_HOLDERS_DDL = """
CREATE TABLE IF NOT EXISTS top10_holders (
    ts_code       VARCHAR(20) NOT NULL,
    ann_date      DATE DEFAULT NULL,
    end_date      DATE NOT NULL,
    holder_name   VARCHAR(200) NOT NULL,
    hold_amount   DECIMAL(18,4) DEFAULT NULL,
    hold_ratio    DECIMAL(10,6) DEFAULT NULL,
    hold_float_ratio DECIMAL(10,6) DEFAULT NULL,
    hold_change   DECIMAL(18,4) DEFAULT NULL,
    holder_type   VARCHAR(20) DEFAULT NULL,
    INDEX idx_ts_code_end (ts_code, end_date),
    INDEX idx_end_date (end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_WEEKLY_DDL = """
CREATE TABLE IF NOT EXISTS stock_weekly (
    ts_code       VARCHAR(32) NOT NULL,
    trade_date    DATE NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_weekly_ts (ts_code),
    INDEX idx_weekly_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

STOCK_MONTHLY_DDL = """
CREATE TABLE IF NOT EXISTS stock_monthly (
    ts_code       VARCHAR(32) NOT NULL,
    trade_date    DATE NOT NULL,
    open          DECIMAL(16,2),
    high          DECIMAL(16,2),
    low           DECIMAL(16,2),
    close         DECIMAL(16,2),
    pre_close     DECIMAL(16,2),
    change_amount DECIMAL(16,2),
    pct_change    DECIMAL(10,2),
    vol           BIGINT,
    amount        DECIMAL(20,2),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_monthly_ts (ts_code),
    INDEX idx_monthly_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

INDEX_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS index_daily (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    close DECIMAL(16,4),
    pre_close DECIMAL(16,4),
    change_amount DECIMAL(16,4),
    pct_change DECIMAL(10,4),
    vol BIGINT,
    amount DECIMAL(20,4),
    PRIMARY KEY (index_code, trade_date),
    INDEX idx_index_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

INDEX_WEEKLY_DDL = """
CREATE TABLE IF NOT EXISTS index_weekly (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    close DECIMAL(16,4),
    pre_close DECIMAL(16,4),
    change_amount DECIMAL(16,4),
    pct_change DECIMAL(10,4),
    vol BIGINT,
    amount DECIMAL(20,4),
    PRIMARY KEY (index_code, trade_date),
    INDEX idx_index_weekly_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

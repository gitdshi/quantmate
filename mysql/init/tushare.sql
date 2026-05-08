-- =============================================================================
-- QuantMate Tushare Database (Merged Init)
-- Database: tushare
--
-- Includes folded tushare-side migration state through 043.
-- Core bootstrap tables are created up front, and post-create fixes for
-- lazily generated tables are retained as guarded ALTER statements so fresh
-- installs and existing environments converge on the same schema.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tushare CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE tushare;

CREATE TABLE IF NOT EXISTS ingest_audit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    api_name VARCHAR(64) NOT NULL,
    params JSON,
    status VARCHAR(32) DEFAULT 'running',
    fetched_rows INT DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    INDEX idx_audit_api (api_name),
    INDEX idx_audit_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    INDEX idx_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS suspend_d (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    suspend_timing VARCHAR(64),
    suspend_type VARCHAR(32),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_suspend_d_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS moneyflow (
        ts_code VARCHAR(20) NOT NULL,
        trade_date DATE NOT NULL,
        buy_sm_vol BIGINT DEFAULT NULL,
        buy_sm_amount DECIMAL(18,4) DEFAULT NULL,
        sell_sm_vol BIGINT DEFAULT NULL,
        sell_sm_amount DECIMAL(18,4) DEFAULT NULL,
        buy_md_vol BIGINT DEFAULT NULL,
        buy_md_amount DECIMAL(18,4) DEFAULT NULL,
        sell_md_vol BIGINT DEFAULT NULL,
        sell_md_amount DECIMAL(18,4) DEFAULT NULL,
        buy_lg_vol BIGINT DEFAULT NULL,
        buy_lg_amount DECIMAL(18,4) DEFAULT NULL,
        sell_lg_vol BIGINT DEFAULT NULL,
        sell_lg_amount DECIMAL(18,4) DEFAULT NULL,
        buy_elg_vol BIGINT DEFAULT NULL,
        buy_elg_amount DECIMAL(18,4) DEFAULT NULL,
        sell_elg_vol BIGINT DEFAULT NULL,
        sell_elg_amount DECIMAL(18,4) DEFAULT NULL,
        net_mf_vol BIGINT DEFAULT NULL,
        net_mf_amount DECIMAL(18,4) DEFAULT NULL,
        PRIMARY KEY (ts_code, trade_date),
        INDEX idx_moneyflow_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stk_limit (
        ts_code VARCHAR(20) NOT NULL,
        trade_date DATE NOT NULL,
        name VARCHAR(50) DEFAULT NULL,
        close DECIMAL(10,4) DEFAULT NULL,
        pct_chg DECIMAL(10,4) DEFAULT NULL,
        amp DECIMAL(10,4) DEFAULT NULL,
        fc_ratio DECIMAL(10,4) DEFAULT NULL,
        fl_ratio DECIMAL(10,4) DEFAULT NULL,
        fd_amount DECIMAL(18,4) DEFAULT NULL,
        first_time VARCHAR(20) DEFAULT NULL,
        last_time VARCHAR(20) DEFAULT NULL,
        open_times INT DEFAULT NULL,
        strth DECIMAL(10,4) DEFAULT NULL,
        limit_type VARCHAR(2) DEFAULT NULL,
        PRIMARY KEY (ts_code, trade_date),
        INDEX idx_stk_limit_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS margin_detail (
        ts_code VARCHAR(20) NOT NULL,
        trade_date DATE NOT NULL,
        rzye DECIMAL(18,4) DEFAULT NULL,
        rqye DECIMAL(18,4) DEFAULT NULL,
        rzmre DECIMAL(18,4) DEFAULT NULL,
        rqyl DECIMAL(18,4) DEFAULT NULL,
        rzche DECIMAL(18,4) DEFAULT NULL,
        rqchl DECIMAL(18,4) DEFAULT NULL,
        rqmcl DECIMAL(18,4) DEFAULT NULL,
        rzrqye DECIMAL(18,4) DEFAULT NULL,
        PRIMARY KEY (ts_code, trade_date),
        INDEX idx_margin_detail_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS block_trade (
        ts_code VARCHAR(20) NOT NULL,
        trade_date DATE NOT NULL,
        price DECIMAL(10,4) DEFAULT NULL,
        vol DECIMAL(18,4) DEFAULT NULL,
        amount DECIMAL(18,4) DEFAULT NULL,
        buyer VARCHAR(100) DEFAULT NULL,
        seller VARCHAR(100) DEFAULT NULL,
        INDEX idx_block_trade_code_date (ts_code, trade_date),
        INDEX idx_block_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
        gross_margin DECIMAL(10,6) DEFAULT NULL,
        current_ratio DECIMAL(10,6) DEFAULT NULL,
        quick_ratio DECIMAL(10,6) DEFAULT NULL,
        cash_ratio DECIMAL(10,6) DEFAULT NULL,
        ar_turn DECIMAL(10,6) DEFAULT NULL,
        ca_turn DECIMAL(10,6) DEFAULT NULL,
        fa_turn DECIMAL(10,6) DEFAULT NULL,
        assets_turn DECIMAL(10,6) DEFAULT NULL,
        op_income DECIMAL(18,4) DEFAULT NULL,
        ebit DECIMAL(18,4) DEFAULT NULL,
        ebitda DECIMAL(18,4) DEFAULT NULL,
        fcff DECIMAL(18,4) DEFAULT NULL,
        fcfe DECIMAL(18,4) DEFAULT NULL,
        roe DECIMAL(10,6) DEFAULT NULL,
        roe_waa DECIMAL(10,6) DEFAULT NULL,
        roe_dt DECIMAL(10,6) DEFAULT NULL,
        roa DECIMAL(10,6) DEFAULT NULL,
        npta DECIMAL(10,6) DEFAULT NULL,
        debt_to_assets DECIMAL(10,6) DEFAULT NULL,
        netprofit_yoy DECIMAL(10,6) DEFAULT NULL,
        or_yoy DECIMAL(10,6) DEFAULT NULL,
        roe_yoy DECIMAL(10,6) DEFAULT NULL,
        PRIMARY KEY (ts_code, end_date),
        INDEX idx_fina_indicator_end_date (end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dividend (
        ts_code VARCHAR(20) NOT NULL,
        end_date DATE NOT NULL,
        ann_date DATE DEFAULT NULL,
        div_proc VARCHAR(20) DEFAULT NULL,
        stk_div DECIMAL(10,6) DEFAULT NULL,
        stk_bo_rate DECIMAL(10,6) DEFAULT NULL,
        stk_co_rate DECIMAL(10,6) DEFAULT NULL,
        cash_div DECIMAL(10,6) DEFAULT NULL,
        cash_div_tax DECIMAL(10,6) DEFAULT NULL,
        record_date DATE DEFAULT NULL,
        ex_date DATE DEFAULT NULL,
        pay_date DATE DEFAULT NULL,
        div_listdate DATE DEFAULT NULL,
        imp_ann_date DATE DEFAULT NULL,
        INDEX idx_dividend_code_end (ts_code, end_date),
        INDEX idx_dividend_ex_date (ex_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS top10_holders (
        ts_code VARCHAR(20) NOT NULL,
        ann_date DATE DEFAULT NULL,
        end_date DATE NOT NULL,
        holder_name VARCHAR(200) NOT NULL,
        hold_amount DECIMAL(18,4) DEFAULT NULL,
        hold_ratio DECIMAL(10,6) DEFAULT NULL,
        hold_float_ratio DECIMAL(10,6) DEFAULT NULL,
        hold_change DECIMAL(18,4) DEFAULT NULL,
        holder_type VARCHAR(20) DEFAULT NULL,
        INDEX idx_top10_holders_code_end (ts_code, end_date),
        INDEX idx_top10_holders_end_date (end_date),
        INDEX idx_top10_holders_holder (holder_name(50))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_minute (
        ts_code VARCHAR(20) NOT NULL,
        trade_time DATETIME NOT NULL,
        period VARCHAR(5) NOT NULL,
        open DECIMAL(10,4) DEFAULT NULL,
        high DECIMAL(10,4) DEFAULT NULL,
        low DECIMAL(10,4) DEFAULT NULL,
        close DECIMAL(10,4) DEFAULT NULL,
        volume BIGINT DEFAULT NULL,
        amount DECIMAL(18,4) DEFAULT NULL,
        PRIMARY KEY (ts_code, trade_time, period),
        INDEX idx_stock_minute_trade_time (trade_time),
        INDEX idx_stock_minute_period (period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hk_stock_basic (
        ts_code VARCHAR(20) NOT NULL,
        name VARCHAR(100) DEFAULT NULL,
        enname VARCHAR(200) DEFAULT NULL,
        industry VARCHAR(100) DEFAULT NULL,
        area VARCHAR(50) DEFAULT 'HK',
        market VARCHAR(20) DEFAULT 'MAIN',
        list_date VARCHAR(8) DEFAULT NULL,
        delist_date VARCHAR(8) DEFAULT NULL,
        list_status VARCHAR(2) DEFAULT 'L',
        curr_type VARCHAR(5) DEFAULT 'HKD',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code),
        KEY idx_hk_stock_basic_industry (industry),
        KEY idx_hk_stock_basic_status (list_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hk_stock_daily (
        ts_code VARCHAR(20) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        open DECIMAL(12,4) DEFAULT NULL,
        high DECIMAL(12,4) DEFAULT NULL,
        low DECIMAL(12,4) DEFAULT NULL,
        close DECIMAL(12,4) DEFAULT NULL,
        vol DECIMAL(18,2) DEFAULT NULL,
        amount DECIMAL(18,4) DEFAULT NULL,
        pct_chg DECIMAL(10,4) DEFAULT NULL,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_hk_stock_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS us_stock_basic (
        ts_code VARCHAR(20) NOT NULL,
        name VARCHAR(200) DEFAULT NULL,
        enname VARCHAR(200) DEFAULT NULL,
        industry VARCHAR(100) DEFAULT NULL,
        exchange VARCHAR(20) DEFAULT NULL,
        area VARCHAR(50) DEFAULT 'US',
        market_cap DECIMAL(18,2) DEFAULT NULL,
        list_date VARCHAR(8) DEFAULT NULL,
        delist_date VARCHAR(8) DEFAULT NULL,
        list_status VARCHAR(2) DEFAULT 'L',
        curr_type VARCHAR(5) DEFAULT 'USD',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code),
        KEY idx_us_stock_basic_exchange (exchange),
        KEY idx_us_stock_basic_status (list_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS us_stock_daily (
        ts_code VARCHAR(20) NOT NULL,
        trade_date VARCHAR(8) NOT NULL,
        open DECIMAL(12,4) DEFAULT NULL,
        high DECIMAL(12,4) DEFAULT NULL,
        low DECIMAL(12,4) DEFAULT NULL,
        close DECIMAL(12,4) DEFAULT NULL,
        vol DECIMAL(18,2) DEFAULT NULL,
        amount DECIMAL(18,4) DEFAULT NULL,
        pct_chg DECIMAL(10,4) DEFAULT NULL,
        PRIMARY KEY (ts_code, trade_date),
        KEY idx_us_stock_daily_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Folded migration 042: normalize widths for lazily created tables.
SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`pledge_detail` MODIFY COLUMN `holder_name` VARCHAR(128) NOT NULL',
        'SELECT ''pledge_detail.holder_name already widened or missing'' AS info')
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'pledge_detail'
        AND column_name = 'holder_name' AND column_type = 'varchar(32)'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`pledge_detail` MODIFY COLUMN `pledgor` VARCHAR(128) NULL',
        'SELECT ''pledge_detail.pledgor already widened or missing'' AS info')
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'pledge_detail'
        AND column_name = 'pledgor' AND column_type = 'varchar(16)'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`forecast` MODIFY COLUMN `change_reason` TEXT NULL',
        'SELECT ''forecast.change_reason already TEXT or missing'' AS info')
    FROM information_schema.columns
    WHERE table_schema = 'tushare' AND table_name = 'forecast'
        AND column_name = 'change_reason' AND column_type NOT IN ('text','mediumtext','longtext')
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Folded migration 043: drop spurious auto-inferred unique keys if they exist.
SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cb_rate` DROP INDEX `ux_cb_rate_ts_code`',
        'SELECT ''cb_rate.ux_cb_rate_ts_code already dropped or missing'' AS info')
    FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cb_rate' AND index_name = 'ux_cb_rate_ts_code'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010801`',
        'SELECT ''cn_pmi index already dropped or missing'' AS info')
    FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010801'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010600`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010600'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_id`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_id'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010402`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010402'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010403`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010403'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_month`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_month'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020601`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020601'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010501`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010501'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010503`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010503'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010401`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010401'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020401`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020401'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_update_by`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_update_by'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020301`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020301'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010502`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010502'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010703`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010703'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010702`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010702'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011600`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011600'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020202`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020202'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011700`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011700'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi020501`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi020501'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011800`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011800'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010603`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010603'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi010802`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi010802'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_create_by`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_create_by'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi030000`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi030000'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE `tushare`.`cn_pmi` DROP INDEX `ux_cn_pmi_pmi011900`',
        'SELECT 1') FROM information_schema.statistics
    WHERE table_schema = 'tushare' AND table_name = 'cn_pmi' AND index_name = 'ux_cn_pmi_pmi011900'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

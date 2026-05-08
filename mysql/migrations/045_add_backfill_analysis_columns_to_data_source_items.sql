SET @has_col_supports_backfill := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'data_source_items' AND column_name = 'supports_backfill'
);
SET @sql_add_supports_backfill := IF(
    @has_col_supports_backfill = 0,
    'ALTER TABLE data_source_items ADD COLUMN supports_backfill TINYINT(1) DEFAULT NULL COMMENT ''Parameter-derived historical backfill support'' AFTER sync_mode',
    'SELECT 1'
);
PREPARE stmt_add_supports_backfill FROM @sql_add_supports_backfill;
EXECUTE stmt_add_supports_backfill;
DEALLOCATE PREPARE stmt_add_supports_backfill;

SET @has_col_backfill_mode := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'data_source_items' AND column_name = 'backfill_mode'
);
SET @sql_add_backfill_mode := IF(
    @has_col_backfill_mode = 0,
    'ALTER TABLE data_source_items ADD COLUMN backfill_mode VARCHAR(20) DEFAULT NULL COMMENT ''Parameter-derived backfill strategy: range/date/code/code_date/other'' AFTER supports_backfill',
    'SELECT 1'
);
PREPARE stmt_add_backfill_mode FROM @sql_add_backfill_mode;
EXECUTE stmt_add_backfill_mode;
DEALLOCATE PREPARE stmt_add_backfill_mode;

SET @has_col_input_params := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'data_source_items' AND column_name = 'input_params'
);
SET @sql_add_input_params := IF(
    @has_col_input_params = 0,
    'ALTER TABLE data_source_items ADD COLUMN input_params TEXT DEFAULT NULL COMMENT ''Raw input parameter names from interface analysis'' AFTER backfill_mode',
    'SELECT 1'
);
PREPARE stmt_add_input_params FROM @sql_add_input_params;
EXECUTE stmt_add_input_params;
DEALLOCATE PREPARE stmt_add_input_params;

SET @has_col_input_param_details := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'data_source_items' AND column_name = 'input_param_details'
);
SET @sql_add_input_param_details := IF(
    @has_col_input_param_details = 0,
    'ALTER TABLE data_source_items ADD COLUMN input_param_details TEXT DEFAULT NULL COMMENT ''Human-readable analyzed input parameter details'' AFTER input_params',
    'SELECT 1'
);
PREPARE stmt_add_input_param_details FROM @sql_add_input_param_details;
EXECUTE stmt_add_input_param_details;
DEALLOCATE PREPARE stmt_add_input_param_details;

SET @has_col_analysis_date_params := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'data_source_items' AND column_name = 'analysis_date_params'
);
SET @sql_add_analysis_date_params := IF(
    @has_col_analysis_date_params = 0,
    'ALTER TABLE data_source_items ADD COLUMN analysis_date_params TEXT DEFAULT NULL COMMENT ''Date-axis params extracted from interface analysis'' AFTER input_param_details',
    'SELECT 1'
);
PREPARE stmt_add_analysis_date_params FROM @sql_add_analysis_date_params;
EXECUTE stmt_add_analysis_date_params;
DEALLOCATE PREPARE stmt_add_analysis_date_params;

SET @has_col_input_params_meta := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'data_source_items' AND column_name = 'input_params_meta'
);
SET @sql_add_input_params_meta := IF(
    @has_col_input_params_meta = 0,
    'ALTER TABLE data_source_items ADD COLUMN input_params_meta JSON DEFAULT NULL COMMENT ''Machine-readable analyzed input parameter metadata'' AFTER analysis_date_params',
    'SELECT 1'
);
PREPARE stmt_add_input_params_meta FROM @sql_add_input_params_meta;
EXECUTE stmt_add_input_params_meta;
DEALLOCATE PREPARE stmt_add_input_params_meta;

CREATE TEMPORARY TABLE IF NOT EXISTS `_tmp_backfill_analysis_seed` (
        `source` VARCHAR(50) NOT NULL,
        `item_key` VARCHAR(100) NOT NULL,
        `supports_backfill` TINYINT(1) DEFAULT NULL,
        `backfill_mode` VARCHAR(20) DEFAULT NULL,
        `input_params` TEXT DEFAULT NULL,
        `input_param_details` TEXT DEFAULT NULL,
        `analysis_date_params` TEXT DEFAULT NULL,
        PRIMARY KEY (`source`, `item_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DELETE FROM `_tmp_backfill_analysis_seed`;

INSERT INTO `_tmp_backfill_analysis_seed` (`source`, `item_key`, `supports_backfill`, `backfill_mode`, `input_params`, `input_param_details`, `analysis_date_params`)
VALUES
    ('akshare', 'fund_etf_daily', 1, 'date', NULL, NULL, NULL),
    ('akshare', 'index_daily', 1, 'date', NULL, NULL, NULL),
    ('akshare', 'stock_zh_index_spot', 0, 'date', NULL, NULL, NULL),
    ('tushare', 'adj_factor', 1, 'range', 'ts_code, trade_date, start_date, end_date, offset, limit', 'ts_code(TS基金代码（支持多只基金输入）)；trade_date(交易日期（格式：yyyymmdd，下同）)；start_date(开始日期)；end_date(结束日期)；offset(开始行数)；limit(最大行数)', 'trade_date, start_date, end_date'),
    ('tushare', 'ann_corpus', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'anns_d', 1, 'range', 'ts_code, ann_date, start_date, end_date', 'ts_code(股票代码)；ann_date(公告日期（yyyymmdd格式，下同）)；start_date(公告开始日期)；end_date(公告结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'bak_basic', 1, 'date', 'trade_date, ts_code', 'trade_date(交易日期)；ts_code(股票代码)', 'trade_date'),
    ('tushare', 'bak_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date, offset, limit', 'ts_code(股票代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)；offset(开始行数)；limit(最大行数)', 'trade_date, start_date, end_date'),
    ('tushare', 'balancesheet', 1, 'range', 'ts_code, ann_date, start_date, end_date, period, report_type, comp_type', 'ts_code(股票代码)；ann_date(公告日期(YYYYMMDD格式，下同))；start_date(公告日开始日期)；end_date(公告日结束日期)；period(报告期(每个季度最后一天的日期，比如20171231表示年报，20170630半年报，20170930三季报))；report_type(报告类型：见下方详细说明)；comp_type(公司类型：1一般工商业 2银行 3保险 4证券)', 'ann_date, start_date, end_date'),
    ('tushare', 'bc_bestotcqt', 1, 'range', 'trade_date, start_date, end_date, ts_code', 'trade_date(报价日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)；ts_code(TS代码)', 'trade_date, start_date, end_date'),
    ('tushare', 'bc_otcqt', 1, 'range', 'trade_date, start_date, end_date, ts_code, bank', 'trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)；ts_code(TS代码)；bank(报价机构)', 'trade_date, start_date, end_date'),
    ('tushare', 'block_trade', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(TS代码（股票代码和日期至少输入一个参数）)；trade_date(交易日期（格式：YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'bo_cinema', 1, 'date', 'date', 'date(日期(格式:YYYYMMDD))', 'date'),
    ('tushare', 'bo_daily', 1, 'date', 'date', 'date(日期 （格式YYYYMMDD）)', 'date'),
    ('tushare', 'bo_monthly', 1, 'date', 'date', 'date(日期（每月1号，格式YYYYMMDD）)', 'date'),
    ('tushare', 'bo_weekly', 1, 'date', 'date', 'date(日期（每周一日期，格式YYYYMMDD）)', 'date'),
    ('tushare', 'bond_blk', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(债券代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'bond_blk_detail', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(债券代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'broker_recommend', 1, 'other', 'month', 'month(月度（YYYYMM）)', NULL),
    ('tushare', 'bse_mapping', 1, 'code', 'o_code, n_code', 'o_code(旧代码)；n_code(新代码)', NULL),
    ('tushare', 'cashflow', 1, 'range', 'ts_code, ann_date, f_ann_date, start_date, end_date, period, report_type, comp_type, is_calc', 'ts_code(股票代码)；ann_date(公告日期（YYYYMMDD格式，下同）)；f_ann_date(实际公告日期)；start_date(公告日开始日期)；end_date(公告日结束日期)；period(报告期(每个季度最后一天的日期，比如20171231表示年报，20170630半年报，20170930三季报))；report_type(报告类型：见下方详细说明)；comp_type(公司类型：1一般工商业 2银行 3保险 4证券)；is_calc(是否计算报表)', 'ann_date, f_ann_date, start_date, end_date'),
    ('tushare', 'cb_basic', 1, 'date', 'ts_code, list_date, exchange', 'ts_code(转债代码)；list_date(上市日期)；exchange(上市交易所)', 'list_date'),
    ('tushare', 'cb_call', 1, 'range', 'ts_code, ann_date, start_date, end_date', 'ts_code(转债代码，支持多值输入)；ann_date(公告日期(YYYYMMDD格式，下同))；start_date(公告开始日期)；end_date(公告结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'cb_daily', 1, 'code', 'ts_code', 'ts_code(转债代码，支持多值输入)', NULL),
    ('tushare', 'cb_factor_pro', 1, 'range', 'ts_code, start_date, end_date, trade_date', 'ts_code(可转债代码)；start_date(开始日期)；end_date(结束日期)；trade_date(交易日期)', 'start_date, end_date, trade_date'),
    ('tushare', 'cb_issue', 1, 'range', 'ts_code, ann_date, start_date, end_date', 'ts_code(TS代码)；ann_date(发行公告日)；start_date(公告开始日期)；end_date(公告结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'cb_price_chg', 1, 'code', 'ts_code', 'ts_code(转债代码，支持多值输入)', NULL),
    ('tushare', 'cb_rate', 1, 'code', 'ts_code', 'ts_code(转债代码，支持多值输入)', NULL),
    ('tushare', 'cb_rating', 1, 'code', 'ts_code', 'ts_code(转债代码，支持多值输入)', NULL),
    ('tushare', 'cb_share', 1, 'range', 'ts_code, ann_date, start_date, end_date', 'ts_code(转债代码，支持多值输入)；ann_date(公告日期（YYYYMMDD格式，下同）)；start_date(公告开始日期)；end_date(公告结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'ccass_hold', 1, 'range', 'ts_code, hk_code, trade_date, start_date, end_date', 'ts_code(股票代码 (e.g. 605009.SH))；hk_code(港交所代码 （e.g. 95009）)；trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'ccass_hold_detail', 1, 'range', 'ts_code, hk_code, trade_date, start_date, end_date', 'ts_code(股票代码 (e.g. 605009.SH))；hk_code(港交所代码 （e.g. 95009）)；trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'cctv_news', 1, 'date', 'date', 'date(日期（输入格式：YYYYMMDD 比如：20181211）)', 'date'),
    ('tushare', 'ci_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(行业代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'ci_index_member', 1, 'code', 'l1_code, l2_code, l3_code, ts_code, is_new', 'l1_code(一级行业代码)；l2_code(二级行业代码)；l3_code(三级行业代码)；ts_code(股票代码)；is_new(是否最新（默认为“Y是”）)', NULL),
    ('tushare', 'cn_cpi', 1, 'other', 'm, start_m, end_m', 'm(月份（YYYYMM，下同），支持多个月份同时输入，逗号分隔)；start_m(开始月份)；end_m(结束月份)', NULL),
    ('tushare', 'cn_gdp', 1, 'other', 'q, start_q, end_q, fields', 'q(季度（2019Q1表示，2019年第一季度）)；start_q(开始季度)；end_q(结束季度)；fields(指定输出字段（e.g. fields=''quarter,gdp,gdp_yoy''）)', NULL),
    ('tushare', 'cn_m', 1, 'other', 'm, start_m, end_m, fields', 'm(月度（202001表示，2020年1月）)；start_m(开始月度)；end_m(结束月度)；fields(指定输出字段（e.g. fields=''month,m0,m1,m2''）)', NULL),
    ('tushare', 'cn_pmi', 1, 'other', 'm, start_m, end_m', 'm(月度（202401表示，2024年1月）)；start_m(开始月度)；end_m(结束月度（e.g. fields=''month,pmi010000,pmi010400''）)', NULL),
    ('tushare', 'cn_ppi', 1, 'other', 'm, start_m, end_m', 'm(月份（YYYYMM，下同），支持多个月份同时输入，逗号分隔)；start_m(开始月份)；end_m(结束月份)', NULL),
    ('tushare', 'concept_corpus', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'cyq_chips', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（YYYYMMDD）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'cyq_perf', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（YYYYMMDD）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'daily_basic', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码（二选一）)；trade_date(交易日期 （二选一）)；start_date(开始日期(YYYYMMDD))；end_date(结束日期(YYYYMMDD))', 'trade_date, start_date, end_date'),
    ('tushare', 'daily_info', 1, 'range', 'trade_date, ts_code, exchange, start_date, end_date, fields', 'trade_date(交易日期（YYYYMMDD格式，下同）)；ts_code(板块代码（请参阅下方列表）)；exchange(股票市场（SH上交所 SZ深交所）)；start_date(开始日期)；end_date(结束日期)；fields(指定提取字段)', 'trade_date, start_date, end_date'),
    ('tushare', 'dc_concept', 1, 'date', 'trade_date, theme_code, name', 'trade_date(交易日期)；theme_code(题材代码(xxxxxx.DC格式))；name(题材名称)', 'trade_date'),
    ('tushare', 'dc_concept_cons', 1, 'date', 'ts_code, trade_date, theme_code', 'ts_code(股票代码)；trade_date(交易日期)；theme_code(题材代码)', 'trade_date'),
    ('tushare', 'dc_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date, idx_type', 'ts_code(板块代码（格式：xxxxx.DC))；trade_date(交易日期(格式：YYYYMMDD下同）)；start_date(开始日期)；end_date(结束日期)；idx_type(板块类型： 概念板块、行业板块、地域板块)', 'trade_date, start_date, end_date'),
    ('tushare', 'dc_hot', 1, 'date', 'trade_date, ts_code, market, hot_type, is_new', 'trade_date(交易日期)；ts_code(TS代码)；market(类型(A股市场、ETF基金、港股市场、美股市场))；hot_type(热点类型(人气榜、飙升榜))；is_new(是否最新（默认Y，如果为N则为盘中和盘后阶段采集，具体时间可参考rank_time字段，状态N每小时更新一次，状态Y更新时间为22：30）)', 'trade_date'),
    ('tushare', 'dc_index', 1, 'range', 'ts_code, name, trade_date, start_date, end_date, idx_type', 'ts_code(指数代码（支持多个代码同时输入，用逗号分隔）)；name(板块名称（例如：人形机器人）)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)；idx_type(板块类型(行业板块、概念板块、地域板块))', 'trade_date, start_date, end_date'),
    ('tushare', 'dc_member', 1, 'range', 'ts_code, con_code, trade_date, start_date, end_date', 'ts_code(板块指数代码)；con_code(成分股票代码)；trade_date(交易日期（YYYYMMDD格式）)；start_date(开始日期（YYYYMMDD格式）)；end_date(结束日期（YYYYMMDD格式）)', 'trade_date, start_date, end_date'),
    ('tushare', 'disclosure_date', 1, 'date', 'ts_code, end_date, pre_date, ann_date, actual_date', 'ts_code(TS股票代码)；end_date(财报周期（每个季度最后一天的日期，比如20181231表示2018年年报，20180630表示中报))；pre_date(计划披露日期)；ann_date(最新披露公告日)；actual_date(实际披露日期)', 'end_date, pre_date, ann_date, actual_date'),
    ('tushare', 'dividend', 1, 'date', 'ts_code, ann_date, record_date, ex_date, imp_ann_date', 'ts_code(TS代码)；ann_date(公告日)；record_date(股权登记日期)；ex_date(除权除息日)；imp_ann_date(实施公告日)', 'ann_date, record_date, ex_date, imp_ann_date'),
    ('tushare', 'eco_cal', 1, 'range', 'date, start_date, end_date, currency, country, event', 'date(日期（YYYYMMDD格式）)；start_date(开始日期)；end_date(结束日期)；currency(货币代码)；country(国家（比如：中国、美国）)；event(事件 （支持模糊匹配： *非农*）)', 'date, start_date, end_date'),
    ('tushare', 'etf_basic', 1, 'date', 'ts_code, index_code, list_date, list_status, exchange, mgr', 'ts_code(ETF代码（带.SZ/.SH后缀的6位数字，如：159526.SZ）)；index_code(跟踪指数代码)；list_date(上市日期（格式：YYYYMMDD）)；list_status(上市状态（L上市 D退市 P待上市）)；exchange(交易所（SH上交所 SZ深交所）)；mgr(管理人（简称，e.g.华夏基金))', 'list_date'),
    ('tushare', 'etf_index', 1, 'date', 'ts_code, pub_date, base_date', 'ts_code(指数代码)；pub_date(发布日期（格式：YYYYMMDD）)；base_date(指数基期（格式：YYYYMMDD）)', 'pub_date, base_date'),
    ('tushare', 'etf_share_size', 1, 'range', 'ts_code, trade_date, start_date, end_date, exchange', 'ts_code(基金代码 （可从ETF基础信息接口提取）)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)；exchange(交易所（SSE上交所 SZSE深交所）)', 'trade_date, start_date, end_date'),
    ('tushare', 'express', 1, 'range', 'ts_code, ann_date, start_date, end_date, period', 'ts_code(股票代码)；ann_date(公告日期)；start_date(公告开始日期)；end_date(公告结束日期)；period(报告期(每个季度最后一天的日期,比如20171231表示年报，20170630半年报，20170930三季报))', 'ann_date, start_date, end_date'),
    ('tushare', 'film_record', 1, 'range', 'ann_date, start_date, end_date', 'ann_date(公布日期 （至少输入一个参数，格式：YYYYMMDD，日期不连续，定期公布）)；start_date(开始日期)；end_date(结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'fina_audit', 1, 'range', 'ts_code, ann_date, start_date, end_date, period', 'ts_code(股票代码)；ann_date(公告日期)；start_date(公告开始日期)；end_date(公告结束日期)；period(报告期(每个季度最后一天的日期,比如20171231表示年报))', 'ann_date, start_date, end_date'),
    ('tushare', 'fina_indicator', 1, 'range', 'ts_code, ann_date, start_date, end_date, period', 'ts_code(TS股票代码,e.g. 600001.SH/000001.SZ)；ann_date(公告日期)；start_date(报告期开始日期)；end_date(报告期结束日期)；period(报告期(每个季度最后一天的日期,比如20171231表示年报))', 'ann_date, start_date, end_date'),
    ('tushare', 'fina_mainbz', 1, 'range', 'ts_code, period, type, start_date, end_date', 'ts_code(股票代码)；period(报告期(每个季度最后一天的日期,比如20171231表示年报))；type(类型：P按产品 D按地区 I按行业（请输入大写字母P或者D）)；start_date(报告期开始日期)；end_date(报告期结束日期)', 'start_date, end_date'),
    ('tushare', 'forecast', 1, 'range', 'ts_code, ann_date, start_date, end_date, period, type', 'ts_code(股票代码(二选一))；ann_date(公告日期 (二选一))；start_date(公告开始日期)；end_date(公告结束日期)；period(报告期(每个季度最后一天的日期，比如20171231表示年报，20170630半年报，20170930三季报))；type(预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减))', 'ann_date, start_date, end_date'),
    ('tushare', 'ft_limit', 1, 'range', 'ts_code, trade_date, start_date, end_date, cont, exchange', 'ts_code(合约代码)；trade_date(交易日期（格式：YYYYMMDD）)；start_date(开始日期)；end_date(结束日期)；cont(合约代码（例如：cont=''CU''))；exchange(交易所代码 （例如：exchange=''DCE''))', 'trade_date, start_date, end_date'),
    ('tushare', 'ft_mins', 1, 'range', 'ts_code, freq, start_date, end_date', 'ts_code(股票代码，e.g.CU2310.SHF)；freq(分钟频度（1min/5min/15min/30min/60min）)；start_date(开始日期 格式：2023-08-25 09:00:00)；end_date(结束时间 格式：2023-08-25 19:00:00)', 'start_date, end_date'),
    ('tushare', 'fund_adj', 1, 'range', 'ts_code, trade_date, start_date, end_date, offset, limit', 'ts_code(TS基金代码（支持多只基金输入）)；trade_date(交易日期（格式：yyyymmdd，下同）)；start_date(开始日期)；end_date(结束日期)；offset(开始行数)；limit(最大行数)', 'trade_date, start_date, end_date'),
    ('tushare', 'fund_basic', 1, 'code', 'ts_code, market, status', 'ts_code(基金代码)；market(交易市场: E场内 O场外（默认E）)；status(存续状态 D摘牌 I发行 L上市中)', NULL),
    ('tushare', 'fund_company', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'fund_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(基金代码)；trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'fund_div', 1, 'date', 'ann_date, ex_date, pay_date, ts_code', 'ann_date(公告日（以下参数四选一）)；ex_date(除息日)；pay_date(派息日)；ts_code(基金代码)', 'ann_date, ex_date, pay_date'),
    ('tushare', 'fund_factor_pro', 1, 'range', 'ts_code, start_date, end_date, trade_date', 'ts_code(基金代码)；start_date(开始日期)；end_date(结束日期)；trade_date(交易日期)', 'start_date, end_date, trade_date'),
    ('tushare', 'fund_manager', 1, 'date', 'ts_code, ann_date, name, offset, limit', 'ts_code(基金代码，支持多只基金，逗号分隔)；ann_date(公告日期，格式：YYYYMMDD)；name(基金经理姓名)；offset(开始行数)；limit(每页行数)', 'ann_date'),
    ('tushare', 'fund_nav', 1, 'range', 'ts_code, nav_date, market, start_date, end_date', 'ts_code(TS基金代码 （二选一）)；nav_date(净值日期 （二选一）)；market(E场内 O场外)；start_date(净值开始日期)；end_date(净值结束日期)', 'nav_date, start_date, end_date'),
    ('tushare', 'fund_portfolio', 1, 'range', 'ts_code, symbol, ann_date, period, start_date, end_date', 'ts_code(基金代码 (ts_code,ann_date,period至少输入一个参数))；symbol(股票代码)；ann_date(公告日期（YYYYMMDD格式）)；period(季度（每个季度最后一天的日期，比如20131231表示2013年年报）)；start_date(报告期开始日期（YYYYMMDD格式）)；end_date(报告期结束日期（YYYYMMDD格式）)', 'ann_date, start_date, end_date'),
    ('tushare', 'fund_sales_ratio', 1, 'other', '年份', '年份(年度)', NULL),
    ('tushare', 'fund_sales_vol', 1, 'other', 'year, quarter, name', 'year(年度)；quarter(季度)；name(机构名称)', NULL),
    ('tushare', 'fund_share', 1, 'range', 'ts_code, trade_date, start_date, end_date, market', 'ts_code(TS基金代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)；market(市场代码（SH上交所 ，SZ深交所）)', 'trade_date, start_date, end_date'),
    ('tushare', 'fut_basic', 1, 'date', 'exchange, fut_type, fut_code, list_date', 'exchange(交易所代码 CFFEX-中金所 DCE-大商所 CZCE-郑商所 SHFE-上期所 INE-上海国际能源交易中心 GFEX-广州期货交易所)；fut_type(合约类型 (1 普通合约 2主力与连续合约 默认取全部))；fut_code(标准合约代码，如白银AG、AP鲜苹果等)；list_date(上市开始日期(格式YYYYMMDD，从某日开始以来所有合约）)', 'list_date'),
    ('tushare', 'fut_daily', 1, 'range', 'trade_date, ts_code, exchange, start_date, end_date', 'trade_date(交易日期(YYYYMMDD格式，下同))；ts_code(合约代码)；exchange(交易所代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'fut_holding', 1, 'range', 'trade_date, symbol, start_date, end_date, exchange', 'trade_date(交易日期 （trade_date/symbol至少输入一个参数）)；symbol(合约或产品代码)；start_date(开始日期(YYYYMMDD格式，下同))；end_date(结束日期)；exchange(交易所代码)', 'trade_date, start_date, end_date'),
    ('tushare', 'fut_mapping', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(合约代码)；trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'fut_settle', 1, 'range', 'trade_date, ts_code, start_date, end_date, exchange', 'trade_date(交易日期 （trade_date/ts_code至少需要输入一个参数）)；ts_code(合约代码)；start_date(开始日期(YYYYMMDD格式，下同))；end_date(结束日期)；exchange(交易所代码)', 'trade_date, start_date, end_date'),
    ('tushare', 'fut_weekly_detail', 1, 'other', 'week, prd, start_week, end_week, exchange, fields', 'week(周期（每年第几周，e.g. 202001 表示2020第1周）)；prd(期货品种（支持多品种输入，逗号分隔）)；start_week(开始周期)；end_week(结束周期)；exchange(交易所（请参考交易所说明）)；fields(提取的字段，e.g. fields=''prd,name,vol'')', NULL),
    ('tushare', 'fut_weekly_monthly', 1, 'range', 'ts_code, trade_date, start_date, end_date, freq, exchange', 'ts_code(TS代码)；trade_date(交易日期)；start_date(开始交易日期)；end_date(结束交易日期)；freq(频率week周，month月)；exchange(交易所)', 'trade_date, start_date, end_date'),
    ('tushare', 'fut_wsr', 1, 'range', 'trade_date, symbol, start_date, end_date, exchange', 'trade_date(交易日期)；symbol(产品代码)；start_date(开始日期(YYYYMMDD格式，下同))；end_date(结束日期)；exchange(交易所代码)', 'trade_date, start_date, end_date'),
    ('tushare', 'fx_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date, exchange', 'ts_code(TS代码)；trade_date(交易日期（GMT，日期是格林尼治时间，比北京时间晚一天）)；start_date(开始日期（GMT）)；end_date(结束日期（GMT）)；exchange(交易商，目前只有FXCM)', 'trade_date, start_date, end_date'),
    ('tushare', 'fx_obasic', 1, 'code', 'exchange, classify, ts_code', 'exchange(交易商)；classify(分类)；ts_code(TS代码)', NULL),
    ('tushare', 'ggt_daily', 1, 'range', 'trade_date, start_date, end_date', 'trade_date(交易日期 （格式YYYYMMDD，下同。支持单日和多日输入）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'ggt_monthly', 1, 'other', 'month, start_month, end_month', 'month(月度（格式YYYYMM，下同，支持多个输入）)；start_month(开始月度)；end_month(结束月度)', NULL),
    ('tushare', 'ggt_top10', 1, 'range', 'ts_code, trade_date, start_date, end_date, market_type', 'ts_code(股票代码（二选一）)；trade_date(交易日期（二选一）)；start_date(开始日期)；end_date(结束日期)；market_type(市场类型 2：港股通（沪） 4：港股通（深）)', 'trade_date, start_date, end_date'),
    ('tushare', 'gz_index', 1, 'range', 'date, start_date, end_date', 'date(日期)；start_date(开始日期)；end_date(结束日期)', 'date, start_date, end_date'),
    ('tushare', 'hibor', 1, 'range', 'date, start_date, end_date', 'date(日期 (日期输入格式：YYYYMMDD，下同))；start_date(开始日期)；end_date(结束日期)', 'date, start_date, end_date'),
    ('tushare', 'hk_adjfactor', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（格式：YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'hk_balancesheet', 1, 'range', 'ts_code, period, ind_name, start_date, end_date', 'ts_code(股票代码)；period(报告期(格式：YYYYMMDD）)；ind_name(指标名（如：应收帐款）)；start_date(报告期开始日期（格式：YYYYMMDD）)；end_date(报告结束始日期（格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'hk_basic', 1, 'code', 'ts_code, list_status', 'ts_code(TS代码)；list_status(上市状态 L上市 D退市 P暂停上市 ，默认L)', NULL),
    ('tushare', 'hk_cashflow', 1, 'range', 'ts_code, period, ind_name, start_date, end_date', 'ts_code(股票代码)；period(报告期(格式：YYYYMMDD）)；ind_name(指标名（如：新增贷款）)；start_date(报告期开始日期（格式：YYYYMMDD）)；end_date(报告结束始日期（格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'hk_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'hk_daily_adj', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码（e.g. 00001.HK）)；trade_date(交易日期（YYYYMMDD）)；start_date(开始日期（YYYYMMDD）)；end_date(结束日期（YYYYMMDD）)', 'trade_date, start_date, end_date'),
    ('tushare', 'hk_fina_indicator', 1, 'range', 'ts_code, period, report_type, start_date, end_date', 'ts_code(股票代码)；period(报告期(格式：YYYYMMDD）)；report_type(报告期类型（Q1一季报Q2半年报Q3三季报Q4年报）)；start_date(报告期开始日期(格式：YYYYMMDD）)；end_date(报告结束日期(格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'hk_hold', 1, 'range', 'code, ts_code, trade_date, start_date, end_date, exchange', 'code(交易所代码)；ts_code(TS股票代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)；exchange(类型：SH沪股通（北向）SZ深股通（北向）HK港股通（南向持股）)', 'trade_date, start_date, end_date'),
    ('tushare', 'hk_income', 1, 'range', 'ts_code, period, ind_name, start_date, end_date', 'ts_code(股票代码)；period(报告期(格式：YYYYMMDD）)；ind_name(指标名（如：营业额）)；start_date(报告期开始日期（格式：YYYYMMDD）)；end_date(报告结束始日期（格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'hk_mins', 1, 'range', 'ts_code, freq, start_date, end_date', 'ts_code(股票代码，e.g.00001.HK)；freq(分钟频度（1min/5min/15min/30min/60min）)；start_date(开始日期 格式：2023-03-13 09:00:00)；end_date(结束时间 格式：2023-03-13 19:00:00)', 'start_date, end_date'),
    ('tushare', 'hk_tradecal', 1, 'range', 'start_date, end_date, is_open', 'start_date(开始日期)；end_date(结束日期)；is_open(是否交易 ''0''休市 ''1''交易)', 'start_date, end_date'),
    ('tushare', 'hm_detail', 1, 'range', 'trade_date, ts_code, hm_name, start_date, end_date', 'trade_date(交易日期(YYYYMMDD))；ts_code(股票代码)；hm_name(游资名称)；start_date(开始日期(YYYYMMDD))；end_date(结束日期(YYYYMMDD))', 'trade_date, start_date, end_date'),
    ('tushare', 'hm_list', 1, 'other', 'name', 'name(游资名称)', NULL),
    ('tushare', 'hsgt_top10', 1, 'range', 'ts_code, trade_date, start_date, end_date, market_type', 'ts_code(股票代码（二选一）)；trade_date(交易日期（二选一）)；start_date(开始日期)；end_date(结束日期)；market_type(市场类型（1：沪市 3：深市）)', 'trade_date, start_date, end_date'),
    ('tushare', 'idx_factor_pro', 1, 'range', 'ts_code, start_date, end_date, trade_date', 'ts_code(指数代码(大盘指数 申万指数 中信指数))；start_date(开始日期)；end_date(结束日期)；trade_date(交易日期)', 'start_date, end_date, trade_date'),
    ('tushare', 'idx_mins', 1, 'range', 'ts_code, freq, start_date, end_date', 'ts_code(指数代码，e.g. 000001.SH)；freq(分钟频度（1min/5min/15min/30min/60min）)；start_date(开始日期 格式：2023-08-25 09:00:00)；end_date(结束时间 格式：2023-08-25 19:00:00)', 'start_date, end_date'),
    ('tushare', 'income', 1, 'range', 'ts_code, ann_date, f_ann_date, start_date, end_date, period, report_type, comp_type', 'ts_code(股票代码)；ann_date(公告日期（YYYYMMDD格式，下同）)；f_ann_date(实际公告日期)；start_date(公告日开始日期)；end_date(公告日结束日期)；period(报告期(每个季度最后一天的日期，比如20171231表示年报，20170630半年报，20170930三季报))；report_type(报告类型，参考文档最下方说明)；comp_type(公司类型（1一般工商业2银行3保险4证券）)', 'ann_date, f_ann_date, start_date, end_date'),
    ('tushare', 'index_basic', 1, 'code', 'ts_code, symbol, name, market, publisher, category', 'ts_code(TS指数代码)；symbol(指数代码，支持多值输入，如000300,000001)；name(指数简称)；market(交易所或服务商(默认SSE))；publisher(发布商)；category(指数类别)', NULL),
    ('tushare', 'index_classify', 1, 'code', 'index_code, level, parent_code, src', 'index_code(指数代码)；level(行业分级（L1/L2/L3）)；parent_code(父级代码（一级为0）)；src(指数来源（SW2014：申万2014年版本，SW2021：申万2021年版本）)', NULL),
    ('tushare', 'index_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(指数代码，来源指数基础信息接口)；trade_date(交易日期 （日期格式：YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'index_dailybasic', 1, 'range', 'trade_date, ts_code, start_date, end_date', 'trade_date(交易日期 （格式：YYYYMMDD，比如20181018，下同）)；ts_code(TS代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'index_global', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(TS指数代码，见下表)；trade_date(交易日期，YYYYMMDD格式，下同)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'index_member_all', 1, 'code', 'l1_code, l2_code, l3_code, ts_code, is_new', 'l1_code(一级行业代码)；l2_code(二级行业代码)；l3_code(三级行业代码)；ts_code(股票代码)；is_new(是否最新（默认为“Y是”）)', NULL),
    ('tushare', 'index_monthly', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(TS代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'index_weekly', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(TS代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'index_weight', 1, 'range', 'index_code, trade_date, start_date, end_date', 'index_code(指数代码，来源指数基础信息接口)；trade_date(交易日期（格式YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'irm_qa_sh', 1, 'range', 'ts_code, trade_date, start_date, end_date, pub_date, pub_date', 'ts_code(股票代码)；trade_date(交易日期（格式YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)；pub_date(发布开始日期(格式：2025-06-03 16:43:03))；pub_date(发布结束日期(格式：2025-06-03 18:43:23))', 'trade_date, start_date, end_date, pub_date, pub_date'),
    ('tushare', 'irm_qa_sz', 1, 'range', 'ts_code, trade_date, start_date, end_date, pub_date, pub_date', 'ts_code(股票代码)；trade_date(交易日期（格式YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)；pub_date(发布开始日期(格式：2025-06-03 16:43:03))；pub_date(发布结束日期(格式：2025-06-03 18:43:23))', 'trade_date, start_date, end_date, pub_date, pub_date'),
    ('tushare', 'kpl_concept_cons', 1, 'date', 'trade_date, ts_code, con_code', 'trade_date(交易日期（YYYYMMDD格式）)；ts_code(题材代码（xxxxxx.KP格式）)；con_code(成分代码（xxxxxx.SH格式）)', 'trade_date'),
    ('tushare', 'kpl_list', 1, 'range', 'ts_code, trade_date, tag, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期)；tag(板单类型（涨停/炸板/跌停/自然涨停/竞价，默认为涨停))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'libor', 1, 'range', 'date, start_date, end_date, curr_type', 'date(日期 (日期输入格式：YYYYMMDD，下同))；start_date(开始日期)；end_date(结束日期)；curr_type(货币代码 (USD美元 EUR欧元 JPY日元 GBP英镑 CHF瑞郎，默认是USD))', 'date, start_date, end_date'),
    ('tushare', 'limit_cpt_list', 1, 'range', 'trade_date, ts_code, start_date, end_date', 'trade_date(交易日期（格式：YYYYMMDD，下同）)；ts_code(板块代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'limit_list_d', 1, 'range', 'trade_date, ts_code, limit_type, exchange, start_date, end_date', 'trade_date(交易日期)；ts_code(股票代码)；limit_type(涨跌停类型（U涨停D跌停Z炸板）)；exchange(交易所（SH上交所SZ深交所BJ北交所）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'limit_list_ths', 1, 'range', 'trade_date, ts_code, limit_type, market, start_date, end_date', 'trade_date(交易日期)；ts_code(股票代码)；limit_type(涨停池、连扳池、冲刺涨停、炸板池、跌停池，默认：涨停池)；market(HS-沪深主板 GEM-创业板 STAR-科创板)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'limit_step', 1, 'range', 'trade_date, ts_code, start_date, end_date, nums', 'trade_date(交易日期（格式：YYYYMMDD，下同）)；ts_code(股票代码)；start_date(开始日期)；end_date(结束日期)；nums(连板次数，支持多个输入，例如nums=''2,3'')', 'trade_date, start_date, end_date'),
    ('tushare', 'major_news', 1, 'range', 'src, start_date, end_date', 'src(新闻来源（新华网、凤凰财经、同花顺、新浪财经、华尔街见闻、中证网、财新网、第一财经、财联社）)；start_date(新闻发布开始时间，e.g. 2018-11-21 00:00:00)；end_date(新闻发布结束时间，e.g. 2018-11-22 00:00:00)', 'start_date, end_date'),
    ('tushare', 'margin', 1, 'range', 'trade_date, start_date, end_date, exchange_id', 'trade_date(交易日期（格式：YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)；exchange_id(交易所代码（SSE上交所SZSE深交所BSE北交所）)', 'trade_date, start_date, end_date'),
    ('tushare', 'margin_detail', 1, 'range', 'trade_date, ts_code, start_date, end_date', 'trade_date(交易日期（格式：YYYYMMDD，下同）)；ts_code(TS代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'margin_secs', 1, 'range', 'ts_code, trade_date, exchange, start_date, end_date', 'ts_code(标的代码)；trade_date(交易日)；exchange(交易所（SSE上交所 SZSE深交所 BSE北交所）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow_cnt_ths', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(代码)；trade_date(交易日期(格式：YYYYMMDD，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow_dc', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow_hsgt', 1, 'range', 'trade_date, start_date, end_date', 'trade_date(交易日期 (二选一))；start_date(开始日期 (二选一))；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow_ind_dc', 1, 'range', 'ts_code, trade_date, start_date, end_date, content_type', 'ts_code(代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)；content_type(资金类型(行业、概念、地域))', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow_ind_ths', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(代码)；trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow_mkt_dc', 1, 'range', 'trade_date, start_date, end_date', 'trade_date(交易日期(YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'moneyflow_ths', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'namechange', 1, 'range', 'ts_code, start_date, end_date', 'ts_code(TS代码)；start_date(公告开始日期)；end_date(公告结束日期)', 'start_date, end_date'),
    ('tushare', 'new_share', 1, 'range', 'start_date, end_date', 'start_date(上网发行开始日期)；end_date(上网发行结束日期)', 'start_date, end_date'),
    ('tushare', 'news', 1, 'range', 'start_date, end_date, src', 'start_date(开始日期(格式：2018-11-20 09:00:00）)；end_date(结束日期)；src(新闻来源 见下表)', 'start_date, end_date'),
    ('tushare', 'npr', 1, 'range', 'org, start_date, end_date, ptype', 'org(发布机构)；start_date(发布开始时间)；end_date(发布结束时间)；ptype(类型)', 'start_date, end_date'),
    ('tushare', 'opt_basic', 1, 'date', 'ts_code, exchange, list_date, opt_code, call_put', 'ts_code(TS期权代码)；exchange(交易所代码 （包括上交所SSE等交易所）)；list_date(上市交易日)；opt_code(标准合约代码，OP+期货合约TS_CODE，如棕榈油2207合约，输入OPP2207.DCE)；call_put(期权类型)', 'list_date'),
    ('tushare', 'opt_mins', 1, 'range', 'ts_code, freq, start_date, end_date', 'ts_code(股票代码，e.g：10007976.SH)；freq(分钟频度（1min/5min/15min/30min/60min）)；start_date(开始日期 格式：2024-08-25 09:00:00)；end_date(结束时间 格式：2024-08-25 19:00:00)', 'start_date, end_date'),
    ('tushare', 'pledge_detail', 1, 'code', 'ts_code', 'ts_code(股票代码)', NULL),
    ('tushare', 'pledge_stat', 1, 'date', 'ts_code, end_date', 'ts_code(股票代码)；end_date(截止日期)', 'end_date'),
    ('tushare', 'realtime_list', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'realtime_quote', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'realtime_tick', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'repo_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(TS代码)；trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'report_corpus', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'report_rc', 1, 'range', 'ts_code, report_date, start_date, end_date', 'ts_code(股票代码)；report_date(报告日期)；start_date(报告开始日期)；end_date(报告结束日期)', 'report_date, start_date, end_date'),
    ('tushare', 'repurchase', 1, 'range', 'ann_date, start_date, end_date', 'ann_date(公告日期（任意填参数，如果都不填，单次默认返回2000条）)；start_date(公告开始日期)；end_date(公告结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'research_report', 1, 'range', 'trade_date, start_date, end_date, report_type, ts_code, inst_csname, ind_name', 'trade_date(研报日期（格式：YYYYMMDD，下同）)；start_date(研报开始日期)；end_date(研报结束日期)；report_type(研报类别：个股研报/行业研报)；ts_code(股票代码)；inst_csname(券商名称)；ind_name(行业名称)', 'trade_date, start_date, end_date'),
    ('tushare', 'rt_etf_k', 1, 'code', 'ts_code, topic', 'ts_code(支持通配符方式，e.g. 5*.SH、15*.SZ、159101.SZ)；topic(分类参数，取上海ETF时，需要输入''HQ_FND_TICK''，参考下面例子)', NULL),
    ('tushare', 'rt_etf_sz_iopv', 1, 'code', 'ts_code', 'ts_code(ETF代码（默认为空，即一次全市场。支持单个和多个ETF过滤提取）)', NULL),
    ('tushare', 'rt_fut_min', 1, 'code', 'ts_code, freq', 'ts_code(股票代码，e.g.CU2310.SHF，支持多个合约（逗号分隔）)；freq(分钟频度（1MIN/5MIN/15MIN/30MIN/60MIN）)', NULL),
    ('tushare', 'rt_hk_k', 1, 'code', 'ts_code', 'ts_code(支持通配符方式，e.g. 00001.HK、02*.HK)', NULL),
    ('tushare', 'rt_idx_k', 1, 'code', 'ts_code', 'ts_code(指数代码，支持通配符方式，e.g. 0*.SH、3*.SZ、000001.SH)', NULL),
    ('tushare', 'rt_idx_min', 1, 'code', 'freq, ts_code', 'freq(1MIN,5MIN,15MIN,30MIN,60MIN （大写）)；ts_code(支持单个和多个：000001.SH 或者 000001.SH,399300.SZ)', NULL),
    ('tushare', 'rt_k', 1, 'code', 'ts_code', 'ts_code(支持通配符方式，e.g. 所有上交所股票：6*.SH、所有创业板股票3*.SZ、所有科创板股票688*.SH，或单个股票600000.SH)', NULL),
    ('tushare', 'rt_min', 1, 'code', 'freq, ts_code', 'freq(1MIN,5MIN,15MIN,30MIN,60MIN （大写）)；ts_code(支持单个和多个：600000.SH 或者 600000.SH,000001.SZ)', NULL),
    ('tushare', 'rt_min_daily', 1, 'code', 'freq, ts_code', 'freq(频度：1MIN,5MIN,15MIN,30MIN,60MIN)；ts_code(股票代码，如：600000.SH)', NULL),
    ('tushare', 'rt_sw_k', 1, 'code', 'ts_code', 'ts_code(指数代码，如: 801005.SI；可以是逗号隔开的多个，如: 801005.SI,801001.SI)', NULL),
    ('tushare', 'sf_month', 1, 'other', 'm, start_m, end_m', 'm(月份（YYYYMM，下同），支持多个月份同时输入，逗号分隔)；start_m(开始月份)；end_m(结束月份)', NULL),
    ('tushare', 'sge_basic', 1, 'code', 'ts_code', 'ts_code(合约代码 （支持多个，逗号分隔，不输入为获取全部）)', NULL),
    ('tushare', 'sge_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(合约代码，可通过基础信息获得)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'share_float', 1, 'range', 'ts_code, ann_date, float_date, start_date, end_date', 'ts_code(TS股票代码)；ann_date(公告日期（日期格式：YYYYMMDD，下同）)；float_date(解禁日期)；start_date(解禁开始日期)；end_date(解禁结束日期)', 'ann_date, float_date, start_date, end_date'),
    ('tushare', 'shibor', 1, 'range', 'date, start_date, end_date, bank', 'date(日期 (日期输入格式：YYYYMMDD，下同))；start_date(开始日期)；end_date(结束日期)；bank(银行名称 （中文名称，例如 农业银行）)', 'date, start_date, end_date'),
    ('tushare', 'shibor_lpr', 1, 'range', 'date, start_date, end_date', 'date(日期 (日期输入格式：YYYYMMDD，下同))；start_date(开始日期)；end_date(结束日期)', 'date, start_date, end_date'),
    ('tushare', 'shibor_quote', 1, 'range', 'date, start_date, end_date, bank', 'date(日期 (日期输入格式：YYYYMMDD，下同))；start_date(开始日期)；end_date(结束日期)；bank(银行名称 （中文名称，例如 农业银行）)', 'date, start_date, end_date'),
    ('tushare', 'slb_len', 1, 'range', 'trade_date, start_date, end_date', 'trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'slb_len_mm', 1, 'range', 'trade_date, ts_code, start_date, end_date', 'trade_date(交易日期（YYYYMMDD格式，下同）)；ts_code(股票代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'slb_sec', 1, 'range', 'trade_date, ts_code, start_date, end_date', 'trade_date(交易日期（YYYYMMDD格式，下同）)；ts_code(股票代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'slb_sec_detail', 1, 'range', 'trade_date, ts_code, start_date, end_date', 'trade_date(交易日期（YYYYMMDD格式，下同）)；ts_code(股票代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'st', 1, 'date', 'ts_code, pub_date, imp_date', 'ts_code(股票代码)；pub_date(发布日期)；imp_date(实施日期)', 'pub_date, imp_date'),
    ('tushare', 'stk_account', 1, 'range', 'date, start_date, end_date', 'date(日期)；start_date(开始日期)；end_date(结束日期)', 'date, start_date, end_date'),
    ('tushare', 'stk_account_old', 1, 'range', 'start_date, end_date', 'start_date(开始日期)；end_date(结束日期)', 'start_date, end_date'),
    ('tushare', 'stk_ah_comparison', 1, 'range', 'hk_code, ts_code, trade_date, start_date, end_date', 'hk_code(港股股票代码（xxxxx.HK))；ts_code(A股票代码(xxxxxx.SH/SZ/BJ))；trade_date(交易日期（格式：YYYYMMDD下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_alert', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码（可以通过stock_basic获取）示例:000001.SZ)；trade_date(交易所重点提示起始日期（YYYYMMDD格式）示例:20260312)；start_date(开始日期（YYYYMMDD格式）示例:20260312)；end_date(结束日期（YYYYMMDD格式）示例:20260312)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_auction', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期(YYYYMMDD))；start_date(开始日期(YYYYMMDD))；end_date(结束日期(YYYYMMDD))', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_auction_c', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期(YYYYMMDD))；start_date(开始日期(YYYYMMDD))；end_date(结束日期(YYYYMMDD))', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_auction_o', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期(YYYYMMDD))；start_date(开始日期(YYYYMMDD))；end_date(结束日期(YYYYMMDD))', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_factor_pro', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期(格式：yyyymmdd，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_high_shock', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码（可以通过stock_basic获取）示例:000001.SZ)；trade_date(交易日期（YYYYMMDD格式）示例:20260312)；start_date(开始日期（YYYYMMDD格式）示例:20260312)；end_date(结束日期（YYYYMMDD格式）示例:20260312)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_holdernumber', 1, 'range', 'ts_code, ann_date, enddate, start_date, end_date', 'ts_code(TS股票代码)；ann_date(公告日期)；enddate(截止日期)；start_date(公告开始日期)；end_date(公告结束日期)', 'ann_date, enddate, start_date, end_date'),
    ('tushare', 'stk_holdertrade', 1, 'range', 'ts_code, ann_date, start_date, end_date, trade_type, holder_type', 'ts_code(TS股票代码)；ann_date(公告日期)；start_date(公告开始日期)；end_date(公告结束日期)；trade_type(交易类型IN增持DE减持)；holder_type(股东类型C公司P个人G高管)', 'ann_date, start_date, end_date'),
    ('tushare', 'stk_limit', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_managers', 1, 'range', 'ts_code, ann_date, start_date, end_date', 'ts_code(股票代码，支持单个或多个股票输入)；ann_date(公告日期（YYYYMMDD格式，下同）)；start_date(公告开始日期)；end_date(公告结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'stk_mins', 1, 'range', 'ts_code, freq, start_date, end_date', 'ts_code(股票代码，e.g. 600000.SH)；freq(分钟频度（1min/5min/15min/30min/60min）)；start_date(开始日期 格式：2023-08-25 09:00:00)；end_date(结束时间 格式：2023-08-25 19:00:00)', 'start_date, end_date'),
    ('tushare', 'stk_nineturn', 1, 'range', 'ts_code, trade_date, freq, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期 （格式：YYYY-MM-DD HH:MM:SS))；freq(频率(日daily))；start_date(开始时间)；end_date(结束时间)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_premarket', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期(YYYYMMDD格式，下同))；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_rewards', 1, 'code', 'ts_code, end_date', 'ts_code(TS股票代码，支持单个或多个代码输入)；end_date(报告期)', 'end_date'),
    ('tushare', 'stk_shock', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码（可以通过stock_basic获取）示例:000001.SZ)；trade_date(交易日期（YYYYMMDD格式）示例:20260312)；start_date(开始日期（YYYYMMDD格式）示例:20260312)；end_date(结束日期（YYYYMMDD格式）示例:20260312)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_surv', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(调研日期)；start_date(调研开始日期)；end_date(调研结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_week_month_adj', 1, 'range', 'ts_code, trade_date, start_date, end_date, freq', 'ts_code(TS代码)；trade_date(交易日期（格式：YYYYMMDD，每周或每月最后一天的日期）)；start_date(开始交易日期)；end_date(结束交易日期)；freq(频率week周，month月)', 'trade_date, start_date, end_date'),
    ('tushare', 'stk_weekly_monthly', 1, 'range', 'ts_code, trade_date, start_date, end_date, freq', 'ts_code(TS代码)；trade_date(交易日期(格式：YYYYMMDD，每周或每月最后一天的日期）)；start_date(开始交易日期)；end_date(结束交易日期)；freq(频率week周，month月)', 'trade_date, start_date, end_date'),
    ('tushare', 'stock_basic', 1, 'code', 'ts_code, name, market, list_status, exchange, is_hs', 'ts_code(TS股票代码(格式说明))；name(名称)；market(市场类别 （主板/创业板/科创板/CDR/北交所）)；list_status(上市状态 L上市 D退市 P暂停上市 G 未交易，默认是L)；exchange(交易所 SSE上交所 SZSE深交所 BSE北交所)；is_hs(是否沪深港通标的，N否 H沪股通 S深股通)', NULL),
    ('tushare', 'stock_company', 1, 'code', 'ts_code, exchange', 'ts_code(股票代码)；exchange(交易所代码 ，SSE上交所 SZSE深交所 BSE北交所)', NULL),
    ('tushare', 'stock_corpus', 1, 'other', NULL, NULL, NULL),
    ('tushare', 'stock_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码（支持多个股票同时提取，逗号分隔）)；trade_date(交易日期（YYYYMMDD）)；start_date(开始日期(YYYYMMDD))；end_date(结束日期(YYYYMMDD))', 'trade_date, start_date, end_date'),
    ('tushare', 'stock_hsgt', 1, 'range', 'ts_code, trade_date, type, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（格式：YYYYMMDD）)；type(类型（参考下表）)；start_date(开始时间)；end_date(结束时间)', 'trade_date, start_date, end_date'),
    ('tushare', 'stock_monthly', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(TS代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'stock_st', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（格式：YYYYMMDD下同）)；start_date(开始时间)；end_date(结束时间)', 'trade_date, start_date, end_date'),
    ('tushare', 'stock_weekly', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(TS代码 （ts_code,trade_date两个参数任选一）)；trade_date(交易日期 （每周最后一个交易日期，YYYYMMDD格式）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'suspend', 1, 'range', 'ts_code, trade_date, start_date, end_date, suspend_type', 'ts_code(股票代码(可输入多值))；trade_date(交易日日期)；start_date(停复牌查询开始日期)；end_date(停复牌查询结束日期)；suspend_type(停复牌类型：S-停牌,R-复牌)', 'trade_date, start_date, end_date'),
    ('tushare', 'suspend_d', 1, 'range', 'ts_code, trade_date, start_date, end_date, suspend_type', 'ts_code(股票代码(可输入多值))；trade_date(交易日日期)；start_date(停复牌查询开始日期)；end_date(停复牌查询结束日期)；suspend_type(停复牌类型：S-停牌,R-复牌)', 'trade_date, start_date, end_date'),
    ('tushare', 'sw_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(行业代码)；trade_date(交易日期)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'sz_daily_info', 1, 'range', 'trade_date, ts_code, start_date, end_date', 'trade_date(交易日期（YYYYMMDD格式，下同）)；ts_code(板块代码)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'tdx_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(板块代码：xxxxxx.TDX)；trade_date(交易日期，格式YYYYMMDD,下同)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'tdx_index', 1, 'date', 'ts_code, trade_date, idx_type', 'ts_code(板块代码：xxxxxx.TDX)；trade_date(交易日期(格式：YYYYMMDD）)；idx_type(板块类型：概念板块、行业板块、风格板块、地区板块)', 'trade_date'),
    ('tushare', 'tdx_member', 1, 'range', 'ts_code, con_code, trade_date, start_date, end_date', 'ts_code(板块代码：xxxxxx.TDX)；con_code(成分股票代码)；trade_date(交易日期：（YYYYMMDD格式）)；start_date(开始日期：（YYYYMMDD格式）)；end_date(结束日期：（YYYYMMDD格式）)', 'trade_date, start_date, end_date'),
    ('tushare', 'teleplay_record', 1, 'range', 'report_date, start_date, end_date, org, name', 'report_date(备案月份（YYYYMM）)；start_date(备案开始月份（YYYYMM）)；end_date(备案结束月份（YYYYMM）)；org(备案机构)；name(电视剧名称)', 'report_date, start_date, end_date'),
    ('tushare', 'ths_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(指数代码)；trade_date(交易日期（YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'ths_hot', 1, 'date', 'trade_date, ts_code, market, is_new', 'trade_date(交易日期)；ts_code(TS代码)；market(热榜类型(热股、ETF、可转债、行业板块、概念板块、期货、港股、热基、美股))；is_new(是否最新（默认Y，如果为N则为盘中和盘后阶段采集，具体时间可参考rank_time字段，状态N每小时更新一次，状态Y更新时间为22：30）)', 'trade_date'),
    ('tushare', 'ths_index', 1, 'code', 'ts_code, exchange, type', 'ts_code(指数代码)；exchange(市场类型A-a股 HK-港股 US-美股)；type(指数类型 N-概念指数 I-行业指数 R-地域指数 S-同花顺特色指数 ST-同花顺风格指数 TH-同花顺主题指数 BB-同花顺宽基指数)', NULL),
    ('tushare', 'ths_member', 1, 'code', 'ts_code, con_code', 'ts_code(板块指数代码)；con_code(股票代码)', NULL),
    ('tushare', 'tmt_twincome', 1, 'range', 'date, item, symbol, start_date, end_date, source', 'date(报告期)；item(产品代码)；symbol(公司代码)；start_date(报告期开始日期)；end_date(报告期结束日期)；source(nan)', 'date, start_date, end_date'),
    ('tushare', 'tmt_twincomedetail', 1, 'range', 'date, item, symbol, start_date, end_date, source', 'date(报告期)；item(产品代码)；symbol(公司代码)；start_date(报告期开始日期)；end_date(报告期结束日期)；source(nan)', 'date, start_date, end_date'),
    ('tushare', 'top_inst', 1, 'date', 'trade_date, ts_code', 'trade_date(交易日期)；ts_code(TS代码)', 'trade_date'),
    ('tushare', 'top_list', 1, 'date', 'trade_date, ts_code', 'trade_date(交易日期)；ts_code(股票代码)', 'trade_date'),
    ('tushare', 'top10_floatholders', 1, 'range', 'ts_code, period, ann_date, start_date, end_date', 'ts_code(TS代码)；period(报告期（YYYYMMDD格式，一般为每个季度最后一天）)；ann_date(公告日期)；start_date(报告期开始日期)；end_date(报告期结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'top10_holders', 1, 'range', 'ts_code, period, ann_date, start_date, end_date', 'ts_code(TS代码)；period(报告期（YYYYMMDD格式，一般为每个季度最后一天）)；ann_date(公告日期)；start_date(报告期开始日期)；end_date(报告期结束日期)', 'ann_date, start_date, end_date'),
    ('tushare', 'trade_cal', 1, 'range', 'exchange, start_date, end_date, is_open', 'exchange(交易所 SHFE 上期所 DCE 大商所 CFFEX中金所 CZCE郑商所 INE上海国际能源交易所)；start_date(开始日期)；end_date(结束日期)；is_open(是否交易 0休市 1交易)', 'start_date, end_date'),
    ('tushare', 'us_adjfactor', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码)；trade_date(交易日期（格式：YYYYMMDD，下同）)；start_date(开始日期)；end_date(结束日期)', 'trade_date, start_date, end_date'),
    ('tushare', 'us_balancesheet', 1, 'range', 'ts_code, period, ind_name, report_type, start_date, end_date', 'ts_code(股票代码)；period(报告期（格式：YYYYMMDD，每个季度最后一天的日期，如20241231))；ind_name(指标名(如：新增借款）)；report_type(报告期类型(Q1一季报Q2半年报Q3三季报Q4年报))；start_date(报告期开始时间（格式：YYYYMMDD）)；end_date(报告结束始时间（格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'us_basic', 1, 'code', 'ts_code, classify, offset, limit', 'ts_code(股票代码)；classify(股票分类)；offset(开始行数)；limit(每页最大行数)', NULL),
    ('tushare', 'us_cashflow', 1, 'range', 'ts_code, period, ind_name, report_type, start_date, end_date', 'ts_code(股票代码)；period(报告期（格式：YYYYMMDD，每个季度最后一天的日期，如20241231))；ind_name(指标名(如：新增借款）)；report_type(报告期类型(Q1一季报Q2半年报Q3三季报Q4年报))；start_date(报告期开始时间（格式：YYYYMMDD）)；end_date(报告结束始时间（格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'us_daily', 1, 'range', 'ts_code, trade_date, start_date, end_date', 'ts_code(股票代码（e.g. AAPL）)；trade_date(交易日期（YYYYMMDD）)；start_date(开始日期（YYYYMMDD）)；end_date(结束日期（YYYYMMDD）)', 'trade_date, start_date, end_date'),
    ('tushare', 'us_daily_adj', 1, 'range', 'ts_code, trade_date, start_date, end_date, exchange, offset, limit', 'ts_code(股票代码（e.g. AAPL）)；trade_date(交易日期（YYYYMMDD）)；start_date(开始日期（YYYYMMDD）)；end_date(结束日期（YYYYMMDD）)；exchange(交易所（NAS/NYS/OTC))；offset(开始行数)；limit(每页行数行数)', 'trade_date, start_date, end_date'),
    ('tushare', 'us_fina_indicator', 1, 'range', 'ts_code, period, report_type, start_date, end_date', 'ts_code(股票代码)；period(报告期（格式：YYYYMMDD，每个季度最后一天的日期，如20241231))；report_type(报告期类型(Q1一季报Q2半年报Q3三季报Q4年报))；start_date(报告期开始时间（格式：YYYYMMDD）)；end_date(报告结束始时间（格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'us_income', 1, 'range', 'ts_code, period, ind_name, report_type, start_date, end_date', 'ts_code(股票代码)；period(报告期（格式：YYYYMMDD，每个季度最后一天的日期，如20241231))；ind_name(指标名(如：新增借款）)；report_type(报告期类型(Q1一季报Q2半年报Q3三季报Q4年报))；start_date(报告期开始时间（格式：YYYYMMDD）)；end_date(报告结束始时间（格式：YYYYMMDD）)', 'start_date, end_date'),
    ('tushare', 'us_tbr', 1, 'range', 'date, start_date, end_date, fields', 'date(日期)；start_date(开始日期(YYYYMMDD格式))；end_date(结束日期)；fields(指定输出字段(e.g. fields=''w4_bd,w52_ce''))', 'date, start_date, end_date'),
    ('tushare', 'us_tltr', 1, 'range', 'date, start_date, end_date, fields', 'date(日期)；start_date(开始日期)；end_date(结束日期)；fields(指定字段)', 'date, start_date, end_date'),
    ('tushare', 'us_tradecal', 1, 'range', 'start_date, end_date, is_open', 'start_date(开始日期)；end_date(结束日期)；is_open(是否交易)', 'start_date, end_date'),
    ('tushare', 'us_trltr', 1, 'range', 'date, start_date, end_date, fields', 'date(日期)；start_date(开始日期)；end_date(结束日期)；fields(指定字段)', 'date, start_date, end_date'),
    ('tushare', 'us_trycr', 1, 'range', 'date, start_date, end_date, fields', 'date(日期 （YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)；fields(指定输出字段)', 'date, start_date, end_date'),
    ('tushare', 'us_tycr', 1, 'range', 'date, start_date, end_date, fields', 'date(日期 （YYYYMMDD格式，下同）)；start_date(开始日期)；end_date(结束日期)；fields(指定输出字段（e.g. fields=''m1,y1''）)', 'date, start_date, end_date'),
    ('tushare', 'wz_index', 1, 'range', 'date, start_date, end_date', 'date(日期)；start_date(开始日期)；end_date(结束日期)', 'date, start_date, end_date'),
    ('tushare', 'yc_cb', 1, 'range', 'ts_code, curve_type, trade_date, start_date, end_date, curve_term', 'ts_code(收益率曲线编码：1001.CB-国债收益率曲线)；curve_type(曲线类型：0-到期，1-即期)；trade_date(交易日期)；start_date(查询起始日期)；end_date(查询结束日期)；curve_term(期限)', 'trade_date, start_date, end_date')

UPDATE `data_source_items` dsi
JOIN `_tmp_backfill_analysis_seed` seed ON seed.source = dsi.source AND seed.item_key = dsi.item_key
SET dsi.supports_backfill = seed.supports_backfill,
        dsi.backfill_mode = seed.backfill_mode,
        dsi.input_params = seed.input_params,
        dsi.input_param_details = seed.input_param_details,
        dsi.analysis_date_params = seed.analysis_date_params,
        dsi.input_params_meta = JSON_OBJECT(
            'input_params',
            CASE
                WHEN seed.input_params IS NULL OR TRIM(seed.input_params) = '' THEN JSON_ARRAY()
                ELSE CAST(
                    CONCAT(
                        '["',
                        REPLACE(REPLACE(REPLACE(TRIM(seed.input_params), ', ', ','), ' ,', ','), ',', '","'),
                        '"]'
                    ) AS JSON
                )
            END,
            'analysis_date_params',
            CASE
                WHEN seed.analysis_date_params IS NULL OR TRIM(seed.analysis_date_params) = '' THEN JSON_ARRAY()
                ELSE CAST(
                    CONCAT(
                        '["',
                        REPLACE(REPLACE(REPLACE(TRIM(seed.analysis_date_params), ', ', ','), ' ,', ','), ',', '","'),
                        '"]'
                    ) AS JSON
                )
            END,
            'supports_backfill',
            seed.supports_backfill,
            'backfill_mode',
            seed.backfill_mode
        );

DROP TEMPORARY TABLE `_tmp_backfill_analysis_seed`;

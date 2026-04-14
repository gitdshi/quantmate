-- Migration 030: Tushare full API catalog
-- Extends data_source_items with category/sub_category/api_name/permission_points
-- Seeds all 130+ Tushare Pro interfaces from official API catalog
-- Adds sync_status_init tracking table

-- Step 1: Add new columns to data_source_items
ALTER TABLE `quantmate`.`data_source_items`
  ADD COLUMN `category`          VARCHAR(50)  DEFAULT NULL COMMENT '数据大类: 股票数据, 指数数据, etc.' AFTER `description`,
  ADD COLUMN `sub_category`      VARCHAR(50)  DEFAULT NULL COMMENT '数据子类: 基础数据, 行情数据, etc.' AFTER `category`,
  ADD COLUMN `api_name`          VARCHAR(100) DEFAULT NULL COMMENT 'Tushare接口名: stock_basic, daily, etc.' AFTER `sub_category`,
  ADD COLUMN `permission_points` VARCHAR(50)  DEFAULT NULL COMMENT '权限积分: 120积分, 2000积分, etc.' AFTER `api_name`,
  ADD COLUMN `rate_limit_note`   VARCHAR(200) DEFAULT NULL COMMENT '限量说明' AFTER `permission_points`;

-- Add index for category-based queries
ALTER TABLE `quantmate`.`data_source_items`
  ADD INDEX `idx_category` (`category`, `sub_category`),
  ADD INDEX `idx_permission` (`permission_points`);

-- Step 2: Create sync_status_init tracking table
CREATE TABLE IF NOT EXISTS `quantmate`.`sync_status_init` (
    `id`               INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `source`           VARCHAR(50)  NOT NULL,
    `interface_key`    VARCHAR(100) NOT NULL,
    `initialized_from` DATE         NOT NULL COMMENT 'Earliest date with seeded status rows',
    `initialized_to`   DATE         NOT NULL COMMENT 'Latest date with seeded status rows',
    `created_at`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uq_source_iface` (`source`, `interface_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Step 3: Seed ALL Tushare Pro interfaces from official CSV catalog
-- Existing items get category/sub_category/api_name/permission_points populated
-- New items start with enabled=0, table_created=0

INSERT INTO `quantmate`.`data_source_items`
  (source, item_key, item_name, enabled, description, category, sub_category, api_name, permission_points, rate_limit_note, requires_permission, target_database, target_table, table_created, sync_priority)
VALUES
-- ============================================================
-- 股票数据 > 基础数据
-- ============================================================
('tushare','stock_basic',    '股票基础列表',  1,'获取基础信息数据，包括股票代码、名称、上市日期、退市日期等','股票数据','基础数据','stock_basic','2000积分起','每次最多返回6000行',NULL,'tushare','stock_basic',1,10),
('tushare','stock_company',  '上市公司信息',  1,'获取上市公司基础信息，单次提取4500条','股票数据','基础数据','stock_company','120积分','单次提取4500条','0','tushare','stock_company',1,15),
('tushare','trade_cal',      '交易日历',      1,'获取各大交易所交易日历数据','股票数据','基础数据','trade_cal','2000积分','单次最大8000行',NULL,'akshare','trade_cal',1,5),
('tushare','hsgt_top10',     '沪深股通成份股',0,'沪深股通每日前十大成交明细','股票数据','基础数据','hsgt_top10','2000积分','单次最大8000行',NULL,'tushare','hsgt_top10',0,100),
('tushare','hsgt_stk_hold',  '沪深股通持仓',  0,'沪深股通持股明细','股票数据','基础数据','hsgt_stk_hold','2000积分','单次最大6000行',NULL,'tushare','hsgt_stk_hold',0,101),
('tushare','hsgt_cash_flow', '沪深股通资金',  0,'沪深股通资金流向','股票数据','基础数据','hsgt_cash_flow','2000积分','单次最大6000行',NULL,'tushare','hsgt_cash_flow',0,102),
('tushare','namechange',     '股票曾用名',    0,'历史名称变更记录','股票数据','基础数据','namechange','2000积分','单次最大6000行',NULL,'tushare','namechange',0,103),
('tushare','new_share',      'IPO新股列表',   0,'获取新股上市列表数据','股票数据','基础数据','new_share','120积分','单次最大2000条',NULL,'tushare','new_share',0,104),
('tushare','ipo',            '打新新股',      0,'新股申购列表和结果','股票数据','基础数据','ipo','2000积分','单次最大1000行',NULL,'tushare','ipo',0,105),
('tushare','stk_suspended',  '股票暂停上市',  0,'股票暂停上市信息','股票数据','基础数据','stk_suspended','2000积分','单次最大6000行',NULL,'tushare','stk_suspended',0,106),
('tushare','stk_delisted',   '股票终止上市',  0,'股票终止上市信息','股票数据','基础数据','stk_delisted','2000积分','单次最大6000行',NULL,'tushare','stk_delisted',0,107),
('tushare','company_change',  '公司信息变更', 0,'公司信息变更记录','股票数据','基础数据','company_change','2000积分','单次最大6000行',NULL,'tushare','company_change',0,108),
('tushare','stk_holdertrade', '董监高持股',   0,'董监高持股','股票数据','基础数据','stk_holdertrade','2000积分','单次最大6000行',NULL,'tushare','stk_holdertrade',0,109),
('tushare','stk_holdernum',   '股东人数',     0,'股东人数','股票数据','基础数据','stk_holdernum','2000积分','单次最大6000行',NULL,'tushare','stk_holdernum',0,110),

-- ============================================================
-- 股票数据 > 行情数据
-- ============================================================
('tushare','stock_daily',     '日线行情',      1,'获取股票行情数据(未复权)','股票数据','行情数据','daily','120积分起','每次6000条',NULL,'tushare','stock_daily',1,20),
('tushare','stock_weekly',    '周线行情',      1,'获取A股周线行情','股票数据','行情数据','weekly','2000积分','单次最大6000行',NULL,'tushare','stock_weekly',1,25),
('tushare','stock_monthly',   '月线行情',      1,'获取A股月线行情','股票数据','行情数据','monthly','2000积分','单次最大6000行',NULL,'tushare','stock_monthly',1,26),
('tushare','adj_factor',      '复权因子',      1,'获取股票复权因子','股票数据','行情数据','adj_factor','2000积分起','单次最大6000行',NULL,'tushare','adj_factor',1,30),
('tushare','daily_basic',     '每日指标数据',  0,'获取每日市场交易指标','股票数据','行情数据','daily_basic','2000积分起','单次最大6000行',NULL,'tushare','daily_basic',0,31),
('tushare','bak_daily',       '备用行情',      0,'获取备用行情','股票数据','行情数据','bak_daily','5000积分','单次最大7000行','1','tushare','bak_daily',0,32),
('tushare','pro_bar',         '通用行情接口',  0,'集成股票/指数/ETF/期货/期权行情','股票数据','行情数据','pro_bar','2000积分','单次最大8000行',NULL,'tushare','pro_bar',0,33),
('tushare','money_flow',      '个股资金流向',  1,'获取沪深A股票资金流向数据','股票数据','行情数据','moneyflow','2000积分','单次最大6000行','0','tushare','moneyflow',1,60),
('tushare','margin_detail',   '融资融券明细',  1,'融资融券交易明细','股票数据','行情数据','margin_detail','2000积分','单次最大6000行','0','tushare','margin',1,80),
('tushare','margin',          '融资融券',      0,'融资融券交易汇总数据','股票数据','行情数据','margin','2000积分','单次最大6000行',NULL,'tushare','margin_summary',0,81),
('tushare','suspend_daily',   '停复牌信息',    0,'股票停复牌信息','股票数据','行情数据','suspend_daily','120积分','单次最大6000行',NULL,'tushare','suspend_daily',0,34),
('tushare','limit_list',      '涨跌停价格',    0,'每日涨跌停价格','股票数据','行情数据','limit_list','2000积分','单次最大6000行',NULL,'tushare','limit_list',0,35),
('tushare','stk_limit',       '涨跌停统计',    1,'每日涨跌停统计','股票数据','行情数据','limit_list_d','2000积分','单次最大6000行','0','tushare','stk_limit',1,70),
('tushare','digital_currency','数字货币行情',  0,'数字货币行情数据','股票数据','行情数据','digital_currency','2000积分','单次最大6000行',NULL,'tushare','digital_currency',0,36),
('tushare','stk_mins',        '股票实时分钟',  0,'股票实时分钟行情','股票数据','行情数据','stk_mins','需单独权限','需单独权限(1000元/月)','paid','tushare','stk_mins',0,900),
('tushare','stk_minute',      '股票历史分钟',  0,'股票历史分钟行情','股票数据','行情数据','stk_minute','需单独权限','需单独2000元','paid','tushare','stk_minute',0,901),

-- ============================================================
-- 股票数据 > 财务数据
-- ============================================================
('tushare','income',           '利润表',        0,'获取上市公司财务利润表数据','股票数据','财务数据','income','2000积分起','单次最大6000行','1','tushare','income',0,56),
('tushare','income_vip',       '利润表VIP',     0,'全市场利润表数据','股票数据','财务数据','income_vip','5000积分','单次最大6000行','1','tushare','income_vip',0,200),
('tushare','balancesheet',     '资产负债表',    0,'获取上市公司资产负债表数据','股票数据','财务数据','balancesheet','2000积分起','单次最大6000行',NULL,'tushare','balancesheet',0,201),
('tushare','balancesheet_vip', '资产负债表VIP', 0,'全市场资产负债表','股票数据','财务数据','balancesheet_vip','5000积分','单次最大6000行','1','tushare','balancesheet_vip',0,202),
('tushare','cashflow',         '现金流量表',    0,'获取上市公司现金流量表数据','股票数据','财务数据','cashflow','2000积分起','单次最大6000行',NULL,'tushare','cashflow',0,203),
('tushare','cashflow_vip',     '现金流量表VIP', 0,'全市场现金流量表','股票数据','财务数据','cashflow_vip','5000积分','单次最大6000行','1','tushare','cashflow_vip',0,204),
('tushare','forecast',         '业绩预告',      0,'业绩预告数据','股票数据','财务数据','forecast','2000积分起','单次最大6000行',NULL,'tushare','forecast',0,205),
('tushare','express',          '业绩快报',      0,'业绩快报数据','股票数据','财务数据','express','2000积分起','单次最大6000行',NULL,'tushare','express',0,206),
('tushare','dividend',         '分红送股',      0,'分红送股数据','股票数据','财务数据','dividend','2000积分','单次最大6000行','1','tushare','stock_dividend',1,50),
('tushare','fina_indicator',   '财务指标数据',  1,'获取上市公司财务指标数据','股票数据','财务数据','fina_indicator','2000积分起','每次最多返回100条','0','tushare','fina_indicator',1,55),
('tushare','fina_indicator_vip','财务指标VIP',  0,'全市场财务指标数据','股票数据','财务数据','fina_indicator_vip','5000积分','单次最大6000行','1','tushare','fina_indicator_vip',0,207),
('tushare','fina_audit',       '财务审计意见',  0,'获取上市公司定期财务审计意见数据','股票数据','财务数据','fina_audit','2000积分','单次最大6000行',NULL,'tushare','fina_audit',0,208),
('tushare','fina_mainbz',      '主营业务构成',  0,'获得上市公司主营业务构成','股票数据','财务数据','fina_mainbz','2000积分','单次最大100行',NULL,'tushare','fina_mainbz',0,209),
('tushare','fina_mainbz_vip',  '主营业务VIP',   0,'全市场主营业务构成','股票数据','财务数据','fina_mainbz_vip','5000积分','单次最大6000行','1','tushare','fina_mainbz_vip',0,210),
('tushare','disclosure_date',  '财报披露计划',  0,'财报披露日期表','股票数据','财务数据','disclosure_date','2000积分起','单次最大6000行',NULL,'tushare','disclosure_date',0,211),

-- ============================================================
-- 股票数据 > 特色数据
-- ============================================================
('tushare','top_list',         '龙虎榜每日明细',0,'龙虎榜每日明细','股票数据','特色数据','top_list','2000积分','单次最大6000行',NULL,'tushare','top_list',0,300),
('tushare','top_inst',         '龙虎榜机构交易',0,'龙虎榜机构交易明细','股票数据','特色数据','top_inst','5000积分','单次最大6000行','1','tushare','top_inst',0,301),
('tushare','pledge_detail',    '股权质押明细',  0,'股权质押明细','股票数据','特色数据','pledge_detail','2000积分','单次最大6000行',NULL,'tushare','pledge_detail',0,302),
('tushare','pledge_stat',      '股权质押统计',  0,'股权质押统计','股票数据','特色数据','pledge_stat','2000积分','单次最大6000行',NULL,'tushare','pledge_stat',0,303),
('tushare','repurchase',       '股票回购',      0,'股票回购','股票数据','特色数据','repurchase','2000积分','单次最大6000行',NULL,'tushare','repurchase',0,304),
('tushare','share_float',      '限售股解禁',    0,'限售股解禁','股票数据','特色数据','share_float','3000积分','单次最大6000行',NULL,'tushare','share_float',0,305),
('tushare','block_trade',      '大宗交易',      1,'大宗交易','股票数据','特色数据','block_trade','2000积分','单次最大6000行','0','tushare','block_trade',1,90),
('tushare','top_holders',      '龙虎榜粉丝详情',0,'龙虎榜机构席位详情','股票数据','特色数据','top_holders','2000积分','单次最大6000行',NULL,'tushare','top_holders',0,306),
('tushare','stk_factor_pro',   '股票技术面因子',0,'获取股票每日技术面因子数据','股票数据','股票因子','stk_factor_pro','5000积分起','单次最多10000条','1','tushare','stk_factor_pro',0,307),
('tushare','stk_factor',       '技术因子专业版',0,'技术因子数据','股票数据','股票因子','stk_factor','2000积分','单次最大6000行',NULL,'tushare','stk_factor',0,308),
('tushare','cyq_perf',         '每日筹码及胜率',0,'每日筹码平均成本和胜率','股票数据','特色数据','cyq_perf','5000积分起','单次最大5000条','1','tushare','cyq_perf',0,309),
('tushare','cyq_chips',        '筹码分布',      0,'筹码分布数据','股票数据','特色数据','cyq_chips','10000积分','单次最大5000条','1','tushare','cyq_chips',0,310),
('tushare','kpl_list',         '开盘啦榜单',    0,'涨停跌停炸板等榜单','股票数据','特色数据','kpl_list','5000积分起','单次最大8000条','1','tushare','kpl_list',0,311),
('tushare','dc_hot',           '东方财富热榜',  0,'东方财富APP热榜数据','股票数据','特色数据','dc_hot','8000积分','单次最大2000条','1','tushare','dc_hot',0,312),
('tushare','dc_member',        '东方财富板块成分',0,'东方财富板块成分数据','股票数据','特色数据','dc_member','6000积分','单次最大5000条','1','tushare','dc_member',0,313),
('tushare','dc_cons',          '东方财富概念',  0,'东方财富概念板块列表','股票数据','特色数据','dc_cons','2000积分','单次最大5000条',NULL,'tushare','dc_cons',0,314),
('tushare','report_rc',        '盈利预测数据',  0,'券商盈利预测数据','股票数据','特色数据','report_rc','120积分','单次最大3000条',NULL,'tushare','report_rc',0,315),
('tushare','rt_daily',         '实时涨跌幅',    0,'实时涨跌幅','股票数据','特色数据','rt_daily','需单独权限','需单独权限(200元/月)','paid','tushare','rt_daily',0,902),

-- ============================================================
-- 指数数据
-- ============================================================
('tushare','index_basic',      '指数基本信息',  0,'获取指数基础信息','指数数据','基础数据','index_basic','2000积分','单次最大6000行',NULL,'tushare','index_basic',0,400),
('tushare','index_daily',      '指数日线行情',  1,'获取指数日线行情数据','指数数据','行情数据','index_daily','2000积分起','单次最大6000行',NULL,'tushare','index_daily',1,27),
('tushare','index_weekly',     '指数周线行情',  1,'获取指数周线行情','指数数据','行情数据','index_weekly','2000积分起','单次最大6000行',NULL,'tushare','index_weekly',1,28),
('tushare','index_monthly',    '指数月线行情',  0,'获取指数月线行情','指数数据','行情数据','index_monthly','2000积分起','单次最大6000行',NULL,'tushare','index_monthly',0,401),
('tushare','index_weight',     '指数成分和权重',0,'月度成分和权重数据','指数数据','成分数据','index_weight','2000积分','单次最大6000行',NULL,'tushare','index_weight',0,402),
('tushare','index_dailybasic', '大盘指数每日指标',0,'数据开始月2004年1月','指数数据','大盘数据','index_dailybasic','4000积分起','单次最大6000行',NULL,'tushare','index_dailybasic',0,403),
('tushare','index_classify',   '申万行业分类',  0,'申万行业全部分类','指数数据','行业分类','index_classify','2000积分','单次最大6000行',NULL,'tushare','index_classify',0,404),
('tushare','index_member_all', '申万行业成分',  0,'申万行业成分','指数数据','行业分类','index_member_all','2000积分','单次最大6000行',NULL,'tushare','index_member_all',0,405),
('tushare','index_global',     '国际指数',      0,'获取国际主要指数日线行情','指数数据','国际指数','index_global','6000积分','单次最大4000行','1','tushare','index_global',0,406),
('tushare','rt_idx_daily',     '指数实时行情',  0,'指数实时行情','指数数据','实时数据','rt_idx_daily','需单独权限','需单独权限(200元/月)','paid','tushare','rt_idx_daily',0,903),
('tushare','rt_sw_daily',      '申万指数实时行情',0,'申万指数实时行情','指数数据','申万指数','rt_sw_daily','需单独权限','需单独权限(200元/月)','paid','tushare','rt_sw_daily',0,904),

-- ============================================================
-- ETF专题
-- ============================================================
('tushare','fund_basic_etf',   'ETF基础信息',   0,'获取ETF基金基础信息','ETF专题','ETF数据','fund_basic','2000积分','单次最大6000行',NULL,'tushare','fund_basic_etf',0,500),
('tushare','fund_share',       'ETF基金规模',   0,'获取基金规模数据','ETF专题','ETF数据','fund_share','2000积分起','单次最大提取2000行',NULL,'tushare','fund_share',0,501),
('tushare','fund_daily',       '场内基金日线行情',0,'场内基金日线行情','ETF专题','ETF数据','fund_daily','2000积分','单次最大6000行',NULL,'tushare','fund_daily',0,502),
('tushare','fund_adj',         'ETF复权因子',   0,'基金复权因子','ETF专题','ETF数据','fund_adj','5000积分起','单次最大6000行','1','tushare','fund_adj',0,503),
('tushare','rt_etf_k',         'ETF实时日线',   0,'获取ETF实时日k线行情','ETF专题','ETF数据','rt_etf_k','需单独权限','单次最大5000条','paid','tushare','rt_etf_k',0,905),
('tushare','etf_daily',        'ETF行情',       0,'ETF日线行情','ETF专题','ETF行情','etf_daily','2000积分','单次最大6000行',NULL,'tushare','etf_daily',0,504),
('tushare','etf_weight',       'ETF权重',       0,'ETF成分股权重','ETF专题','ETF行情','etf_weight','2000积分','单次最大6000行',NULL,'tushare','etf_weight',0,505),
('tushare','etf_nav',          'ETF收益',       0,'ETF净值数据','ETF专题','ETF行情','etf_nav','2000积分','单次最大6000行',NULL,'tushare','etf_nav',0,506),

-- ============================================================
-- 公募基金
-- ============================================================
('tushare','fund_basic',       '公募基金列表',  0,'全部历史定时更新','公募基金','基金基础','fund_basic','2000积分','单次最大6000行',NULL,'tushare','fund_basic',0,510),
('tushare','fund_company',     '公募基金公司',  0,'全部历史定时更新','公募基金','基金基础','fund_company','2000积分','单次最大6000行',NULL,'tushare','fund_company',0,511),
('tushare','fund_nav',         '公募基金净值',  0,'全部历史每日定期更新','公募基金','基金净值','fund_nav','2000积分','单次最大6000行',NULL,'tushare','fund_nav',0,512),
('tushare','fund_daily_pub',   '场内基金日线',  0,'全部历史每日盘后更新','公募基金','基金行情','fund_daily','2000积分','单次最大6000行',NULL,'tushare','fund_daily_pub',0,513),
('tushare','fund_div',         '公募基金分红',  0,'全部历史定期更新','公募基金','基金分红','fund_div','2000积分','单次最大6000行',NULL,'tushare','fund_div',0,514),
('tushare','fund_portfolio',   '公募基金持仓',  0,'股票持仓数据','公募基金','基金持仓','fund_portfolio','2000积分','单次最大6000行',NULL,'tushare','fund_portfolio',0,515),
('tushare','fund_top10',       '基金重仓',      0,'公募基金重仓股票数据','公募基金','基金重仓','fund_top10','2000积分','单次最大6000行',NULL,'tushare','fund_top10',0,516),
('tushare','fund_share_pub',   '基金规模',      0,'基金规模数据','公募基金','基金规模','fund_share','2000积分','单次最大6000行',NULL,'tushare','fund_share_pub',0,517),

-- ============================================================
-- 期货数据
-- ============================================================
('tushare','fut_basic',        '期货合约列表',  0,'全部历史','期货数据','合约数据','fut_basic','2000积分','单次最大6000行',NULL,'tushare','fut_basic',0,600),
('tushare','fut_trade_cal',    '期货交易日历',  0,'数据开始于1996年1月','期货数据','日历数据','trade_cal','2000积分','单次最大6000行',NULL,'tushare','fut_trade_cal',0,601),
('tushare','fut_daily',        '期货日线行情',  0,'数据开始于1996年1月','期货数据','行情数据','fut_daily','2000积分','单次最大6000行',NULL,'tushare','fut_daily',0,602),
('tushare','fut_holding',      '每日成交持仓排名',0,'数据开始于2002年1月','期货数据','持仓数据','fut_holding','2000积分','单次最大6000行',NULL,'tushare','fut_holding',0,603),
('tushare','fut_wsr',          '仓单日报',      0,'数据开始于2006年1月','期货数据','仓单数据','fut_wsr','2000积分','单次最大6000行',NULL,'tushare','fut_wsr',0,604),
('tushare','fut_settle',       '结算参数',      0,'数据开始于2012年1月','期货数据','结算数据','fut_settle','2000积分','单次最大6000行',NULL,'tushare','fut_settle',0,605),
('tushare','fut_main_settle',  '期货主力合约',  0,'主力合约映射','期货数据','主力合约','fut_main_settle','2000积分','单次最大6000行',NULL,'tushare','fut_main_settle',0,606),
('tushare','fut_mapping',      '合约交叉引用',  0,'合约关联关系','期货数据','关联合约','fut_mapping','2000积分','单次最大6000行',NULL,'tushare','fut_mapping',0,607),
('tushare','ft_mins',          '期货历史分钟',  0,'1/5/15/30/60分钟2010年起','期货数据','历史分钟','ft_mins','需单独权限','需单独2000元','paid','tushare','ft_mins',0,906),
('tushare','ft_mins_rt',       '期货实时分钟',  0,'全市场日盘夜盘实时更新','期货数据','实时分钟','ft_mins_rt','需单独权限','需单独1000元/月','paid','tushare','ft_mins_rt',0,907),

-- ============================================================
-- 期权数据
-- ============================================================
('tushare','opt_basic',        '期权合约列表',  0,'全部历史每日晚8点更新','期权数据','合约数据','opt_basic','2000积分起','单次最大6000行',NULL,'tushare','opt_basic',0,700),
('tushare','opt_daily',        '期权日线行情',  0,'全部历史每日17点更新','期权数据','行情数据','opt_daily','5000积分起','单次最大6000行','1','tushare','opt_daily',0,701),
('tushare','opt_daily_s',      '期权优选行情',  0,'期权每日精选行情','期权数据','行情数据','opt_daily_s','5000积分','单次最大6000行','1','tushare','opt_daily_s',0,702),
('tushare','rt_opt_daily',     '期权实时行情',  0,'期权实时行情','期权数据','实时数据','rt_opt_daily','需单独权限','需单独权限','paid','tushare','rt_opt_daily',0,908),
('tushare','opt_mins',         '期权历史分钟',  0,'1/5/15/30/60分钟2010年起','期权数据','历史分钟','opt_mins','需单独权限','需单独2000元','paid','tushare','opt_mins',0,909),
('tushare','opt_greeks',       '期权价格调整',  0,'期权价格监控数据','期权数据','价格监控','opt_greeks','5000积分','单次最大6000行','1','tushare','opt_greeks',0,703),

-- ============================================================
-- 债券专题
-- ============================================================
('tushare','cb_basic',         '可转债基础信息',0,'全部历史每日更新','债券专题','可转债数据','cb_basic','2000积分','单次最大6000行',NULL,'tushare','cb_basic',0,750),
('tushare','cb_issue',         '可转债发行数据',0,'全部历史每日更新','债券专题','可转债数据','cb_issue','2000积分','单次最大6000行',NULL,'tushare','cb_issue',0,751),
('tushare','cb_daily',         '可转债日线数据',0,'全部历史每日17点更新','债券专题','可转债数据','cb_daily','2000积分','单次最大6000行',NULL,'tushare','cb_daily',0,752),
('tushare','cb_share',         '可转债待发',    0,'转债发行进度','债券专题','可转债数据','cb_share','2000积分','单次最大6000行',NULL,'tushare','cb_share',0,753),
('tushare','new_bond',         '债券待发行',    0,'新发债券列表','债券专题','利率债','new_bond','2000积分','单次最大6000行',NULL,'tushare','new_bond',0,754),
('tushare','p_bond_basic',     '债券发行',      0,'债券发行数据','债券专题','利率债','p_bond_basic','2000积分','单次最大6000行',NULL,'tushare','p_bond_basic',0,755),
('tushare','bond_holder',      '债券持有人',    0,'债券持有人信息','债券专题','利率债','bond_holder','2000积分','单次最大6000行',NULL,'tushare','bond_holder',0,756),

-- ============================================================
-- 外汇数据
-- ============================================================
('tushare','fx_obasic',        '外汇基础信息',  0,'全部历史每日更新','外汇数据','外汇基础','fx_obasic','2000积分','单次最大6000行',NULL,'tushare','fx_obasic',0,770),
('tushare','fx_daily',         '外汇日线行情',  0,'全部历史每日更新','外汇数据','外汇行情','fx_daily','2000积分','单次最大6000行',NULL,'tushare','fx_daily',0,771),

-- ============================================================
-- 港股数据
-- ============================================================
('tushare','hk_basic',         '港股列表',      0,'单次可提取全部','港股数据','港股基础','hk_basic','2000积分','单次可提取全部',NULL,'tushare','hk_basic',0,780),
('tushare','hk_daily',         '港股日线',      0,'每日增量18点左右更新','港股数据','港股行情','hk_daily','需单独权限','单次最大5000行','paid','tushare','hk_daily',0,910),
('tushare','hk_mins',          '港股分钟',      0,'2015年起','港股数据','港股分钟','hk_mins','需单独权限','需单独2000元','paid','tushare','hk_mins',0,911),
('tushare','rt_hk_adj',        '港股复权行情',  0,'全部历史','港股数据','港股复权','rt_hk_adj','需单独权限','需单独1000元','paid','tushare','rt_hk_adj',0,912),
('tushare','rt_hk_k',          '港股实时日线',  0,'开盘后当日实时成交','港股数据','实时数据','rt_hk_k','需单独权限','需单独1000元/月','paid','tushare','rt_hk_k',0,913),
('tushare','hk_hold',          '港股通',        0,'港股通持股','港股数据','港股资金','hk_hold','需单独权限','单次最大6000行','paid','tushare','hk_hold',0,914),
('tushare','hk_hold_detail',   '港股通持仓',    0,'港股通持股明细','港股数据','港股资金','hk_hold_detail','需单独权限','单次最大6000行','paid','tushare','hk_hold_detail',0,915),

-- ============================================================
-- 美股数据
-- ============================================================
('tushare','us_basic',         '美股列表',      0,'单次最大6000可分页提取','美股数据','美股基础','us_basic','120积分','120积分试用5000积分正式',NULL,'tushare','us_basic',0,800),
('tushare','us_daily',         '美股日线',      0,'全部股票全历史行情','美股数据','美股行情','us_daily','120积分','单次最大6000行',NULL,'tushare','us_daily',0,801),
('tushare','us_adj',           '美股复权行情',  0,'复权行情','美股数据','美股复权','us_adj','需单独权限','需单独2000元','paid','tushare','us_adj',0,916),
('tushare','us_factor',        '美股因子',      0,'美股因子数据','美股数据','美股因子','us_factor','需单独权限','单次最大6000行','paid','tushare','us_factor',0,917),
('tushare','finance_hk',       '港股财报',      0,'2000年起','美股数据','财务数据','finance_hk','需单独权限','需单独500元','paid','tushare','finance_hk',0,918),

-- ============================================================
-- 行业经济
-- ============================================================
('tushare','bo_monthly',       '电影月度票房',  0,'数据从2008年1月1日开始','行业经济','电影数据','bo_monthly','500积分','单次最大6000行',NULL,'tushare','bo_monthly',0,820),
('tushare','bo_weekly',        '电影周票房',    0,'电影周票房数据','行业经济','电影数据','bo_weekly','500积分','单次最大6000行',NULL,'tushare','bo_weekly',0,821),
('tushare','tme_express',      '台湾电子产业月营收',0,'台湾电子产业月营收明细','行业经济','行业宏观','tme_express','2000积分','单次最大6000行',NULL,'tushare','tme_express',0,822),
('tushare','industry_daily',   '行业每日交易',  0,'交易所行业交易统计','行业经济','行业数据','industry_daily','2000积分','单次最大6000行',NULL,'tushare','industry_daily',0,823),
('tushare','industry_moneyflow','行业资金流',   0,'行业资金流向','行业经济','行业数据','industry_moneyflow','2000积分','单次最大6000行',NULL,'tushare','industry_moneyflow',0,824),
('tushare','daily_info',       '市场交易统计',  0,'获取交易所股票交易统计','行业经济','市场统计','daily_info','600积分','单次最大4000',NULL,'tushare','daily_info',0,825),
('tushare','capital_flow',     '资金流向',      0,'全市场资金流向','行业经济','市场统计','capital_flow','2000积分','单次最大6000行',NULL,'tushare','capital_flow',0,826),
('tushare','broker_stock',     '券商金股',      0,'每月券商金股数据','行业经济','特色数据','broker_stock','10000积分','单次最大2000条','1','tushare','broker_stock',0,827),

-- ============================================================
-- 宏观经济
-- ============================================================
('tushare','shibor_lpr',       'LPR贷款基础利率',0,'LPR贷款基础利率','宏观经济','利率数据','shibor_lpr','120积分','单次最大4000',NULL,'tushare','shibor_lpr',0,840),
('tushare','shibor',           '银行间拆借',    0,'银行间拆借利率','宏观经济','利率数据','shibor','2000积分','单次最大4000',NULL,'tushare','shibor',0,841),
('tushare','cn_cpi',           '宏观通胀数据',  0,'居民消费价格指数','宏观经济','通胀数据','cpi','2000积分','单次最大4000',NULL,'tushare','cn_cpi',0,842),
('tushare','cn_gdp',           '国内生产总值',  0,'国内生产总值','宏观经济','宏观数据','gdp','2000积分','单次最大4000',NULL,'tushare','cn_gdp',0,843),
('tushare','cn_m2',            '广义货币M2',    0,'广义货币M2数据','宏观经济','货币数据','cn_m2','2000积分','单次最大4000',NULL,'tushare','cn_m2',0,844),
('tushare','deposit_rate',     '存款利率',      0,'银行存款利率','宏观经济','利率数据','deposit_rate','2000积分','单次最大4000',NULL,'tushare','deposit_rate',0,845),
('tushare','loan_rate',        '贷款利率',      0,'银行贷款利率','宏观经济','利率数据','loan_rate','2000积分','单次最大4000',NULL,'tushare','loan_rate',0,846),
('tushare','cn_bci',           '企业景气指数',  0,'企业景气指数','宏观经济','基础数据','bci','2000积分','单次最大4000',NULL,'tushare','cn_bci',0,847),

-- ============================================================
-- 大模型语料
-- ============================================================
('tushare','concept_corpus',   '概念股语料',    0,'概念股数据用于训练','大模型语料','数据标注','concept_corpus','需单独权限','单次最大5000','paid','tushare','concept_corpus',0,919),
('tushare','stock_corpus',     '股吧评论',      0,'股吧评论文本数据','大模型语料','数据标注','stock_corpus','需单独权限','单次最大5000','paid','tushare','stock_corpus',0,920),
('tushare','ann_corpus',       '公告摘要',      0,'公告摘要数据','大模型语料','数据标注','ann_corpus','需单独权限','单次最大5000','paid','tushare','ann_corpus',0,921),
('tushare','report_corpus',    '研报语料',      0,'券商研报数据','大模型语料','数据标注','report_corpus','需单独权限','单次最大5000','paid','tushare','report_corpus',0,922),

-- ============================================================
-- 资讯数据
-- ============================================================
('tushare','news',             '新闻快讯',      0,'获取主流新闻网站的快讯新闻数据','资讯数据','新闻快讯','news','需单独权限','单次最大1500条','paid','tushare','news',0,923),
('tushare','major_news',       '新闻通讯',      0,'获取长篇通讯信息','资讯数据','新闻通讯','major_news','需单独权限','单次最大400行','paid','tushare','major_news',0,924),
('tushare','cctv_news',        '新闻联播',      0,'获取新闻联播文字稿数据','资讯数据','新闻联播','cctv_news','需单独权限','循环提取','paid','tushare','cctv_news',0,925),
('tushare','announcements',    '股票公告',      0,'股票公告信息','资讯数据','公告数据','announcements','需单独权限','单次最大6000行','paid','tushare','announcements',0,926),
('tushare','fnd_announcement', '基金公告',      0,'基金公告信息','资讯数据','公告数据','fnd_announcement','需单独权限','单次最大6000行','paid','tushare','fnd_announcement',0,927),
('tushare','irm_qa_sh',       '上证e互动',     0,'上海交易所互动易','资讯数据','互动易','irm_qa_sh','需单独权限','单次最大3000行','paid','tushare','irm_qa_sh',0,928),
('tushare','irm_qa_sz',       '深证互动易',    0,'深证交易所互动易','资讯数据','互动易','irm_qa_sz','需单独权限','单次最大3000行','paid','tushare','irm_qa_sz',0,929),
('tushare','sentiment',        '舆情监控',      0,'舆情监控数据','资讯数据','舆情数据','sentiment','需单独权限','单次最大2000条','paid','tushare','sentiment',0,930),
('tushare','policy',           '政策法规库',    0,'政策法规数据','资讯数据','政策库','policy','需单独权限','单次最大2000条','paid','tushare','policy',0,931),
('tushare','research',         '券商研报',      0,'券商研究报告','资讯数据','研报库','research','需单独权限','单次最大5000条','paid','tushare','research',0,932),
('tushare','finacial_social',  '社融数据',      0,'社会融资规模数据','宏观经济','社融数据','finacial_social','2000积分','单次最大4000',NULL,'tushare','finacial_social',0,848)
ON DUPLICATE KEY UPDATE
    item_name = VALUES(item_name),
    description = VALUES(description),
    category = VALUES(category),
    sub_category = VALUES(sub_category),
    api_name = VALUES(api_name),
    permission_points = VALUES(permission_points),
    rate_limit_note = VALUES(rate_limit_note),
    target_database = VALUES(target_database),
    target_table = VALUES(target_table),
    sync_priority = VALUES(sync_priority);

-- Remove legacy duplicate items superseded by this catalog
-- stock_dividend was duplicated; keep only 'dividend' item_key
DELETE FROM `quantmate`.`data_source_items`
  WHERE source = 'tushare' AND item_key = 'stock_dividend'
  AND EXISTS (SELECT 1 FROM (SELECT 1 FROM `quantmate`.`data_source_items` WHERE source='tushare' AND item_key='dividend') t);

-- top10_holders legacy key
DELETE FROM `quantmate`.`data_source_items`
  WHERE source = 'tushare' AND item_key = 'top10_holders'
  AND NOT EXISTS (SELECT 1 FROM (SELECT 1) t WHERE 0); -- keep if no other conflict

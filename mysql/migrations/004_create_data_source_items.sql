-- Issue #5: Data source items configuration table
CREATE TABLE IF NOT EXISTS `quantmate`.`data_source_items` (
    id                  INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source              VARCHAR(20)  NOT NULL COMMENT 'tushare or akshare',
    item_key            VARCHAR(100) NOT NULL,
    item_name           VARCHAR(200) NOT NULL,
    enabled             TINYINT(1)   NOT NULL DEFAULT 1,
    description         TEXT         DEFAULT NULL,
    requires_permission VARCHAR(50)  DEFAULT NULL COMMENT 'Permission level required',
    updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_source_item (source, item_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Default enabled items
INSERT INTO `quantmate`.`data_source_items` (source, item_key, item_name, enabled, description) VALUES
('tushare', 'stock_basic',  '股票基本信息',  1, 'A股基本资料'),
('tushare', 'stock_daily',  '日线行情',      1, 'A股日K线'),
('tushare', 'adj_factor',   '复权因子',      1, '前复权因子'),
('tushare', 'trade_cal',    '交易日历',      1, '交易所交易日历')
ON DUPLICATE KEY UPDATE item_name = VALUES(item_name);

-- Default disabled items (require higher permissions)
INSERT INTO `quantmate`.`data_source_items` (source, item_key, item_name, enabled, requires_permission, description) VALUES
('tushare', 'top10_holders',   '十大股东',     0, 'premium', '十大流通股东'),
('tushare', 'stock_dividend',  '分红送股',     0, 'premium', '分红送转信息'),
('tushare', 'money_flow',      '资金流向',     0, 'premium', '个股资金流向'),
('tushare', 'margin_detail',   '融资融券',     0, 'premium', '融资融券明细'),
('akshare', 'stock_zh_index',  '指数行情',     0, NULL,      'A股指数实时行情'),
('akshare', 'fund_etf_daily',  'ETF日线',      0, NULL,      'ETF日K线数据')
ON DUPLICATE KEY UPDATE item_name = VALUES(item_name);

"""Static catalog-backed Tushare interfaces.

This module keeps interface registration code-defined rather than profile-driven.
Missing catalog items are registered through static specs and share a generic
sync implementation backed by code-owned DDL and DAO insert helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging

from app.datasync.base import BaseIngestInterface, InterfaceInfo, SyncResult, SyncStatus
from app.datasync.sources.tushare import ddl
from app.datasync.sources.tushare.sync_error_handling import handle_tushare_sync_exception

logger = logging.getLogger(__name__)

_TRADE_DATE_APIS = {
    "hsgt_top10",
    "hsgt_stk_hold",
    "hsgt_cash_flow",
    "daily_basic",
    "moneyflow",
    "margin_detail",
    "margin",
    "limit_list",
    "limit_list_d",
    "digital_currency",
    "cyq_perf",
    "report_rc",
    "index_monthly",
    "fund_daily",
    "fund_adj",
    "etf_daily",
    "etf_weight",
    "etf_nav",
    "fut_daily",
    "fut_holding",
    "fut_wsr",
    "fut_settle",
    "opt_daily",
    "opt_daily_s",
    "cb_daily",
    "fx_daily",
    "hk_daily",
    "us_daily",
    "industry_daily",
    "industry_moneyflow",
    "capital_flow",
    "daily_info",
    "block_trade",
}

_ANN_DATE_APIS = {
    "income",
    "income_vip",
    "balancesheet",
    "balancesheet_vip",
    "cashflow",
    "cashflow_vip",
    "forecast",
    "express",
    "fina_indicator",
    "fina_indicator_vip",
    "fina_audit",
    "disclosure_date",
}

_END_DATE_APIS = {
    "fina_mainbz",
    "fina_mainbz_vip",
}

_RANGE_APIS = {
    "new_share",
    "ipo",
}

_NONEMPTY_TRADING_DAY_APIS = {
    "hsgt_top10",
    "hsgt_stk_hold",
    "hsgt_cash_flow",
    "daily_basic",
    "margin_detail",
    "limit_list",
    "limit_list_d",
    "fund_daily",
    "etf_daily",
    "fut_daily",
    "cb_daily",
    "fx_daily",
    "hk_daily",
    "us_daily",
    "industry_daily",
}

# These catalog interfaces stay registered so bootstrap code can still resolve
# DDL and metadata, but the runtime scheduler should not execute them. Some are
# bootstrap-only per-symbol datasets, and others currently map to APIs that are
# not callable in this environment with the generic scheduler path.
_RUNTIME_UNSUPPORTED_INTERFACE_KEYS = {
    "pro_bar",
    "ipo",
    "digital_currency",
    "hsgt_stk_hold",
    "hsgt_cash_flow",
    "income",
    "income_vip",
    "balancesheet",
    "balancesheet_vip",
    "cashflow",
    "cashflow_vip",
    "fina_indicator",
    "fina_indicator_vip",
    "fina_audit",
    "fina_mainbz",
    "fina_mainbz_vip",
    "etf_daily",
    "etf_weight",
    "etf_nav",
    "industry_daily",
    "industry_moneyflow",
    "capital_flow",
    # Interfaces that currently require extra per-symbol/per-period inputs or
    # are not callable through the generic query path in this environment.
    "bo_monthly",
    "bo_weekly",
    "bond_holder",
    "broker_stock",
    "cn_bci",
    "cn_cpi",
    "cn_gdp",
    "cn_m2",
    "company_change",
    "cyq_chips",
    "dc_cons",
    "deposit_rate",
    "finacial_social",
    "fund_div",
    "fund_nav",
    "fund_portfolio",
    "fund_top10",
    "fut_main_settle",
    "index_dailybasic",
    "index_weight",
    "loan_rate",
    "new_bond",
    "opt_daily_s",
    "opt_greeks",
    "p_bond_basic",
    "pledge_detail",
    "stk_delisted",
    "stk_factor",
    "stk_factor_pro",
    "stk_holdernum",
    "stk_suspended",
    "tme_express",
    "top_holders",
    "top_inst",
    "top_list",
}


@dataclass(frozen=True)
class TushareCatalogSpec:
    interface_key: str
    display_name: str
    api_name: str
    target_table: str
    sync_priority: int
    requires_permission: str | None = None


class TushareCatalogInterface(BaseIngestInterface):
    def __init__(self, spec: TushareCatalogSpec):
        self._spec = spec

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key=self._spec.interface_key,
            display_name=self._spec.display_name,
            source_key="tushare",
            target_database="tushare",
            target_table=self._spec.target_table,
            sync_priority=self._spec.sync_priority,
            requires_permission=self._spec.requires_permission,
            enabled_by_default=False,
            description=f"Tushare catalog interface: {self._spec.api_name}",
        )

    def get_ddl(self) -> str:
        return ddl.get_catalog_ddl(self.info.target_table)

    def supports_scheduled_sync(self) -> bool:
        return self._spec.interface_key not in _RUNTIME_UNSUPPORTED_INTERFACE_KEYS

    def supports_backfill(self) -> bool:
        if not self.supports_scheduled_sync():
            return False
        return self._date_param() is not None or self._range_params() is not None

    def backfill_mode(self) -> str:
        if self._range_params() is not None:
            return "range"
        return "date"

    def requires_nonempty_trading_day_data(self) -> bool:
        return self._spec.api_name in _NONEMPTY_TRADING_DAY_APIS

    def sync_date(self, trade_date: date) -> SyncResult:
        params = self._build_params(trade_date, trade_date)
        return self._sync_with_params(params)

    def sync_range(self, start: date, end: date) -> SyncResult:
        range_params = self._range_params()
        if not self.supports_backfill():
            return self.sync_date(end)
        if range_params is None:
            return super().sync_range(start, end)
        return self._sync_with_params(self._build_params(start, end))

    def _date_param(self) -> str | None:
        api_name = self._spec.api_name
        if api_name in _TRADE_DATE_APIS:
            return "trade_date"
        if api_name in _ANN_DATE_APIS:
            return "ann_date"
        if api_name in _END_DATE_APIS:
            return "end_date"
        return None

    def _range_params(self) -> tuple[str, str] | None:
        if self._spec.api_name in _RANGE_APIS:
            return ("start_date", "end_date")
        return None

    def _build_params(self, start: date, end: date) -> dict[str, object]:
        params: dict[str, object] = {}
        range_params = self._range_params()
        if range_params is not None:
            start_param, end_param = range_params
            params[start_param] = start.strftime("%Y%m%d")
            params[end_param] = end.strftime("%Y%m%d")
            return params

        date_param = self._date_param()
        if date_param is not None:
            params[date_param] = end.strftime("%Y%m%d")
        return params

    def _payload_key_fields(self) -> tuple[str, ...]:
        date_param = self._date_param()
        fields = ["ts_code"]
        if date_param is not None:
            fields.append(date_param)
        fields.extend(["symbol", "code", "exchange", "market", "name"])
        return tuple(dict.fromkeys(fields))

    def _sync_with_params(self, params: dict[str, object]) -> SyncResult:
        from app.datasync.service.tushare_ingest import call_pro
        from app.domains.extdata.dao.tushare_dao import insert_catalog_rows

        try:
            df = call_pro(self._spec.api_name, **params)
            if df is None or getattr(df, "empty", True):
                return SyncResult(SyncStatus.SUCCESS, 0)
            rows = insert_catalog_rows(
                self.info.target_table,
                df,
                key_fields=self._payload_key_fields(),
            )
            return SyncResult(SyncStatus.SUCCESS, rows)
        except Exception as exc:
            return handle_tushare_sync_exception(
                logger,
                f"catalog sync for {self._spec.interface_key} via {self._spec.api_name} with {params}",
                exc,
                allow_permission_partial=True,
            )


def _normalize_permission(raw: str | None) -> str | None:
    if raw in {None, "NULL", ""}:
        return "0"

    normalized = str(raw).strip().strip("'").lower()
    if normalized == "paid":
        return "1"
    if normalized in {"1", "true", "yes", "on", "y"}:
        return "0"
    return "0"


def _build_specs() -> tuple[TushareCatalogSpec, ...]:
    return tuple(
        TushareCatalogSpec(
            interface_key=item_key,
            display_name=display_name,
            api_name=api_name,
            target_table=target_table,
            sync_priority=sync_priority,
            requires_permission=_normalize_permission(requires_permission),
        )
        for item_key, display_name, api_name, target_table, sync_priority, requires_permission in _CATALOG_ROWS
    )


def build_catalog_interfaces(existing_keys: set[str] | None = None) -> list[BaseIngestInterface]:
    seen = existing_keys or set()
    return [TushareCatalogInterface(spec) for spec in TUSHARE_CATALOG_SPECS if spec.interface_key not in seen]


_CATALOG_ROWS = [
    ('hsgt_top10', '沪深股通成份股', 'hsgt_top10', 'hsgt_top10', 100, 'NULL'),
    ('hsgt_stk_hold', '沪深股通持仓', 'hsgt_stk_hold', 'hsgt_stk_hold', 101, 'NULL'),
    ('hsgt_cash_flow', '沪深股通资金', 'hsgt_cash_flow', 'hsgt_cash_flow', 102, 'NULL'),
    ('namechange', '股票曾用名', 'namechange', 'namechange', 103, 'NULL'),
    ('new_share', 'IPO新股列表', 'new_share', 'new_share', 104, 'NULL'),
    ('ipo', '打新新股', 'ipo', 'ipo', 105, 'NULL'),
    ('stk_suspended', '股票暂停上市', 'stk_suspended', 'stk_suspended', 106, 'NULL'),
    ('stk_delisted', '股票终止上市', 'stk_delisted', 'stk_delisted', 107, 'NULL'),
    ('company_change', '公司信息变更', 'company_change', 'company_change', 108, 'NULL'),
    ('stk_holdertrade', '董监高持股', 'stk_holdertrade', 'stk_holdertrade', 109, 'NULL'),
    ('stk_holdernum', '股东人数', 'stk_holdernum', 'stk_holdernum', 110, 'NULL'),
    ('daily_basic', '每日指标数据', 'daily_basic', 'daily_basic', 31, 'NULL'),
    ('pro_bar', '通用行情接口', 'pro_bar', 'pro_bar', 33, 'NULL'),
    ('moneyflow', '个股资金流向', 'moneyflow', 'stock_moneyflow', 60, "'0'"),
    ('margin_detail', '融资融券明细', 'margin_detail', 'margin', 80, "'0'"),
    ('margin', '融资融券', 'margin', 'margin_summary', 81, 'NULL'),
    ('limit_list', '涨跌停价格', 'limit_list', 'limit_list', 35, 'NULL'),
    ('stk_limit', '涨跌停统计', 'limit_list_d', 'stk_limit', 70, "'0'"),
    ('digital_currency', '数字货币行情', 'digital_currency', 'digital_currency', 36, 'NULL'),
    ('stk_mins', '股票实时分钟', 'stk_mins', 'stk_mins', 900, "'paid'"),
    ('stk_minute', '股票历史分钟', 'stk_minute', 'stk_minute', 901, "'paid'"),
    ('income', '利润表', 'income', 'income', 56, "'1'"),
    ('income_vip', '利润表VIP', 'income_vip', 'income_vip', 200, "'1'"),
    ('balancesheet', '资产负债表', 'balancesheet', 'balancesheet', 201, 'NULL'),
    ('balancesheet_vip', '资产负债表VIP', 'balancesheet_vip', 'balancesheet_vip', 202, "'1'"),
    ('cashflow', '现金流量表', 'cashflow', 'cashflow', 203, 'NULL'),
    ('cashflow_vip', '现金流量表VIP', 'cashflow_vip', 'cashflow_vip', 204, "'1'"),
    ('forecast', '业绩预告', 'forecast', 'forecast', 205, 'NULL'),
    ('express', '业绩快报', 'express', 'express', 206, 'NULL'),
    ('fina_indicator', '财务指标数据', 'fina_indicator', 'fina_indicator', 55, "'0'"),
    ('fina_indicator_vip', '财务指标VIP', 'fina_indicator_vip', 'fina_indicator_vip', 207, "'1'"),
    ('fina_audit', '财务审计意见', 'fina_audit', 'fina_audit', 208, 'NULL'),
    ('fina_mainbz', '主营业务构成', 'fina_mainbz', 'fina_mainbz', 209, 'NULL'),
    ('fina_mainbz_vip', '主营业务VIP', 'fina_mainbz_vip', 'fina_mainbz_vip', 210, "'1'"),
    ('disclosure_date', '财报披露计划', 'disclosure_date', 'disclosure_date', 211, 'NULL'),
    ('top_list', '龙虎榜每日明细', 'top_list', 'top_list', 300, 'NULL'),
    ('top_inst', '龙虎榜机构交易', 'top_inst', 'top_inst', 301, "'1'"),
    ('pledge_detail', '股权质押明细', 'pledge_detail', 'pledge_detail', 302, 'NULL'),
    ('pledge_stat', '股权质押统计', 'pledge_stat', 'pledge_stat', 303, 'NULL'),
    ('repurchase', '股票回购', 'repurchase', 'repurchase', 304, 'NULL'),
    ('share_float', '限售股解禁', 'share_float', 'share_float', 305, 'NULL'),
    ('block_trade', '大宗交易', 'block_trade', 'block_trade', 90, "'0'"),
    ('top_holders', '龙虎榜粉丝详情', 'top_holders', 'top_holders', 306, 'NULL'),
    ('stk_factor_pro', '股票技术面因子', 'stk_factor_pro', 'stk_factor_pro', 307, "'1'"),
    ('stk_factor', '技术因子专业版', 'stk_factor', 'stk_factor', 308, 'NULL'),
    ('cyq_perf', '每日筹码及胜率', 'cyq_perf', 'cyq_perf', 309, "'1'"),
    ('cyq_chips', '筹码分布', 'cyq_chips', 'cyq_chips', 310, "'1'"),
    ('kpl_list', '开盘啦榜单', 'kpl_list', 'kpl_list', 311, "'1'"),
    ('dc_hot', '东方财富热榜', 'dc_hot', 'dc_hot', 312, "'1'"),
    ('dc_member', '东方财富板块成分', 'dc_member', 'dc_member', 313, "'1'"),
    ('dc_cons', '东方财富概念', 'dc_cons', 'dc_cons', 314, 'NULL'),
    ('report_rc', '盈利预测数据', 'report_rc', 'report_rc', 315, 'NULL'),
    ('rt_daily', '实时涨跌幅', 'rt_daily', 'rt_daily', 902, "'paid'"),
    ('index_basic', '指数基本信息', 'index_basic', 'index_basic', 400, 'NULL'),
    ('index_monthly', '指数月线行情', 'index_monthly', 'index_monthly', 401, 'NULL'),
    ('index_weight', '指数成分和权重', 'index_weight', 'index_weight', 402, 'NULL'),
    ('index_dailybasic', '大盘指数每日指标', 'index_dailybasic', 'index_dailybasic', 403, 'NULL'),
    ('index_classify', '申万行业分类', 'index_classify', 'index_classify', 404, 'NULL'),
    ('index_member_all', '申万行业成分', 'index_member_all', 'index_member_all', 405, 'NULL'),
    ('index_global', '国际指数', 'index_global', 'index_global', 406, "'1'"),
    ('rt_idx_daily', '指数实时行情', 'rt_idx_daily', 'rt_idx_daily', 903, "'paid'"),
    ('rt_sw_daily', '申万指数实时行情', 'rt_sw_daily', 'rt_sw_daily', 904, "'paid'"),
    ('fund_basic_etf', 'ETF基础信息', 'fund_basic', 'fund_basic_etf', 500, 'NULL'),
    ('fund_share', 'ETF基金规模', 'fund_share', 'fund_share', 501, 'NULL'),
    ('fund_daily', '场内基金日线行情', 'fund_daily', 'fund_daily', 502, 'NULL'),
    ('fund_adj', 'ETF复权因子', 'fund_adj', 'fund_adj', 503, "'1'"),
    ('rt_etf_k', 'ETF实时日线', 'rt_etf_k', 'rt_etf_k', 905, "'paid'"),
    ('etf_daily', 'ETF行情', 'etf_daily', 'etf_daily', 504, 'NULL'),
    ('etf_weight', 'ETF权重', 'etf_weight', 'etf_weight', 505, 'NULL'),
    ('etf_nav', 'ETF收益', 'etf_nav', 'etf_nav', 506, 'NULL'),
    ('fund_basic', '公募基金列表', 'fund_basic', 'fund_basic', 510, 'NULL'),
    ('fund_company', '公募基金公司', 'fund_company', 'fund_company', 511, 'NULL'),
    ('fund_nav', '公募基金净值', 'fund_nav', 'fund_nav', 512, 'NULL'),
    ('fund_daily_pub', '场内基金日线', 'fund_daily', 'fund_daily_pub', 513, 'NULL'),
    ('fund_div', '公募基金分红', 'fund_div', 'fund_div', 514, 'NULL'),
    ('fund_portfolio', '公募基金持仓', 'fund_portfolio', 'fund_portfolio', 515, 'NULL'),
    ('fund_top10', '基金重仓', 'fund_top10', 'fund_top10', 516, 'NULL'),
    ('fund_share_pub', '基金规模', 'fund_share', 'fund_share_pub', 517, 'NULL'),
    ('fut_basic', '期货合约列表', 'fut_basic', 'fut_basic', 600, 'NULL'),
    ('fut_trade_cal', '期货交易日历', 'trade_cal', 'fut_trade_cal', 601, 'NULL'),
    ('fut_daily', '期货日线行情', 'fut_daily', 'fut_daily', 602, 'NULL'),
    ('fut_holding', '每日成交持仓排名', 'fut_holding', 'fut_holding', 603, 'NULL'),
    ('fut_wsr', '仓单日报', 'fut_wsr', 'fut_wsr', 604, 'NULL'),
    ('fut_settle', '结算参数', 'fut_settle', 'fut_settle', 605, 'NULL'),
    ('fut_main_settle', '期货主力合约', 'fut_main_settle', 'fut_main_settle', 606, 'NULL'),
    ('fut_mapping', '合约交叉引用', 'fut_mapping', 'fut_mapping', 607, 'NULL'),
    ('ft_mins', '期货历史分钟', 'ft_mins', 'ft_mins', 906, "'paid'"),
    ('ft_mins_rt', '期货实时分钟', 'ft_mins_rt', 'ft_mins_rt', 907, "'paid'"),
    ('opt_basic', '期权合约列表', 'opt_basic', 'opt_basic', 700, 'NULL'),
    ('opt_daily', '期权日线行情', 'opt_daily', 'opt_daily', 701, "'1'"),
    ('opt_daily_s', '期权优选行情', 'opt_daily_s', 'opt_daily_s', 702, "'1'"),
    ('rt_opt_daily', '期权实时行情', 'rt_opt_daily', 'rt_opt_daily', 908, "'paid'"),
    ('opt_mins', '期权历史分钟', 'opt_mins', 'opt_mins', 909, "'paid'"),
    ('opt_greeks', '期权价格调整', 'opt_greeks', 'opt_greeks', 703, "'1'"),
    ('cb_basic', '可转债基础信息', 'cb_basic', 'cb_basic', 750, 'NULL'),
    ('cb_issue', '可转债发行数据', 'cb_issue', 'cb_issue', 751, 'NULL'),
    ('cb_daily', '可转债日线数据', 'cb_daily', 'cb_daily', 752, 'NULL'),
    ('cb_share', '可转债待发', 'cb_share', 'cb_share', 753, 'NULL'),
    ('new_bond', '债券待发行', 'new_bond', 'new_bond', 754, 'NULL'),
    ('p_bond_basic', '债券发行', 'p_bond_basic', 'p_bond_basic', 755, 'NULL'),
    ('bond_holder', '债券持有人', 'bond_holder', 'bond_holder', 756, 'NULL'),
    ('fx_obasic', '外汇基础信息', 'fx_obasic', 'fx_obasic', 770, 'NULL'),
    ('fx_daily', '外汇日线行情', 'fx_daily', 'fx_daily', 771, 'NULL'),
    ('hk_basic', '港股列表', 'hk_basic', 'hk_basic', 780, 'NULL'),
    ('hk_daily', '港股日线', 'hk_daily', 'hk_daily', 910, "'paid'"),
    ('hk_mins', '港股分钟', 'hk_mins', 'hk_mins', 911, "'paid'"),
    ('rt_hk_adj', '港股复权行情', 'rt_hk_adj', 'rt_hk_adj', 912, "'paid'"),
    ('rt_hk_k', '港股实时日线', 'rt_hk_k', 'rt_hk_k', 913, "'paid'"),
    ('hk_hold', '港股通', 'hk_hold', 'hk_hold', 914, "'paid'"),
    ('hk_hold_detail', '港股通持仓', 'hk_hold_detail', 'hk_hold_detail', 915, "'paid'"),
    ('us_basic', '美股列表', 'us_basic', 'us_basic', 800, 'NULL'),
    ('us_daily', '美股日线', 'us_daily', 'us_daily', 801, 'NULL'),
    ('us_adj', '美股复权行情', 'us_adj', 'us_adj', 916, "'paid'"),
    ('us_factor', '美股因子', 'us_factor', 'us_factor', 917, "'paid'"),
    ('finance_hk', '港股财报', 'finance_hk', 'finance_hk', 918, "'paid'"),
    ('bo_monthly', '电影月度票房', 'bo_monthly', 'bo_monthly', 820, 'NULL'),
    ('bo_weekly', '电影周票房', 'bo_weekly', 'bo_weekly', 821, 'NULL'),
    ('tme_express', '台湾电子产业月营收', 'tme_express', 'tme_express', 822, 'NULL'),
    ('industry_daily', '行业每日交易', 'industry_daily', 'industry_daily', 823, 'NULL'),
    ('industry_moneyflow', '行业资金流', 'industry_moneyflow', 'industry_moneyflow', 824, 'NULL'),
    ('daily_info', '市场交易统计', 'daily_info', 'daily_info', 825, 'NULL'),
    ('capital_flow', '资金流向', 'capital_flow', 'capital_flow', 826, 'NULL'),
    ('broker_stock', '券商金股', 'broker_stock', 'broker_stock', 827, "'1'"),
    ('shibor_lpr', 'LPR贷款基础利率', 'shibor_lpr', 'shibor_lpr', 840, 'NULL'),
    ('shibor', '银行间拆借', 'shibor', 'shibor', 841, 'NULL'),
    ('cn_cpi', '宏观通胀数据', 'cpi', 'cn_cpi', 842, 'NULL'),
    ('cn_gdp', '国内生产总值', 'gdp', 'cn_gdp', 843, 'NULL'),
    ('cn_m2', '广义货币M2', 'cn_m2', 'cn_m2', 844, 'NULL'),
    ('deposit_rate', '存款利率', 'deposit_rate', 'deposit_rate', 845, 'NULL'),
    ('loan_rate', '贷款利率', 'loan_rate', 'loan_rate', 846, 'NULL'),
    ('cn_bci', '企业景气指数', 'bci', 'cn_bci', 847, 'NULL'),
    ('concept_corpus', '概念股语料', 'concept_corpus', 'concept_corpus', 919, "'paid'"),
    ('stock_corpus', '股吧评论', 'stock_corpus', 'stock_corpus', 920, "'paid'"),
    ('ann_corpus', '公告摘要', 'ann_corpus', 'ann_corpus', 921, "'paid'"),
    ('report_corpus', '研报语料', 'report_corpus', 'report_corpus', 922, "'paid'"),
    ('news', '新闻快讯', 'news', 'news', 923, "'paid'"),
    ('major_news', '新闻通讯', 'major_news', 'major_news', 924, "'paid'"),
    ('cctv_news', '新闻联播', 'cctv_news', 'cctv_news', 925, "'paid'"),
    ('announcements', '股票公告', 'announcements', 'announcements', 926, "'paid'"),
    ('fnd_announcement', '基金公告', 'fnd_announcement', 'fnd_announcement', 927, "'paid'"),
    ('irm_qa_sh', '上证e互动', 'irm_qa_sh', 'irm_qa_sh', 928, "'paid'"),
    ('irm_qa_sz', '深证互动易', 'irm_qa_sz', 'irm_qa_sz', 929, "'paid'"),
    ('sentiment', '舆情监控', 'sentiment', 'sentiment', 930, "'paid'"),
    ('policy', '政策法规库', 'policy', 'policy', 931, "'paid'"),
    ('research', '券商研报', 'research', 'research', 932, "'paid'"),
    ('finacial_social', '社融数据', 'finacial_social', 'finacial_social', 848, 'NULL'),
]

TUSHARE_CATALOG_SPECS = _build_specs()
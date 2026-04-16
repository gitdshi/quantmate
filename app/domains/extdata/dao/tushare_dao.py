"""DAO helpers for Tushare DB operations used by datasync services."""

import logging
import pandas as pd
import numpy as np
import json
from sqlalchemy import text
from app.infrastructure.db.connections import get_tushare_engine

logger = logging.getLogger(__name__)

engine = get_tushare_engine()


def audit_start(api_name: str, params: dict) -> int:
    with engine.begin() as conn:
        res = conn.execute(
            text(
                "INSERT INTO ingest_audit (api_name, params, status, fetched_rows) VALUES (:api, :params, 'running', 0)"
            ),
            {"api": api_name, "params": json.dumps(params)},
        )
        try:
            return int(res.lastrowid)
        except Exception:
            return 0


def audit_finish(audit_id: int, status: str, rows: int):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE ingest_audit SET status=:status, fetched_rows=:rows, finished_at=NOW() WHERE id=:id"),
            {"status": status, "rows": rows, "id": audit_id},
        )


def upsert_daily(df: pd.DataFrame) -> int:
    """Bulk upsert stock_daily rows from a DataFrame. Returns number of rows processed."""
    if df is None or df.empty:
        return 0
    count = 0
    insert_sql = (
        "INSERT INTO stock_daily (ts_code, trade_date, open, high, low, close, pre_close, change_amount, pct_change, vol, amount)"
        " VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :pre_close, :change_amount, :pct_change, :vol, :amount)"
        " ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), pre_close=VALUES(pre_close), change_amount=VALUES(change_amount), pct_change=VALUES(pct_change), vol=VALUES(vol), amount=VALUES(amount)"
    )

    def clean(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
        return v

    def round2(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        try:
            return round(float(v), 2)
        except Exception:
            return v

    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            params = {
                "ts_code": clean(r.get("ts_code")),
                "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                "open": round2(clean(r.get("open"))),
                "high": round2(clean(r.get("high"))),
                "low": round2(clean(r.get("low"))),
                "close": round2(clean(r.get("close"))),
                "pre_close": round2(clean(r.get("pre_close"))),
                "change_amount": round2(clean(r.get("change") or r.get("change_amount"))),
                "pct_change": round2(clean(r.get("pct_chg") or r.get("pct_change"))),
                "vol": (int(r.get("vol")) if (r.get("vol") is not None and not pd.isna(r.get("vol"))) else None),
                "amount": round2(clean(r.get("amount"))),
            }
            conn.execute(text(insert_sql), params)
            count += 1
    return count


def upsert_bak_daily(df: pd.DataFrame) -> int:
    """Bulk upsert bak_daily rows from a DataFrame."""
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO bak_daily ("
        "ts_code, trade_date, name, pct_change, close, change_amount, open, high, low, pre_close, vol_ratio, "
        "turn_over, swing, vol, amount, selling, buying, total_share, float_share, pe, industry, area, "
        "float_mv, total_mv, avg_price, strength, activity, avg_turnover, attack, interval_3, interval_6"
        ") VALUES ("
        ":ts_code, :trade_date, :name, :pct_change, :close, :change_amount, :open, :high, :low, :pre_close, :vol_ratio, "
        ":turn_over, :swing, :vol, :amount, :selling, :buying, :total_share, :float_share, :pe, :industry, :area, "
        ":float_mv, :total_mv, :avg_price, :strength, :activity, :avg_turnover, :attack, :interval_3, :interval_6"
        ") ON DUPLICATE KEY UPDATE "
        "name=VALUES(name), pct_change=VALUES(pct_change), close=VALUES(close), change_amount=VALUES(change_amount), "
        "open=VALUES(open), high=VALUES(high), low=VALUES(low), pre_close=VALUES(pre_close), vol_ratio=VALUES(vol_ratio), "
        "turn_over=VALUES(turn_over), swing=VALUES(swing), vol=VALUES(vol), amount=VALUES(amount), selling=VALUES(selling), "
        "buying=VALUES(buying), total_share=VALUES(total_share), float_share=VALUES(float_share), pe=VALUES(pe), "
        "industry=VALUES(industry), area=VALUES(area), float_mv=VALUES(float_mv), total_mv=VALUES(total_mv), "
        "avg_price=VALUES(avg_price), strength=VALUES(strength), activity=VALUES(activity), avg_turnover=VALUES(avg_turnover), "
        "attack=VALUES(attack), interval_3=VALUES(interval_3), interval_6=VALUES(interval_6)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "name": _clean(r.get("name")),
                    "pct_change": _round2(_clean(r.get("pct_change") or r.get("pct_chg"))),
                    "close": _round2(_clean(r.get("close"))),
                    "change_amount": _round2(_clean(r.get("change") or r.get("change_amount"))),
                    "open": _round2(_clean(r.get("open"))),
                    "high": _round2(_clean(r.get("high"))),
                    "low": _round2(_clean(r.get("low"))),
                    "pre_close": _round2(_clean(r.get("pre_close"))),
                    "vol_ratio": _round2(_clean(r.get("vol_ratio"))),
                    "turn_over": _round2(_clean(r.get("turn_over"))),
                    "swing": _round2(_clean(r.get("swing"))),
                    "vol": (int(r.get("vol")) if (r.get("vol") is not None and not pd.isna(r.get("vol"))) else None),
                    "amount": _round2(_clean(r.get("amount"))),
                    "selling": (int(r.get("selling")) if (r.get("selling") is not None and not pd.isna(r.get("selling"))) else None),
                    "buying": (int(r.get("buying")) if (r.get("buying") is not None and not pd.isna(r.get("buying"))) else None),
                    "total_share": _round2(_clean(r.get("total_share"))),
                    "float_share": _round2(_clean(r.get("float_share"))),
                    "pe": _round2(_clean(r.get("pe"))),
                    "industry": _clean(r.get("industry")),
                    "area": _clean(r.get("area")),
                    "float_mv": _round2(_clean(r.get("float_mv"))),
                    "total_mv": _round2(_clean(r.get("total_mv"))),
                    "avg_price": _round2(_clean(r.get("avg_price"))),
                    "strength": _round2(_clean(r.get("strength"))),
                    "activity": (int(r.get("activity")) if (r.get("activity") is not None and not pd.isna(r.get("activity"))) else None),
                    "avg_turnover": _round2(_clean(r.get("avg_turnover"))),
                    "attack": _round2(_clean(r.get("attack"))),
                    "interval_3": _round2(_clean(r.get("interval_3"))),
                    "interval_6": _round2(_clean(r.get("interval_6"))),
                },
            )
            rows += 1
    return rows


def upsert_suspend_d(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO suspend_d (ts_code, trade_date, suspend_timing, suspend_type) "
        "VALUES (:ts_code, :trade_date, :suspend_timing, :suspend_type) "
        "ON DUPLICATE KEY UPDATE suspend_timing=VALUES(suspend_timing), suspend_type=VALUES(suspend_type)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "suspend_timing": _clean(r.get("suspend_timing")),
                    "suspend_type": _clean(r.get("suspend_type")),
                },
            )
            rows += 1
    return rows


def upsert_suspend(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO `suspend` (ts_code, suspend_date, resume_date, suspend_reason) "
        "VALUES (:ts_code, :suspend_date, :resume_date, :suspend_reason) "
        "ON DUPLICATE KEY UPDATE resume_date=VALUES(resume_date), suspend_reason=VALUES(suspend_reason)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "suspend_date": (pd.to_datetime(r.get("suspend_date")).date() if r.get("suspend_date") else None),
                    "resume_date": (pd.to_datetime(r.get("resume_date")).date() if r.get("resume_date") else None),
                    "suspend_reason": _clean(r.get("suspend_reason")),
                },
            )
            rows += 1
    return rows


def upsert_stock_company(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_company ("
        "ts_code, com_name, com_id, exchange, chairman, manager, secretary, reg_capital, setup_date, province, city, "
        "introduction, website, email, office, employees, main_business, business_scope"
        ") VALUES ("
        ":ts_code, :com_name, :com_id, :exchange, :chairman, :manager, :secretary, :reg_capital, :setup_date, :province, :city, "
        ":introduction, :website, :email, :office, :employees, :main_business, :business_scope"
        ") ON DUPLICATE KEY UPDATE "
        "com_name=VALUES(com_name), com_id=VALUES(com_id), exchange=VALUES(exchange), chairman=VALUES(chairman), "
        "manager=VALUES(manager), secretary=VALUES(secretary), reg_capital=VALUES(reg_capital), setup_date=VALUES(setup_date), "
        "province=VALUES(province), city=VALUES(city), introduction=VALUES(introduction), website=VALUES(website), "
        "email=VALUES(email), office=VALUES(office), employees=VALUES(employees), main_business=VALUES(main_business), "
        "business_scope=VALUES(business_scope)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "com_name": _clean(r.get("com_name")),
                    "com_id": _clean(r.get("com_id")),
                    "exchange": _clean(r.get("exchange")),
                    "chairman": _clean(r.get("chairman")),
                    "manager": _clean(r.get("manager")),
                    "secretary": _clean(r.get("secretary")),
                    "reg_capital": _clean(r.get("reg_capital")),
                    "setup_date": _to_date_value(r.get("setup_date")),
                    "province": _clean(r.get("province")),
                    "city": _clean(r.get("city")),
                    "introduction": _clean(r.get("introduction")),
                    "website": _clean(r.get("website")),
                    "email": _clean(r.get("email")),
                    "office": _clean(r.get("office")),
                    "employees": _int_value(r.get("employees")),
                    "main_business": _clean(r.get("main_business")),
                    "business_scope": _clean(r.get("business_scope")),
                },
            )
            rows += 1
    return rows


def upsert_new_share(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO new_share ("
        "ts_code, sub_code, name, ipo_date, issue_date, market_amount, issue_price, pe, limit_amount, funds, ballot, amount, market"
        ") VALUES ("
        ":ts_code, :sub_code, :name, :ipo_date, :issue_date, :market_amount, :issue_price, :pe, :limit_amount, :funds, :ballot, :amount, :market"
        ") ON DUPLICATE KEY UPDATE sub_code=VALUES(sub_code), name=VALUES(name), issue_date=VALUES(issue_date), "
        "market_amount=VALUES(market_amount), issue_price=VALUES(issue_price), pe=VALUES(pe), "
        "limit_amount=VALUES(limit_amount), funds=VALUES(funds), ballot=VALUES(ballot), amount=VALUES(amount), market=VALUES(market)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "sub_code": _clean(r.get("sub_code")),
                    "name": _clean(r.get("name")),
                    "ipo_date": _to_date_value(r.get("ipo_date")),
                    "issue_date": _to_date_value(r.get("issue_date")),
                    "market_amount": _clean(r.get("market_amount")),
                    "issue_price": _clean(r.get("price") or r.get("issue_price")),
                    "pe": _clean(r.get("pe")),
                    "limit_amount": _clean(r.get("limit_amount")),
                    "funds": _clean(r.get("funds")),
                    "ballot": _clean(r.get("ballot")),
                    "amount": _int_value(r.get("amount")),
                    "market": _clean(r.get("market") or r.get("sub_code")),
                },
            )
            rows += 1
    return rows


def upsert_fina_indicator(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO fina_indicator ("
        "ts_code, ann_date, end_date, eps, dt_eps, total_revenue_ps, revenue_ps, capital_rese_ps, surplus_rese_ps, "
        "undist_profit_ps, extra_item, profit_dedt, gross_margin, current_ratio, quick_ratio, cash_ratio, ar_turn, ca_turn, "
        "fa_turn, assets_turn, op_income, ebit, ebitda, fcff, fcfe, roe, roe_waa, roe_dt, roa, npta, debt_to_assets, "
        "netprofit_yoy, or_yoy, roe_yoy"
        ") VALUES ("
        ":ts_code, :ann_date, :end_date, :eps, :dt_eps, :total_revenue_ps, :revenue_ps, :capital_rese_ps, :surplus_rese_ps, "
        ":undist_profit_ps, :extra_item, :profit_dedt, :gross_margin, :current_ratio, :quick_ratio, :cash_ratio, :ar_turn, :ca_turn, "
        ":fa_turn, :assets_turn, :op_income, :ebit, :ebitda, :fcff, :fcfe, :roe, :roe_waa, :roe_dt, :roa, :npta, :debt_to_assets, "
        ":netprofit_yoy, :or_yoy, :roe_yoy"
        ") ON DUPLICATE KEY UPDATE ann_date=VALUES(ann_date), eps=VALUES(eps), dt_eps=VALUES(dt_eps), "
        "total_revenue_ps=VALUES(total_revenue_ps), revenue_ps=VALUES(revenue_ps), capital_rese_ps=VALUES(capital_rese_ps), "
        "surplus_rese_ps=VALUES(surplus_rese_ps), undist_profit_ps=VALUES(undist_profit_ps), extra_item=VALUES(extra_item), "
        "profit_dedt=VALUES(profit_dedt), gross_margin=VALUES(gross_margin), current_ratio=VALUES(current_ratio), "
        "quick_ratio=VALUES(quick_ratio), cash_ratio=VALUES(cash_ratio), ar_turn=VALUES(ar_turn), ca_turn=VALUES(ca_turn), "
        "fa_turn=VALUES(fa_turn), assets_turn=VALUES(assets_turn), op_income=VALUES(op_income), ebit=VALUES(ebit), ebitda=VALUES(ebitda), "
        "fcff=VALUES(fcff), fcfe=VALUES(fcfe), roe=VALUES(roe), roe_waa=VALUES(roe_waa), roe_dt=VALUES(roe_dt), roa=VALUES(roa), "
        "npta=VALUES(npta), debt_to_assets=VALUES(debt_to_assets), netprofit_yoy=VALUES(netprofit_yoy), or_yoy=VALUES(or_yoy), "
        "roe_yoy=VALUES(roe_yoy)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "ann_date": _to_date_value(r.get("ann_date")),
                    "end_date": _to_date_value(r.get("end_date")),
                    "eps": _clean(r.get("eps")),
                    "dt_eps": _clean(r.get("dt_eps")),
                    "total_revenue_ps": _clean(r.get("total_revenue_ps")),
                    "revenue_ps": _clean(r.get("revenue_ps")),
                    "capital_rese_ps": _clean(r.get("capital_rese_ps")),
                    "surplus_rese_ps": _clean(r.get("surplus_rese_ps")),
                    "undist_profit_ps": _clean(r.get("undist_profit_ps")),
                    "extra_item": _clean(r.get("extra_item")),
                    "profit_dedt": _clean(r.get("profit_dedt")),
                    "gross_margin": _clean(r.get("gross_margin")),
                    "current_ratio": _clean(r.get("current_ratio")),
                    "quick_ratio": _clean(r.get("quick_ratio")),
                    "cash_ratio": _clean(r.get("cash_ratio")),
                    "ar_turn": _clean(r.get("ar_turn")),
                    "ca_turn": _clean(r.get("ca_turn")),
                    "fa_turn": _clean(r.get("fa_turn")),
                    "assets_turn": _clean(r.get("assets_turn")),
                    "op_income": _clean(r.get("op_income")),
                    "ebit": _clean(r.get("ebit")),
                    "ebitda": _clean(r.get("ebitda")),
                    "fcff": _clean(r.get("fcff")),
                    "fcfe": _clean(r.get("fcfe")),
                    "roe": _clean(r.get("roe")),
                    "roe_waa": _clean(r.get("roe_waa")),
                    "roe_dt": _clean(r.get("roe_dt")),
                    "roa": _clean(r.get("roa")),
                    "npta": _clean(r.get("npta")),
                    "debt_to_assets": _clean(r.get("debt_to_assets")),
                    "netprofit_yoy": _clean(r.get("netprofit_yoy")),
                    "or_yoy": _clean(r.get("or_yoy")),
                    "roe_yoy": _clean(r.get("roe_yoy")),
                },
            )
            rows += 1
    return rows


def upsert_income(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO income ("
        "ts_code, ann_date, f_ann_date, end_date, report_type, comp_type, total_revenue, revenue, total_cogs, oper_cost, "
        "sell_exp, admin_exp, fin_exp, operate_profit, total_profit, income_tax, n_income, n_income_attr_p, minority_gain, basic_eps, diluted_eps"
        ") VALUES ("
        ":ts_code, :ann_date, :f_ann_date, :end_date, :report_type, :comp_type, :total_revenue, :revenue, :total_cogs, :oper_cost, "
        ":sell_exp, :admin_exp, :fin_exp, :operate_profit, :total_profit, :income_tax, :n_income, :n_income_attr_p, :minority_gain, :basic_eps, :diluted_eps"
        ") ON DUPLICATE KEY UPDATE ann_date=VALUES(ann_date), f_ann_date=VALUES(f_ann_date), report_type=VALUES(report_type), "
        "comp_type=VALUES(comp_type), total_revenue=VALUES(total_revenue), revenue=VALUES(revenue), total_cogs=VALUES(total_cogs), "
        "oper_cost=VALUES(oper_cost), sell_exp=VALUES(sell_exp), admin_exp=VALUES(admin_exp), fin_exp=VALUES(fin_exp), "
        "operate_profit=VALUES(operate_profit), total_profit=VALUES(total_profit), income_tax=VALUES(income_tax), "
        "n_income=VALUES(n_income), n_income_attr_p=VALUES(n_income_attr_p), minority_gain=VALUES(minority_gain), "
        "basic_eps=VALUES(basic_eps), diluted_eps=VALUES(diluted_eps)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "ann_date": _to_date_value(r.get("ann_date")),
                    "f_ann_date": _to_date_value(r.get("f_ann_date")),
                    "end_date": _to_date_value(r.get("end_date")),
                    "report_type": _clean(r.get("report_type")),
                    "comp_type": _clean(r.get("comp_type")),
                    "total_revenue": _clean(r.get("total_revenue")),
                    "revenue": _clean(r.get("revenue")),
                    "total_cogs": _clean(r.get("total_cogs")),
                    "oper_cost": _clean(r.get("oper_cost")),
                    "sell_exp": _clean(r.get("sell_exp")),
                    "admin_exp": _clean(r.get("admin_exp")),
                    "fin_exp": _clean(r.get("fin_exp")),
                    "operate_profit": _clean(r.get("operate_profit")),
                    "total_profit": _clean(r.get("total_profit")),
                    "income_tax": _clean(r.get("income_tax")),
                    "n_income": _clean(r.get("n_income")),
                    "n_income_attr_p": _clean(r.get("n_income_attr_p")),
                    "minority_gain": _clean(r.get("minority_gain")),
                    "basic_eps": _clean(r.get("basic_eps")),
                    "diluted_eps": _clean(r.get("diluted_eps")),
                },
            )
            rows += 1
    return rows


def _upsert_statement_payload(table_name: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        f"INSERT INTO `{table_name}` (ts_code, ann_date, f_ann_date, end_date, report_type, comp_type, data) "
        f"VALUES (:ts_code, :ann_date, :f_ann_date, :end_date, :report_type, :comp_type, :data) "
        f"ON DUPLICATE KEY UPDATE ann_date=VALUES(ann_date), f_ann_date=VALUES(f_ann_date), report_type=VALUES(report_type), "
        f"comp_type=VALUES(comp_type), data=VALUES(data)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": _clean(r.get("ts_code")),
                    "ann_date": _to_date_value(r.get("ann_date")),
                    "f_ann_date": _to_date_value(r.get("f_ann_date")),
                    "end_date": _to_date_value(r.get("end_date")),
                    "report_type": _clean(r.get("report_type")),
                    "comp_type": _clean(r.get("comp_type")),
                    "data": json.dumps(_json_safe(r), default=str, ensure_ascii=False, allow_nan=False),
                },
            )
            rows += 1
    return rows


def upsert_balancesheet(df: pd.DataFrame) -> int:
    return _upsert_statement_payload("balancesheet", df)


def upsert_cashflow(df: pd.DataFrame) -> int:
    return _upsert_statement_payload("cashflow", df)


def upsert_index_daily_df(df: pd.DataFrame) -> int:
    """Upsert index_daily rows from Tushare daily index API DataFrame."""
    if df is None or df.empty:
        return 0
    insert_sql = (
        "INSERT INTO index_daily (index_code, trade_date, open, high, low, close, vol, amount) "
        "VALUES (:index_code, :trade_date, :open, :high, :low, :close, :vol, :amount) "
        "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), "
        "close=VALUES(close), vol=VALUES(vol), amount=VALUES(amount)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                text(insert_sql),
                {
                    "index_code": r.get("ts_code") or r.get("index_code"),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "open": None if pd.isna(r.get("open")) else float(r.get("open")),
                    "high": None if pd.isna(r.get("high")) else float(r.get("high")),
                    "low": None if pd.isna(r.get("low")) else float(r.get("low")),
                    "close": None if pd.isna(r.get("close")) else float(r.get("close")),
                    "vol": (int(r.get("vol")) if (r.get("vol") is not None and not pd.isna(r.get("vol"))) else None),
                    "amount": None if pd.isna(r.get("amount")) else float(r.get("amount")),
                },
            )
            rows += 1
    return rows


def get_all_ts_codes() -> list:
    """Return all ts_code values from stock_basic ordered."""
    with engine.connect() as conn:
        res = conn.execute(text("SELECT ts_code FROM stock_basic ORDER BY ts_code"))
        return [r[0] for r in res.fetchall()]


def get_max_trade_date(ts_code: str):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT MAX(trade_date) FROM stock_daily WHERE ts_code=:ts"), {"ts": ts_code})
        row = res.fetchone()
        return row[0] if row is not None else None


def upsert_dividend_df(df: pd.DataFrame) -> int:
    """Upsert rows from a dividend DataFrame into stock_dividend."""
    if df is None or df.empty:
        return 0

    def _to_date_or_none(value):
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if value is None:
            return None
        try:
            return pd.to_datetime(value).date()
        except Exception:
            return None

    insert_sql = text(
        "INSERT INTO stock_dividend (ts_code, ann_date, imp_ann_date, record_date, ex_date, pay_date, div_cash, div_stock, bonus_ratio)"
        " VALUES (:ts_code, :ann_date, :imp_ann_date, :record_date, :ex_date, :pay_date, :div_cash, :div_stock, :bonus_ratio)"
        " ON DUPLICATE KEY UPDATE div_cash=VALUES(div_cash), div_stock=VALUES(div_stock), bonus_ratio=VALUES(bonus_ratio)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "ann_date": _to_date_or_none(r.get("ann_date")),
                    "imp_ann_date": _to_date_or_none(r.get("imp_ann_date")),
                    "record_date": _to_date_or_none(r.get("record_date")),
                    "ex_date": _to_date_or_none(r.get("ex_date")),
                    "pay_date": _to_date_or_none(r.get("pay_date")),
                    "div_cash": None if pd.isna(r.get("div_cash")) else float(r.get("div_cash")),
                    "div_stock": None if pd.isna(r.get("div_stock")) else float(r.get("div_stock")),
                    "bonus_ratio": None if pd.isna(r.get("bonus_ratio")) else float(r.get("bonus_ratio")),
                },
            )
            rows += 1
    return rows


def upsert_financial_statement(df: pd.DataFrame, statement_type: str) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO financial_statement (ts_code, statement_type, ann_date, end_date, report_date, data) VALUES (:ts_code, :statement_type, :ann_date, :end_date, :report_date, :data)"
    )
    count = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            ann_date = r.get("ann_date")
            end_date = r.get("end_date") or r.get("period")
            report_date = r.get("f_ann_date") or r.get("report_date")
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "statement_type": statement_type,
                    "ann_date": ann_date,
                    "end_date": end_date,
                    "report_date": report_date,
                    "data": json.dumps(r, default=str),
                },
            )
            count += 1
    return count


def upsert_daily_basic(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO daily_basic (ts_code, trade_date, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv)"
        " VALUES (:ts_code, :trade_date, :turnover_rate, :turnover_rate_f, :volume_ratio, :pe, :pe_ttm, :pb, :ps, :ps_ttm, :total_mv, :circ_mv)"
        " ON DUPLICATE KEY UPDATE turnover_rate=VALUES(turnover_rate), turnover_rate_f=VALUES(turnover_rate_f), volume_ratio=VALUES(volume_ratio), pe=VALUES(pe), pe_ttm=VALUES(pe_ttm), pb=VALUES(pb), ps=VALUES(ps), ps_ttm=VALUES(ps_ttm), total_mv=VALUES(total_mv), circ_mv=VALUES(circ_mv)"
    )

    def clean(v):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        return v

    def round2(v):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        try:
            return round(float(v), 2)
        except Exception:
            return v

    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": clean(r.get("ts_code")),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "turnover_rate": round2(clean(r.get("turnover_rate"))),
                    "turnover_rate_f": round2(clean(r.get("turnover_rate_f"))),
                    "volume_ratio": round2(clean(r.get("volume_ratio"))),
                    "pe": round2(clean(r.get("pe"))),
                    "pe_ttm": round2(clean(r.get("pe_ttm"))),
                    "pb": round2(clean(r.get("pb"))),
                    "ps": round2(clean(r.get("ps"))),
                    "ps_ttm": round2(clean(r.get("ps_ttm"))),
                    "total_mv": round2(clean(r.get("total_mv"))),
                    "circ_mv": round2(clean(r.get("circ_mv"))),
                },
            )
            rows += 1
    return rows


def upsert_adj_factor(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (:ts_code, :trade_date, :adj_factor) ON DUPLICATE KEY UPDATE adj_factor=VALUES(adj_factor)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            adj = r.get("adj_factor")
            if pd.isna(adj):
                adj = None
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "adj_factor": adj,
                },
            )
            rows += 1
    return rows


def upsert_moneyflow(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_moneyflow (ts_code, trade_date, net_mf, buy_small, sell_small, buy_medium, sell_medium, buy_large, sell_large, buy_huge, sell_huge)"
        " VALUES (:ts_code, :trade_date, :net_mf, :buy_small, :sell_small, :buy_medium, :sell_medium, :buy_large, :sell_large, :buy_huge, :sell_huge)"
        " ON DUPLICATE KEY UPDATE net_mf=VALUES(net_mf), buy_small=VALUES(buy_small), sell_small=VALUES(sell_small), buy_medium=VALUES(buy_medium), sell_medium=VALUES(sell_medium), buy_large=VALUES(buy_large), sell_large=VALUES(sell_large), buy_huge=VALUES(buy_huge), sell_huge=VALUES(sell_huge)"
    )

    def clean(v):
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        return None if v is None else float(v)

    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "net_mf": clean(r.get("net_mf") or r.get("net_mf_amount") or r.get("net_mf_vol")),
                    "buy_small": clean(r.get("buy_sm_vol") or r.get("buy_small")),
                    "sell_small": clean(r.get("sell_sm_vol") or r.get("sell_small")),
                    "buy_medium": clean(r.get("buy_md_vol") or r.get("buy_medium")),
                    "sell_medium": clean(r.get("sell_md_vol") or r.get("sell_medium")),
                    "buy_large": clean(r.get("buy_lg_vol") or r.get("buy_large")),
                    "sell_large": clean(r.get("sell_lg_vol") or r.get("sell_large")),
                    "buy_huge": clean(r.get("buy_elg_vol") or r.get("buy_hu_vol") or r.get("buy_huge")),
                    "sell_huge": clean(r.get("sell_elg_vol") or r.get("sell_hu_vol") or r.get("sell_huge")),
                },
            )
            rows += 1
    return rows


def upsert_top10_holders(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO top10_holders (ts_code, ann_date, end_date, holder_name, hold_amount, hold_ratio, hold_float_ratio, hold_change, holder_type)"
        " VALUES (:ts_code, :ann_date, :end_date, :holder_name, :hold_amount, :hold_ratio, :hold_float_ratio, :hold_change, :holder_type)"
        " ON DUPLICATE KEY UPDATE ann_date=VALUES(ann_date), hold_amount=VALUES(hold_amount), hold_ratio=VALUES(hold_ratio),"
        " hold_float_ratio=VALUES(hold_float_ratio), hold_change=VALUES(hold_change), holder_type=VALUES(holder_type)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "ann_date": (pd.to_datetime(r.get("ann_date")).date() if r.get("ann_date") else None),
                    "end_date": (pd.to_datetime(r.get("end_date")).date() if r.get("end_date") else None),
                    "holder_name": r.get("holder_name"),
                    "hold_amount": None if pd.isna(r.get("hold_amount")) else float(r.get("hold_amount")),
                    "hold_ratio": None if pd.isna(r.get("hold_ratio")) else float(r.get("hold_ratio")),
                    "hold_float_ratio": None
                    if pd.isna(r.get("hold_float_ratio"))
                    else float(r.get("hold_float_ratio")),
                    "hold_change": None if pd.isna(r.get("hold_change")) else float(r.get("hold_change")),
                    "holder_type": r.get("holder_type"),
                },
            )
            rows += 1
    return rows


def upsert_margin(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_margin (ts_code, trade_date, financing_balance, financing_buy, financing_repay, securities_lend_balance)"
        " VALUES (:ts_code, :trade_date, :financing_balance, :financing_buy, :financing_repay, :securities_lend_balance)"
        " ON DUPLICATE KEY UPDATE financing_balance=VALUES(financing_balance), financing_buy=VALUES(financing_buy), financing_repay=VALUES(financing_repay), securities_lend_balance=VALUES(securities_lend_balance)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "financing_balance": None
                    if pd.isna(r.get("financing_balance"))
                    else float(r.get("financing_balance")),
                    "financing_buy": None if pd.isna(r.get("financing_buy")) else float(r.get("financing_buy")),
                    "financing_repay": None if pd.isna(r.get("financing_repay")) else float(r.get("financing_repay")),
                    "securities_lend_balance": None
                    if pd.isna(r.get("securities_lend_balance"))
                    else float(r.get("securities_lend_balance")),
                },
            )
            rows += 1
    return rows


def upsert_block_trade(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO block_trade (ts_code, trade_date, trade_time, price, volume, amount, side)"
        " VALUES (:ts_code, :trade_date, :trade_time, :price, :volume, :amount, :side)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "trade_time": (pd.to_datetime(r.get("trade_time")) if r.get("trade_time") else None),
                    "price": None if pd.isna(r.get("price")) else float(r.get("price")),
                    "volume": (
                        int(r.get("volume")) if (r.get("volume") is not None and not pd.isna(r.get("volume"))) else None
                    ),
                    "amount": None if pd.isna(r.get("amount")) else float(r.get("amount")),
                    "side": r.get("side"),
                },
            )
            rows += 1
    return rows


def upsert_stock_basic(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_basic (ts_code, symbol, name, area, industry, fullname, enname, market, exchange, list_status, list_date, delist_date, is_hs)"
        " VALUES (:ts_code, :symbol, :name, :area, :industry, :fullname, :enname, :market, :exchange, :list_status, :list_date, :delist_date, :is_hs)"
        " ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), name=VALUES(name), area=VALUES(area), industry=VALUES(industry), fullname=VALUES(fullname), enname=VALUES(enname), market=VALUES(market), exchange=VALUES(exchange), list_status=VALUES(list_status), list_date=VALUES(list_date), delist_date=VALUES(delist_date), is_hs=VALUES(is_hs)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "ts_code": r.get("ts_code"),
                    "symbol": r.get("symbol"),
                    "name": r.get("name"),
                    "area": r.get("area"),
                    "industry": r.get("industry"),
                    "fullname": r.get("fullname"),
                    "enname": r.get("enname"),
                    "market": r.get("market"),
                    "exchange": r.get("exchange"),
                    "list_status": r.get("list_status"),
                    "list_date": (pd.to_datetime(r.get("list_date")).date() if r.get("list_date") else None),
                    "delist_date": (pd.to_datetime(r.get("delist_date")).date() if r.get("delist_date") else None),
                    "is_hs": r.get("is_hs"),
                },
            )
            rows += 1
    return rows


def upsert_repo_df(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO repo (repo_date, instrument, rate, amount) VALUES (:repo_date, :instrument, :rate, :amount)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "repo_date": (pd.to_datetime(r.get("repo_date")).date() if r.get("repo_date") else None),
                    "instrument": r.get("instrument"),
                    "rate": None if pd.isna(r.get("rate")) else float(r.get("rate")),
                    "amount": None if pd.isna(r.get("amount")) else float(r.get("amount")),
                },
            )
            rows += 1
    return rows


def fetch_stock_daily_rows(ts_code: str, start_date=None):
    """Fetch rows from stock_daily for a given ts_code and optional start_date. Returns list of rows."""
    q = "SELECT trade_date, open, high, low, close, vol, amount FROM stock_daily WHERE ts_code = :ts_code"
    params = {"ts_code": ts_code}
    if start_date is not None:
        q += " AND trade_date >= :start_date"
        params["start_date"] = start_date
    q += " ORDER BY trade_date ASC"
    with engine.connect() as conn:
        res = conn.execute(text(q), params)
        return res.fetchall()


def fetch_existing_keys(table: str, key_date_col: str, start_date=None, end_date=None):
    """Generic fetch of existing keys (ts_code, date) for a table between dates."""
    q = f"SELECT ts_code, `{key_date_col}` FROM `{table}` WHERE `{key_date_col}` BETWEEN :s AND :e"
    with engine.connect() as conn:
        res = conn.execute(text(q), {"s": start_date, "e": end_date})
        existing = set()
        for r in res.fetchall():
            ts = r[0]
            d = r[1]
            if d is None:
                continue
            try:
                dval = d if isinstance(d, (str,)) else d
            except Exception:
                dval = d
            if hasattr(dval, "isoformat"):
                dstr = dval.isoformat()
            else:
                dstr = str(dval)
            existing.add((ts, dstr))
        return existing


def fetch_top10_holder_keys(start_date=None, end_date=None):
    q = "SELECT ts_code, end_date, holder_name FROM top10_holders WHERE end_date BETWEEN :s AND :e"
    with engine.connect() as conn:
        res = conn.execute(text(q), {"s": start_date, "e": end_date})
        existing = set()
        for ts_code, end_date_value, holder_name in res.fetchall():
            if not ts_code or not end_date_value or not holder_name:
                continue
            dstr = end_date_value.isoformat() if hasattr(end_date_value, "isoformat") else str(end_date_value)
            existing.add((ts_code, dstr, holder_name))
        return existing


def get_failed_ts_codes(limit: int = None):
    q = "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(params,'$.ts_code')) AS ts FROM ingest_audit WHERE api_name='daily' AND status='error'"
    if limit:
        q += f" LIMIT {int(limit)}"
    with engine.connect() as conn:
        res = conn.execute(text(q))
        return [r[0] for r in res.fetchall() if r[0]]


def _clean(v):
    """Normalize pandas/numpy types to Python native types."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v


def _round2(v):
    """Round to 2 decimal places, handling None/NaN."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return round(float(v), 2)
    except Exception:
        return v


def _to_date_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def _int_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(value)
    except Exception:
        return None


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value


def upsert_weekly(df: pd.DataFrame) -> int:
    """Bulk upsert stock_weekly rows from a DataFrame."""
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_weekly (ts_code, trade_date, open, high, low, close, pre_close, change_amount, pct_change, vol, amount)"
        " VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :pre_close, :change_amount, :pct_change, :vol, :amount)"
        " ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),"
        " pre_close=VALUES(pre_close), change_amount=VALUES(change_amount), pct_change=VALUES(pct_change),"
        " vol=VALUES(vol), amount=VALUES(amount)"
    )
    count = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            params = {
                "ts_code": _clean(r.get("ts_code")),
                "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                "open": _round2(_clean(r.get("open"))),
                "high": _round2(_clean(r.get("high"))),
                "low": _round2(_clean(r.get("low"))),
                "close": _round2(_clean(r.get("close"))),
                "pre_close": _round2(_clean(r.get("pre_close"))),
                "change_amount": _round2(_clean(r.get("change") or r.get("change_amount"))),
                "pct_change": _round2(_clean(r.get("pct_chg") or r.get("pct_change"))),
                "vol": (int(r.get("vol")) if (r.get("vol") is not None and not pd.isna(r.get("vol"))) else None),
                "amount": _round2(_clean(r.get("amount"))),
            }
            conn.execute(insert_sql, params)
            count += 1
    return count


def upsert_monthly(df: pd.DataFrame) -> int:
    """Bulk upsert stock_monthly rows from a DataFrame."""
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_monthly (ts_code, trade_date, open, high, low, close, pre_close, change_amount, pct_change, vol, amount)"
        " VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :pre_close, :change_amount, :pct_change, :vol, :amount)"
        " ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),"
        " pre_close=VALUES(pre_close), change_amount=VALUES(change_amount), pct_change=VALUES(pct_change),"
        " vol=VALUES(vol), amount=VALUES(amount)"
    )
    count = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            params = {
                "ts_code": _clean(r.get("ts_code")),
                "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                "open": _round2(_clean(r.get("open"))),
                "high": _round2(_clean(r.get("high"))),
                "low": _round2(_clean(r.get("low"))),
                "close": _round2(_clean(r.get("close"))),
                "pre_close": _round2(_clean(r.get("pre_close"))),
                "change_amount": _round2(_clean(r.get("change") or r.get("change_amount"))),
                "pct_change": _round2(_clean(r.get("pct_chg") or r.get("pct_change"))),
                "vol": (int(r.get("vol")) if (r.get("vol") is not None and not pd.isna(r.get("vol"))) else None),
                "amount": _round2(_clean(r.get("amount"))),
            }
            conn.execute(insert_sql, params)
            count += 1
    return count


def upsert_index_weekly_df(df: pd.DataFrame) -> int:
    """Upsert index_weekly rows from a DataFrame."""
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO index_weekly (index_code, trade_date, open, high, low, close, vol, amount) "
        "VALUES (:index_code, :trade_date, :open, :high, :low, :close, :vol, :amount) "
        "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), "
        "close=VALUES(close), vol=VALUES(vol), amount=VALUES(amount)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient="records"):
            conn.execute(
                insert_sql,
                {
                    "index_code": r.get("ts_code") or r.get("index_code"),
                    "trade_date": (pd.to_datetime(r.get("trade_date")).date() if r.get("trade_date") else None),
                    "open": None if pd.isna(r.get("open")) else float(r.get("open")),
                    "high": None if pd.isna(r.get("high")) else float(r.get("high")),
                    "low": None if pd.isna(r.get("low")) else float(r.get("low")),
                    "close": None if pd.isna(r.get("close")) else float(r.get("close")),
                    "vol": (int(r.get("vol")) if (r.get("vol") is not None and not pd.isna(r.get("vol"))) else None),
                    "amount": None if pd.isna(r.get("amount")) else float(r.get("amount")),
                },
            )
            rows += 1
    return rows

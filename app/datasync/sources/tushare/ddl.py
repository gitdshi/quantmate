"""DDL definitions for all Tushare tables."""

from __future__ import annotations

from datetime import date, datetime
import hashlib
import re

import pandas as pd

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

SUSPEND_HISTORY_DDL = """
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

DAILY_BASIC_DDL = """
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

INDEX_BASIC_DDL = """
CREATE TABLE IF NOT EXISTS index_basic (
    index_code VARCHAR(32) NOT NULL PRIMARY KEY,
    name VARCHAR(255),
    market VARCHAR(32),
    publisher VARCHAR(128),
    category VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_index_basic_market (market)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


_DATE_COLUMN_PRIORITY = (
    "trade_date",
    "ann_date",
    "end_date",
    "report_date",
    "date",
    "cal_date",
    "record_date",
    "start_date",
    "end_date",
    "ex_date",
    "pay_date",
    "issue_date",
    "ipo_date",
    "list_date",
    "delist_date",
    "f_ann_date",
    "imp_ann_date",
    "pretrade_date",
    "resume_date",
    "suspend_date",
)

_DATE_COLUMN_NAMES = frozenset(_DATE_COLUMN_PRIORITY)

_CODE_COLUMN_PRIORITY = (
    "ts_code",
    "index_code",
    "con_code",
    "fund_code",
    "stock_code",
    "bond_code",
    "code",
    "symbol",
)

_SAMPLE_INFERRED_TABLES = frozenset({"report_rc", "us_basic", "us_daily", "shibor_lpr"})


def uses_sample_inferred_schema(table_name: str) -> bool:
    normalized = str(table_name or "").strip()
    if not normalized:
        return False
    return normalized in _SAMPLE_INFERRED_TABLES or normalized not in _CATALOG_DDL_MAP


def get_catalog_ddl(table_name: str) -> str:
    normalized = str(table_name or "").strip()
    if uses_sample_inferred_schema(normalized):
        raise ValueError(f"Tushare table {normalized} requires sample-based schema inference")
    try:
        return _CATALOG_DDL_MAP[normalized]
    except KeyError as exc:
        raise ValueError(f"No static DDL registered for Tushare table {normalized}") from exc


def _safe_index_name(table_name: str, column_name: str) -> str:
    raw = re.sub(r"[^0-9a-zA-Z_]+", "_", f"idx_{table_name}_{column_name}")
    if len(raw) <= 60:
        return raw
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"{raw[:51]}_{digest}"


def _safe_unique_index_name(table_name: str, key_columns: tuple[str, ...]) -> str:
    suffix = "_".join(key_columns) if key_columns else "row_key"
    raw = re.sub(r"[^0-9a-zA-Z_]+", "_", f"ux_{table_name}_{suffix}")
    if len(raw) <= 60:
        return raw
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"{raw[:51]}_{digest}"


def _normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z_]+", "_", str(name or "").strip()).strip("_").lower()
    return normalized or "value"


def _coerce_records(rows: pd.DataFrame | list[dict]) -> list[dict]:
    if isinstance(rows, pd.DataFrame):
        return rows.to_dict(orient="records") if rows is not None and not rows.empty else []
    return list(rows or [])


def _ordered_source_columns(rows: pd.DataFrame | list[dict], records: list[dict]) -> list[str]:
    if isinstance(rows, pd.DataFrame):
        return [str(column) for column in rows.columns]

    ordered: list[str] = []
    for record in records:
        for key in record.keys():
            column = str(key)
            if column not in ordered:
                ordered.append(column)
    return ordered


def _clean_sample_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _is_code_column(name: str) -> bool:
    normalized = _normalize_column_name(name)
    if normalized in _CODE_COLUMN_PRIORITY:
        return True
    return normalized.endswith("_code") or normalized in {"code", "symbol"}


def _looks_like_date_column(name: str) -> bool:
    normalized = _normalize_column_name(name)
    return normalized in _DATE_COLUMN_NAMES or normalized.endswith("_date")


def _looks_like_date_value(value) -> bool:
    cleaned = _clean_sample_value(value)
    if cleaned is None:
        return False
    if isinstance(cleaned, (datetime, date)):
        return True

    text = str(cleaned).strip()
    if not text:
        return False
    if re.fullmatch(r"\d{8}", text) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return True
    try:
        pd.to_datetime(text)
        return True
    except Exception:
        return False


def _is_bool_value(value) -> bool:
    return isinstance(value, bool)


def _is_int_like_value(value) -> bool:
    cleaned = _clean_sample_value(value)
    if cleaned is None or _is_bool_value(cleaned):
        return False
    if isinstance(cleaned, int):
        return True
    if isinstance(cleaned, float):
        return float(cleaned).is_integer()
    return False


def _is_numeric_value(value) -> bool:
    cleaned = _clean_sample_value(value)
    if cleaned is None or _is_bool_value(cleaned):
        return False
    return isinstance(cleaned, (int, float))


def _varchar_bucket(max_length: int) -> int:
    for bucket in (16, 32, 64, 128, 255, 512):
        if max_length <= bucket:
            return bucket
    return 1024


def _infer_column_spec(column_name: str, values: list[object]) -> dict[str, object]:
    normalized_name = _normalize_column_name(column_name)
    samples = [_clean_sample_value(value) for value in values if _clean_sample_value(value) is not None]

    spec: dict[str, object] = {
        "name": normalized_name,
        "source_fields": [str(column_name)],
        "normalizer": "clean",
        "nullable": True,
    }

    if samples and any(isinstance(value, (dict, list)) for value in samples):
        spec["sql_type"] = "JSON"
        spec["normalizer"] = "json"
        return spec

    if samples and _looks_like_date_column(normalized_name) and all(_looks_like_date_value(value) for value in samples):
        spec["sql_type"] = "DATE"
        spec["normalizer"] = "date"
        return spec

    if samples and all(_is_bool_value(value) for value in samples):
        spec["sql_type"] = "TINYINT(1)"
        spec["normalizer"] = "bool"
        return spec

    if samples and all(_is_int_like_value(value) for value in samples):
        spec["sql_type"] = "BIGINT"
        spec["normalizer"] = "int"
        return spec

    if samples and all(_is_numeric_value(value) for value in samples):
        spec["sql_type"] = "DOUBLE"
        spec["normalizer"] = "float"
        return spec

    max_length = max((len(str(value)) for value in samples), default=32)
    if _is_code_column(normalized_name):
        spec["sql_type"] = f"VARCHAR({max(32, min(_varchar_bucket(max_length), 128))})"
    elif max_length <= 512:
        spec["sql_type"] = f"VARCHAR({_varchar_bucket(max_length)})"
    else:
        spec["sql_type"] = "TEXT"
    return spec


def _resolve_date_column(
    column_specs: list[dict[str, object]],
    preferred_date_column: str | None = None,
) -> str | None:
    by_source = {
        str(spec["source_fields"][0]): str(spec["name"])
        for spec in column_specs
        if spec.get("source_fields")
    }
    if preferred_date_column and preferred_date_column in by_source:
        return by_source[preferred_date_column]

    for candidate in _DATE_COLUMN_PRIORITY:
        for spec in column_specs:
            if spec["name"] == candidate:
                return str(spec["name"])
    for spec in column_specs:
        if _looks_like_date_column(str(spec["name"])):
            return str(spec["name"])
    return None


def _resolve_code_column(
    column_specs: list[dict[str, object]],
    preferred_key_fields: tuple[str, ...] | list[str] | None = None,
) -> str | None:
    by_source = {
        str(spec["source_fields"][0]): str(spec["name"])
        for spec in column_specs
        if spec.get("source_fields")
    }
    for candidate in preferred_key_fields or ():
        if candidate in by_source and _is_code_column(candidate):
            return by_source[candidate]

    names = {str(spec["name"]) for spec in column_specs}
    for candidate in _CODE_COLUMN_PRIORITY:
        if candidate in names:
            return candidate
    for spec in column_specs:
        if _is_code_column(str(spec["name"])):
            return str(spec["name"])
    return None


def _resolve_key_columns(
    column_specs: list[dict[str, object]],
    preferred_date_column: str | None = None,
    preferred_key_fields: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    date_column = _resolve_date_column(column_specs, preferred_date_column)
    code_column = _resolve_code_column(column_specs, preferred_key_fields)
    if code_column and date_column and code_column != date_column:
        return (date_column, code_column)
    if code_column:
        return (code_column,)
    if date_column:
        return (date_column,)

    preferred = [
        str(spec["name"])
        for spec in column_specs
        if spec.get("source_fields") and str(spec["source_fields"][0]) in set(preferred_key_fields or ())
    ]
    if preferred:
        return tuple(dict.fromkeys(preferred))
    if column_specs:
        return (str(column_specs[0]["name"]),)
    return ()


def _column_ddl(column_spec: dict[str, object], key_columns: tuple[str, ...]) -> str:
    name = str(column_spec["name"])
    sql_type = str(column_spec["sql_type"])
    nullable = "NOT NULL" if name in key_columns else "NULL"
    return f"`{name}` {sql_type} {nullable}"


def build_dynamic_table_ddl(
    table_name: str,
    column_specs: list[dict[str, object]],
    key_columns: tuple[str, ...],
) -> str:
    lines = [f"    {_column_ddl(spec, key_columns)}" for spec in column_specs]
    lines.append("    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    lines.append("    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

    indexes: list[str] = []
    if key_columns:
        unique_index_name = _safe_unique_index_name(table_name, key_columns)
        joined_columns = ", ".join(f"`{column}`" for column in key_columns)
        indexes.append(f"    UNIQUE KEY `{unique_index_name}` ({joined_columns})")

    body = ",\n".join(lines + indexes)
    return f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
{body}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""".strip()


def infer_dynamic_table_schema(
    table_name: str,
    rows: pd.DataFrame | list[dict],
    *,
    preferred_date_column: str | None = None,
    preferred_key_fields: tuple[str, ...] | list[str] | None = None,
) -> dict[str, object]:
    records = _coerce_records(rows)
    if not records:
        raise ValueError(f"Cannot infer schema for {table_name} without sample rows")

    column_specs = [
        _infer_column_spec(column_name, [record.get(column_name) for record in records])
        for column_name in _ordered_source_columns(rows, records)
    ]
    key_columns = _resolve_key_columns(
        column_specs,
        preferred_date_column=preferred_date_column,
        preferred_key_fields=preferred_key_fields,
    )
    return {
        "column_specs": column_specs,
        "key_columns": key_columns,
        "unique_index_name": _safe_unique_index_name(table_name, key_columns),
        "ddl": build_dynamic_table_ddl(table_name, column_specs, key_columns),
    }

TUSHARE_BOOTSTRAP_TABLES = frozenset(
    {
        "stock_company",
        "new_share",
        "stock_daily",
        "suspend_d",
    }
)


def should_bootstrap_table(table_name: str) -> bool:
    return str(table_name or "").strip() in TUSHARE_BOOTSTRAP_TABLES


_CATALOG_DDL_MAP = {
    "trade_cal": TRADE_CAL_DDL,
    "stock_basic": STOCK_BASIC_DDL,
    "stock_company": STOCK_COMPANY_DDL,
    "new_share": NEW_SHARE_DDL,
    "stock_daily": STOCK_DAILY_DDL,
    "bak_daily": BAK_DAILY_DDL,
    "stock_moneyflow": STOCK_MONEYFLOW_DDL,
    "suspend_d": SUSPEND_D_DDL,
    "suspend": SUSPEND_HISTORY_DDL,
    "adj_factor": ADJ_FACTOR_DDL,
    "daily_basic": DAILY_BASIC_DDL,
    "fina_indicator": FINA_INDICATOR_DDL,
    "income": INCOME_DDL,
    "balancesheet": BALANCESHEET_DDL,
    "cashflow": CASHFLOW_DDL,
    "stock_dividend": STOCK_DIVIDEND_DDL,
    "top10_holders": TOP10_HOLDERS_DDL,
    "stock_weekly": STOCK_WEEKLY_DDL,
    "stock_monthly": STOCK_MONTHLY_DDL,
    "index_basic": INDEX_BASIC_DDL,
    "index_daily": INDEX_DAILY_DDL,
    "index_weekly": INDEX_WEEKLY_DDL,
}

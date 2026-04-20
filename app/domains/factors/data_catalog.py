"""Data Catalog — inventory all synced data fields available for factor mining.

Scans MySQL tushare.* and akshare.* tables via INFORMATION_SCHEMA to build
a structured catalog of available data fields. This enables RD-Agent and the
factor engine to dynamically adapt to whatever data DataSync currently provides.
"""

from __future__ import annotations

import logging
from typing import Any

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

# Mapping of well-known columns to semantic categories
_CATEGORY_MAP: dict[str, str] = {
    # Price
    "open": "price",
    "high": "price",
    "low": "price",
    "close": "price",
    "pre_close": "price",
    "change": "price",
    "change_amount": "price",
    "pct_change": "price",
    "pct_chg": "price",
    # Volume
    "vol": "volume",
    "volume": "volume",
    "amount": "volume",
    "turnover_rate": "volume",
    "turnover_rate_f": "volume",
    "volume_ratio": "volume",
    # Fundamental / Valuation
    "pe": "fundamental",
    "pe_ttm": "fundamental",
    "pb": "fundamental",
    "ps": "fundamental",
    "ps_ttm": "fundamental",
    "total_mv": "fundamental",
    "circ_mv": "fundamental",
    "dv_ratio": "fundamental",
    "dv_ttm": "fundamental",
    # Money flow
    "net_mf": "flow",
    "net_mf_amount": "flow",
    "buy_sm_vol": "flow",
    "sell_sm_vol": "flow",
    "buy_md_vol": "flow",
    "sell_md_vol": "flow",
    "buy_lg_vol": "flow",
    "sell_lg_vol": "flow",
    "buy_elg_vol": "flow",
    "sell_elg_vol": "flow",
    "buy_sm_amount": "flow",
    "sell_sm_amount": "flow",
    "buy_md_amount": "flow",
    "sell_md_amount": "flow",
    "buy_lg_amount": "flow",
    "sell_lg_amount": "flow",
    "buy_elg_amount": "flow",
    "sell_elg_amount": "flow",
    # Margin
    "financing_balance": "margin",
    "financing_buy": "margin",
    "financing_repay": "margin",
    "securities_lend_balance": "margin",
    # Dividend
    "div_cash": "dividend",
    "div_stock": "dividend",
    "bonus_ratio": "dividend",
    # Adjustment
    "adj_factor": "technical",
    "factor": "technical",
}

# Columns that are identifiers/metadata, not numeric features
_METADATA_COLUMNS = frozenset({
    "ts_code",
    "symbol",
    "name",
    "trade_date",
    "ann_date",
    "end_date",
    "holder_name",
    "area",
    "industry",
    "fullname",
    "enname",
    "market",
    "exchange",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
    "id",
    "created_at",
    "updated_at",
})

# MySQL numeric types that make sense as factor features
_NUMERIC_TYPES = frozenset({
    "int",
    "bigint",
    "smallint",
    "tinyint",
    "mediumint",
    "float",
    "double",
    "decimal",
    "numeric",
})


def _classify_column(column_name: str) -> str:
    """Return semantic category for a column name."""
    lower = column_name.lower()
    if lower in _METADATA_COLUMNS:
        return "metadata"
    return _CATEGORY_MAP.get(lower, "other")


def _is_numeric_type(data_type: str) -> bool:
    """Check if a MySQL column type is numeric."""
    base = data_type.lower().split("(")[0].strip()
    return base in _NUMERIC_TYPES


def scan_database_columns(
    source: str,
    database_name: str,
) -> list[dict[str, Any]]:
    """Scan INFORMATION_SCHEMA.COLUMNS for a given database.

    Returns list of dicts with: table_name, column_name, data_type, category.
    """
    from sqlalchemy import text

    query = text(
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = :db "
        "ORDER BY table_name, ordinal_position"
    )

    results: list[dict[str, Any]] = []
    try:
        with connection(source) as conn:  # type: ignore[arg-type]
            rows = conn.execute(query, {"db": database_name}).fetchall()
            for row in rows:
                table_name = row[0]
                column_name = row[1]
                data_type = row[2]
                category = _classify_column(column_name)
                results.append(
                    {
                        "source": source,
                        "table_name": table_name,
                        "column_name": column_name,
                        "data_type": data_type,
                        "category": category,
                        "is_numeric": _is_numeric_type(data_type),
                    }
                )
    except Exception:
        logger.debug("[data-catalog] Failed to scan %s.%s", source, database_name, exc_info=True)

    return results


def get_catalog(
    include_metadata: bool = False,
    numeric_only: bool = True,
) -> list[dict[str, Any]]:
    """Build the full data catalog from tushare + akshare databases.

    Returns list of dicts describing every available data field.
    """
    catalog: list[dict[str, Any]] = []

    for source, db_name in [("tushare", "tushare"), ("akshare", "akshare")]:
        try:
            columns = scan_database_columns(source, db_name)
            for col in columns:
                if numeric_only and not col["is_numeric"]:
                    continue
                if not include_metadata and col["category"] == "metadata":
                    continue
                catalog.append(col)
        except Exception:
            logger.debug("[data-catalog] Skipping %s", source, exc_info=True)

    return catalog


def get_catalog_summary() -> dict[str, Any]:
    """Return a summary of available data grouped by category.

    Returns dict with:
      - categories: {category_name: [field_names]}
      - total_fields: int
      - sources: list of source names
    """
    catalog = get_catalog(numeric_only=True, include_metadata=False)
    categories: dict[str, list[str]] = {}
    sources: set[str] = set()

    for entry in catalog:
        cat = entry["category"]
        col = entry["column_name"]
        sources.add(entry["source"])
        categories.setdefault(cat, [])
        if col not in categories[cat]:
            categories[cat].append(col)

    return {
        "categories": categories,
        "total_fields": sum(len(v) for v in categories.values()),
        "sources": sorted(sources),
    }


def get_feature_columns_for_qlib() -> list[dict[str, str]]:
    """Return the list of numeric columns suitable for Qlib binary conversion.

    Each entry: {source, table_name, column_name, category}
    Only includes columns from tables that have a trade_date + ts_code key pattern.
    """
    catalog = get_catalog(numeric_only=True, include_metadata=False)
    # Filter to only tables that look like time-series data (have trade_date-like columns)
    return [
        {
            "source": e["source"],
            "table_name": e["table_name"],
            "column_name": e["column_name"],
            "category": e["category"],
        }
        for e in catalog
        if e["category"] != "metadata"
    ]

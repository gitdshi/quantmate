from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd


_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parents[1]
_TMP_DIR = _PROJECT_ROOT / "tmp"
_METADATA_DIR = _PROJECT_ROOT / "app" / "datasync" / "metadata"
_CACHE_DIR = _SCRIPT_DIR / ".cache"
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.datasync.sources.tushare.catalog_interfaces import (  # noqa: E402
    _ANN_DATE_APIS,
    _END_DATE_APIS,
    _RANGE_APIS,
    _REQUEST_DATE_OVERRIDES,
    _TRADE_DATE_APIS,
)
from app.datasync.sources.tushare.interfaces import (  # noqa: E402
    _ONE_SHOT_DATE_CATALOG_KEYS,
    _PER_SYMBOL_DATE_CATALOG_CONFIG,
)


SEARCH_URL = "https://tushare.pro/document/search?q={query}"
DOC_URL = "https://tushare.pro/document/2?doc_id={doc_id}"
DOC_ID_PATTERN = re.compile(r"doc_id=(\d+)")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")
PARAM_COLUMNS = {"名称", "类型", "描述"}
REQUIRED_PARAM_COLUMNS = ("必选", "必须")
DATE_PARAM_PATTERN = re.compile(r"date", re.IGNORECASE)
CODE_PARAM_NAMES = {
    "ts_code",
    "index_code",
    "con_code",
    "code",
    "symbol",
    "fund_code",
    "o_code",
    "n_code",
    "hk_code",
}

SPECIAL_FALLBACK_PARAMS: dict[str, list[str]] = {
    "bo_monthly": ["date"],
    "bo_weekly": ["date"],
    "cyq_chips": ["ts_code", "trade_date"],
    "fund_div": ["ts_code", "ann_date", "start_date", "end_date"],
    "fund_nav": ["ts_code", "nav_date", "start_date", "end_date"],
    "fund_portfolio": ["ts_code", "ann_date", "start_date", "end_date"],
    "index_daily": ["ts_code", "start_date", "end_date"],
    "index_weight": ["index_code", "start_date", "end_date"],
    "pledge_detail": ["ts_code"],
    "stock_basic": ["ts_code"],
    "stock_company": ["ts_code"],
    "stock_daily": ["ts_code", "trade_date"],
    "stock_monthly": ["ts_code", "trade_date"],
    "stock_weekly": ["ts_code", "trade_date"],
    "top10_holders": ["ts_code", "start_date", "end_date"],
    "trade_cal": ["exchange", "start_date", "end_date"],
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Re-analyze Tushare backfill methods from docs parameters.")
    parser.add_argument(
        "--input-json",
        default=str(_TMP_DIR / "staging_datasync_table_updated.json"),
        help="Input payload file containing the current datasync table records. Supports JSON or CSV.",
    )
    parser.add_argument(
        "--output-prefix",
        default=str(_METADATA_DIR / "tushare_backfill_analysis"),
        help="Output file prefix without extension.",
    )
    parser.add_argument(
        "--cache-json",
        default=str(_CACHE_DIR / "tushare_doc_param_cache.json"),
        help="Cache file for fetched Tushare doc parameter metadata.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only analyze the first N Tushare records. 0 means all records.",
    )
    parser.add_argument(
        "--refresh-doc-cache",
        action="store_true",
        help="Ignore any cached Tushare doc metadata and refetch all interface docs online.",
    )
    return parser.parse_args()


def _curl(url: str) -> str:
    return subprocess.check_output(
        ["curl", "-Lks", "--max-time", "20", url],
        text=True,
        stderr=subprocess.DEVNULL,
    )


def _dedupe_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _load_payload(input_path: Path) -> dict[str, Any]:
    if input_path.suffix.lower() == ".csv":
        with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            records = [dict(row) for row in reader]
            return {
                "columns": list(reader.fieldnames or []),
                "records": records,
            }

    return json.loads(input_path.read_text(encoding="utf-8"))


def _extract_input_params_from_html(html: str) -> list[dict[str, str]]:
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []

    for table in tables:
        columns = [str(column).strip() for column in table.columns]
        column_set = set(columns)
        if not PARAM_COLUMNS.issubset(column_set):
            continue
        required_column = next((column for column in REQUIRED_PARAM_COLUMNS if column in column_set), None)
        if not required_column:
            continue

        params: list[dict[str, str]] = []
        for _, row in table.iterrows():
            name = str(row.get("名称", "")).strip()
            if not name or name == "nan":
                continue
            params.append(
                {
                    "name": name,
                    "type": str(row.get("类型", "")).strip(),
                    "required": str(row.get(required_column, "")).strip(),
                    "description": str(row.get("描述", "")).strip(),
                }
            )
        if params:
            return params
    return []


def _search_doc_ids(query: str) -> list[str]:
    html = _curl(SEARCH_URL.format(query=query))
    return _dedupe_preserve(DOC_ID_PATTERN.findall(html))


def _page_matches_api(html: str, api_name: str) -> bool:
    markers = (
        f"pro.{api_name}",
        f'"{api_name}"',
        f"'{api_name}'",
        f">{api_name}<",
    )
    return any(marker in html for marker in markers)


def _page_matches_item_name(html: str, item_name: str) -> bool:
    normalized = str(item_name or "").strip()
    return bool(normalized) and normalized in html


def _page_has_no_input_params(html: str) -> bool:
    normalized = HTML_TAG_PATTERN.sub("", html)
    normalized = WHITESPACE_PATTERN.sub("", normalized)
    return "输入参数无" in normalized


def _fetch_doc_metadata(api_name: str, item_name: str) -> dict[str, Any]:
    queries = [api_name]
    if item_name and item_name not in queries:
        queries.append(item_name)

    candidate_ids: list[str] = []
    for query in queries:
        try:
            candidate_ids.extend(_search_doc_ids(query))
        except subprocess.CalledProcessError:
            continue
    candidate_ids = _dedupe_preserve(candidate_ids)

    for doc_id in candidate_ids:
        try:
            html = _curl(DOC_URL.format(doc_id=doc_id))
        except subprocess.CalledProcessError:
            continue
        if not (_page_matches_api(html, api_name) or _page_matches_item_name(html, item_name)):
            continue
        params = _extract_input_params_from_html(html)
        if params or _page_has_no_input_params(html):
            return {
                "doc_id": doc_id,
                "doc_url": DOC_URL.format(doc_id=doc_id),
                "params": params,
                "basis": "tushare_docs",
            }

    return {"doc_id": "", "doc_url": "", "params": [], "basis": "not_found"}


def _fallback_params(record: dict[str, Any]) -> list[dict[str, str]]:
    raw_item = record.get("raw_item") or {}
    interface_key = str(raw_item.get("item_key") or record.get("interface") or "")
    api_name = str(raw_item.get("api_name") or interface_key)

    if interface_key in SPECIAL_FALLBACK_PARAMS:
        param_names = SPECIAL_FALLBACK_PARAMS[interface_key]
    elif interface_key in _ONE_SHOT_DATE_CATALOG_KEYS:
        param_names = [_ONE_SHOT_DATE_CATALOG_KEYS[interface_key]]
    elif interface_key in _PER_SYMBOL_DATE_CATALOG_CONFIG:
        date_config = _PER_SYMBOL_DATE_CATALOG_CONFIG[interface_key]
        param_names = ["ts_code", str(date_config["request_date_param"])]
        if bool(date_config.get("supports_range", False)):
            param_names.extend(["start_date", "end_date"])
    else:
        param_names = []
        override = _REQUEST_DATE_OVERRIDES.get(api_name)
        if override:
            param_names.append(override)
        elif api_name in _TRADE_DATE_APIS:
            param_names.append("trade_date")
        elif api_name in _ANN_DATE_APIS:
            param_names.append("ann_date")
        elif api_name in _END_DATE_APIS:
            param_names.append("end_date")
        if api_name in _RANGE_APIS:
            param_names.extend(["start_date", "end_date"])
        if not param_names and record.get("runtime_supports_backfill") is False:
            param_names = ["ts_code"]

    return [
        {
            "name": name,
            "type": "",
            "required": "",
            "description": "runtime fallback",
        }
        for name in _dedupe_preserve(param_names)
    ]


def _is_required_param(raw: str | None) -> bool:
    normalized = str(raw or "").strip().lower()
    return normalized in {"y", "yes", "true", "1", "required", "是", "必填"}


def _normalize_backfill_mode(raw: str | None) -> str | None:
    normalized = str(raw or "").strip().lower()
    return normalized if normalized in {"range", "date", "code", "code_date", "other"} else None


def _preferred_record_value(record: dict[str, Any], preferred_key: str, fallback_key: str) -> Any:
    preferred_value = record.get(preferred_key)
    if preferred_value not in {None, ""}:
        return preferred_value
    return record.get(fallback_key)


def _analyze_method(params: list[dict[str, str]]) -> tuple[str, list[str]]:
    normalized_params = [
        {
            "name": str(param.get("name") or "").strip(),
            "required": _is_required_param(param.get("required")),
        }
        for param in params
        if str(param.get("name") or "").strip() and str(param.get("name") or "").strip() != "nan"
    ]
    param_names = [param["name"] for param in normalized_params]
    name_set = set(param_names)
    date_params = [name for name in param_names if DATE_PARAM_PATTERN.search(name)]
    code_params = [name for name in param_names if name in CODE_PARAM_NAMES or name.endswith("_code")]
    required_date_params = [param["name"] for param in normalized_params if param["required"] and DATE_PARAM_PATTERN.search(param["name"])]
    required_code_params = [param["name"] for param in normalized_params if param["required"] and (param["name"] in CODE_PARAM_NAMES or param["name"].endswith("_code"))]

    if {"start_date", "end_date"}.issubset(name_set):
        return "range", date_params
    if required_code_params and required_date_params:
        return "code_date", date_params
    if date_params and not required_code_params:
        return "date", date_params
    if code_params and not required_date_params:
        return "code", date_params

    if code_params and date_params:
        return "code_date", date_params
    if code_params:
        return "code", date_params
    if date_params:
        return "date", date_params
    return "other", date_params


def _analysis_logic(method: str, param_names: list[str], date_params: list[str]) -> str:
    if method == "range":
        return f"官网输入参数含 start_date/end_date；参数={', '.join(param_names)}；按区间回补"
    if method == "code_date":
        return f"官网输入参数同时要求代码和日期；日期参数={', '.join(date_params)}；参数={', '.join(param_names)}；按代码+日期维度回补"
    if method == "date":
        return f"官网输入参数含日期字段 {', '.join(date_params)}；参数={', '.join(param_names)}；按单日期回补"
    if method == "code":
        return f"官网输入参数不要求日期字段；参数={', '.join(param_names) if param_names else '未解析到参数'}；按代码维度回补/同步"
    return f"官网输入参数不提供代码或日期回补轴；参数={', '.join(param_names) if param_names else '未解析到参数'}；归类为其他回补方式"


def _analysis_slow_reason(method: str) -> str:
    if method == "range":
        return "按区间回补；单次任务扫描窗口较长，吞吐受窗口跨度和配额影响"
    if method == "code_date":
        return "按代码+日期组合回补；吞吐同时受标的数量、日期跨度和接口配额影响"
    if method == "date":
        return "按日期逐次回补；吞吐受日期跨度和接口配额影响"
    if method == "code":
        return "按代码逐个回补/同步；吞吐受标的数量和接口配额影响"
    return "接口不提供标准代码/日期回补轴；通常需要额外条件或一次性同步策略"


def _serialize_params(params: list[dict[str, str]]) -> tuple[str, str]:
    names = [param["name"] for param in params]
    details = [
        f"{param['name']}({param['description']})" if param.get("description") else param["name"]
        for param in params
    ]
    return ", ".join(names), "；".join(details)


def _rewrite_record(record: dict[str, Any], cache: dict[str, dict[str, Any]]) -> dict[str, Any]:
    updated = dict(record)
    updated["runtime_supports_backfill"] = _preferred_record_value(record, "runtime_supports_backfill", "supports_backfill")
    updated["runtime_backfill_mode"] = _preferred_record_value(record, "backfill_mode", "runtime_backfill_mode")
    updated["runtime_backfill_logic"] = _preferred_record_value(record, "backfill_logic", "runtime_backfill_logic")
    updated["runtime_slow_reason"] = _preferred_record_value(record, "slow_reason", "runtime_slow_reason")

    if record.get("source") != "tushare":
        updated["analysis_basis"] = "source_passthrough"
        updated["input_params"] = ""
        updated["input_param_details"] = ""
        updated["analysis_date_params"] = ""
        updated["analysis_doc_id"] = ""
        updated["analysis_doc_url"] = ""
        return updated

    raw_item = record.get("raw_item") or {}
    interface_key = str(raw_item.get("item_key") or record.get("interface") or "")
    api_name = str(raw_item.get("api_name") or interface_key)
    item_name = str(raw_item.get("item_name") or record.get("interface_name") or "")

    doc_meta = cache.get(api_name)
    if doc_meta is None or doc_meta.get("basis") != "tushare_docs":
        doc_meta = _fetch_doc_metadata(api_name, item_name)
        if doc_meta.get("basis") != "tushare_docs":
            doc_meta = dict(doc_meta)
            doc_meta["params"] = _fallback_params(updated)
            doc_meta["basis"] = "runtime_fallback"
        cache[api_name] = doc_meta

    param_names, param_details = _serialize_params(doc_meta.get("params", []))
    method, date_params = _analyze_method(doc_meta.get("params", []))

    updated["supports_backfill"] = True
    updated["backfill_mode"] = method
    updated["input_params"] = param_names
    updated["input_param_details"] = param_details
    updated["analysis_date_params"] = ", ".join(date_params)
    updated["analysis_doc_id"] = doc_meta.get("doc_id", "")
    updated["analysis_doc_url"] = doc_meta.get("doc_url", "")
    updated["analysis_basis"] = doc_meta.get("basis", "")
    updated["backfill_logic"] = _analysis_logic(method, [param["name"] for param in doc_meta.get("params", [])], date_params)
    updated["slow_reason"] = _analysis_slow_reason(method)
    updated["runtime_backfill_mode"] = updated["backfill_mode"]
    updated["runtime_backfill_logic"] = updated["backfill_logic"]
    updated["runtime_slow_reason"] = updated["slow_reason"]
    return updated


def _write_outputs(prefix: Path, payload: dict[str, Any]) -> None:
    json_path = prefix.with_suffix(".json")
    csv_path = prefix.with_suffix(".csv")
    md_path = prefix.with_suffix(".md")

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_columns = [
        "source",
        "major_category",
        "subcategory",
        "interface",
        "interface_name",
        "enabled",
        "total_count",
        "pending_count",
        "partial_count",
        "error_count",
        "supports_backfill",
        "backfill_mode",
        "input_params",
        "input_param_details",
        "analysis_date_params",
        "analysis_basis",
        "analysis_doc_id",
        "analysis_doc_url",
        "runtime_supports_backfill",
        "runtime_backfill_logic",
        "latest_problem_date",
        "latest_problem_status",
        "latest_error",
        "failure_reason",
        "slow_reason",
        "backfill_logic",
        "iface_type",
    ]
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_columns)
        writer.writeheader()
        for record in payload["records"]:
            writer.writerow({column: record.get(column, "") for column in csv_columns})

    md_headers = [
        "source",
        "major_category",
        "subcategory",
        "interface",
        "interface_name",
        "supports_backfill",
        "backfill_mode",
        "input_params",
        "analysis_date_params",
        "analysis_basis",
        "backfill_logic",
    ]
    lines = [
        "| " + " | ".join(md_headers) + " |",
        "| " + " | ".join(["---"] * len(md_headers)) + " |",
    ]
    for record in payload["records"]:
        row = []
        for header in md_headers:
            value = str(record.get(header, "") if record.get(header, "") is not None else "")
            row.append(value.replace("|", "\\|").replace("\n", "<br>"))
        lines.append("| " + " | ".join(row) + " |")
    md_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = _parse_args()
    input_path = Path(args.input_json)
    output_prefix = Path(args.output_prefix)
    cache_path = Path(args.cache_json)

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    payload = _load_payload(input_path)
    cache: dict[str, dict[str, Any]] = {}
    if cache_path.exists() and not args.refresh_doc_cache:
        cache = json.loads(cache_path.read_text(encoding="utf-8"))

    rewritten_records: list[dict[str, Any]] = []
    tushare_seen = 0
    for record in payload["records"]:
        if record.get("source") == "tushare":
            tushare_seen += 1
            if args.limit and tushare_seen > args.limit:
                rewritten_records.append(dict(record))
                continue
        rewritten_records.append(_rewrite_record(record, cache))

    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

    output_columns = [column for column in payload.get("columns", []) if column != "runtime_backfill_mode"]

    output_payload = {
        "columns": output_columns,
        "records": rewritten_records,
        "meta": {
            "analysis_rule": "start_date+end_date => range; required code + required date => code_date; date axis without required code => date; code axis without required date => code; otherwise => other",
            "cache_file": str(cache_path),
            "input_file": str(input_path),
        },
    }
    _write_outputs(output_prefix, output_payload)

    print(
        json.dumps(
            {
                "json": str(output_prefix.with_suffix('.json')),
                "csv": str(output_prefix.with_suffix('.csv')),
                "markdown": str(output_prefix.with_suffix('.md')),
                "cache": str(cache_path),
                "record_count": len(rewritten_records),
                "tushare_cached": len(cache),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
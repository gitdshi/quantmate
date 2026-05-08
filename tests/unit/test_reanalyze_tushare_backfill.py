from __future__ import annotations

import importlib.util
from pathlib import Path


_MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "datasync" / "reanalyze_tushare_backfill.py"
_SPEC = importlib.util.spec_from_file_location("reanalyze_tushare_backfill", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


def _param(name: str, required: str = "") -> dict[str, str]:
    return {"name": name, "required": required, "type": "", "description": ""}


def test_load_payload_supports_csv(tmp_path):
    csv_path = tmp_path / "analysis.csv"
    csv_path.write_text(
        "source,interface,backfill_mode\n"
        "tushare,stock_daily,date\n",
        encoding="utf-8",
    )

    payload = _MODULE._load_payload(csv_path)

    assert payload == {
        "columns": ["source", "interface", "backfill_mode"],
        "records": [{"source": "tushare", "interface": "stock_daily", "backfill_mode": "date"}],
    }


def test_extract_input_params_supports_bixu_header():
        html = """
        <table>
            <thead>
                <tr><th>名称</th><th>类型</th><th>必须</th><th>描述</th></tr>
            </thead>
            <tbody>
                <tr><td>ts_code</td><td>str</td><td>N</td><td>股票代码</td></tr>
            </tbody>
        </table>
        """

        params = _MODULE._extract_input_params_from_html(html)

        assert params == [{"name": "ts_code", "type": "str", "required": "N", "description": "股票代码"}]


def test_analyze_method_classifies_range():
    method, date_params = _MODULE._analyze_method([
        _param("ts_code"),
        _param("start_date"),
        _param("end_date"),
    ])

    assert method == "range"
    assert date_params == ["start_date", "end_date"]


def test_analyze_method_classifies_code_date_when_both_axes_are_required():
    method, date_params = _MODULE._analyze_method([
        _param("ts_code", "Y"),
        _param("trade_date", "Y"),
    ])

    assert method == "code_date"
    assert date_params == ["trade_date"]


def test_analyze_method_classifies_date_when_code_is_optional():
    method, date_params = _MODULE._analyze_method([
        _param("ts_code", "N"),
        _param("trade_date", "Y"),
    ])

    assert method == "date"
    assert date_params == ["trade_date"]


def test_analyze_method_classifies_code_without_date_axis():
    method, date_params = _MODULE._analyze_method([
        _param("ts_code", "Y"),
    ])

    assert method == "code"
    assert date_params == []


def test_analyze_method_classifies_other_without_code_or_date_axis():
    method, date_params = _MODULE._analyze_method([
        _param("exchange", "Y"),
    ])

    assert method == "other"
    assert date_params == []


def test_rewrite_record_prefers_explicit_runtime_columns_and_doc_params(monkeypatch):
    monkeypatch.setattr(
        _MODULE,
        "_fetch_doc_metadata",
        lambda api_name, item_name: {
            "doc_id": "252",
            "doc_url": "https://tushare.pro/document/2?doc_id=252",
            "params": [
                {"name": "ts_code", "type": "str", "required": "N", "description": "股票代码"},
                {"name": "classify", "type": "str", "required": "N", "description": "股票分类"},
            ],
            "basis": "tushare_docs",
        },
    )

    rewritten = _MODULE._rewrite_record(
        {
            "source": "tushare",
            "interface": "us_basic",
            "interface_name": "美股列表",
            "supports_backfill": "True",
            "backfill_mode": "other",
            "backfill_logic": "旧分析逻辑",
            "slow_reason": "旧分析慢原因",
            "runtime_supports_backfill": "True",
            "runtime_backfill_mode": "code",
            "runtime_backfill_logic": "运行时按代码同步",
            "runtime_slow_reason": "运行时按代码逐个同步",
        },
        {},
    )

    assert rewritten["runtime_backfill_mode"] == "code"
    assert rewritten["runtime_backfill_logic"] == "运行时按代码同步"
    assert rewritten["backfill_mode"] == "code"
    assert rewritten["input_params"] == "ts_code, classify"
    assert rewritten["analysis_basis"] == "tushare_docs"


def test_rewrite_record_refetches_non_doc_cache_entries(monkeypatch):
    fetch_calls: list[tuple[str, str]] = []

    def _fake_fetch(api_name: str, item_name: str) -> dict[str, object]:
        fetch_calls.append((api_name, item_name))
        return {
            "doc_id": "252",
            "doc_url": "https://tushare.pro/document/2?doc_id=252",
            "params": [
                {"name": "ts_code", "type": "str", "required": "N", "description": "股票代码"},
            ],
            "basis": "tushare_docs",
        }

    monkeypatch.setattr(_MODULE, "_fetch_doc_metadata", _fake_fetch)

    cache = {
        "us_basic": {
            "doc_id": "",
            "doc_url": "",
            "params": [
                {"name": "trade_date", "type": "", "required": "", "description": "runtime fallback"},
            ],
            "basis": "runtime_fallback",
        }
    }

    rewritten = _MODULE._rewrite_record(
        {
            "source": "tushare",
            "interface": "us_basic",
            "interface_name": "美股列表",
            "runtime_backfill_mode": "code",
        },
        cache,
    )

    assert fetch_calls == [("us_basic", "美股列表")]
    assert cache["us_basic"]["basis"] == "tushare_docs"
    assert rewritten["analysis_basis"] == "tushare_docs"
    assert rewritten["input_params"] == "ts_code"


def test_fetch_doc_metadata_accepts_item_name_match_for_alias_api(monkeypatch):
    monkeypatch.setattr(_MODULE, "_search_doc_ids", lambda query: ["27"])
    monkeypatch.setattr(
        _MODULE,
        "_curl",
        lambda url: "<html><body><h1>A股日线行情</h1><p>接口：daily</p></body></html>",
    )
    monkeypatch.setattr(
        _MODULE,
        "_extract_input_params_from_html",
        lambda html: [{"name": "ts_code", "type": "str", "required": "N", "description": "股票代码"}],
    )

    metadata = _MODULE._fetch_doc_metadata("stock_daily", "A股日线行情")

    assert metadata["doc_id"] == "27"
    assert metadata["basis"] == "tushare_docs"
    assert metadata["params"][0]["name"] == "ts_code"


def test_fetch_doc_metadata_keeps_doc_basis_when_page_has_no_input_params(monkeypatch):
    monkeypatch.setattr(_MODULE, "_search_doc_ids", lambda query: ["118"])
    monkeypatch.setattr(
        _MODULE,
        "_curl",
        lambda url: (
            "<html><body><h1>基金管理人</h1><p>接口：fundcompany</p>"
            "<p><strong>输入参数</strong></p><p>无，可提取全部</p></body></html>"
        ),
    )
    monkeypatch.setattr(_MODULE, "_extract_input_params_from_html", lambda html: [])

    metadata = _MODULE._fetch_doc_metadata("fund_company", "基金管理人")

    assert metadata["doc_id"] == "118"
    assert metadata["basis"] == "tushare_docs"
    assert metadata["params"] == []


def test_rewrite_record_keeps_doc_basis_for_empty_param_docs(monkeypatch):
    monkeypatch.setattr(
        _MODULE,
        "_fetch_doc_metadata",
        lambda api_name, item_name: {
            "doc_id": "118",
            "doc_url": "https://tushare.pro/document/2?doc_id=118",
            "params": [],
            "basis": "tushare_docs",
        },
    )

    rewritten = _MODULE._rewrite_record(
        {
            "source": "tushare",
            "interface": "fund_company",
            "interface_name": "基金管理人",
            "runtime_backfill_mode": "other",
        },
        {},
    )

    assert rewritten["analysis_basis"] == "tushare_docs"
    assert rewritten["input_params"] == ""
    assert rewritten["backfill_mode"] == "other"
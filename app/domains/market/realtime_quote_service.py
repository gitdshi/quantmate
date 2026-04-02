"""Realtime quote fetchers for multiple markets via AkShare."""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from typing import Any, Callable, Iterable

import requests

try:
    import akshare as ak

    _AKSHARE_IMPORT_ERROR: Exception | None = None
except Exception as _e:  # pragma: no cover - environment dependent
    ak = None  # type: ignore[assignment]
    _AKSHARE_IMPORT_ERROR = _e
import pandas as pd

logger = logging.getLogger(__name__)

# ── In-memory TTL cache for expensive AkShare bulk calls ──────────────────
_BULK_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_BULK_CACHE_LOCK = threading.Lock()
_BULK_CACHE_TTL = 60  # seconds
_AKSHARE_TIMEOUT = 15  # seconds — max wait per AkShare bulk call
_TENCENT_TIMEOUT = 8  # seconds
_TENCENT_RETRIES = 2
_TENCENT_BACKOFF = 1.0  # seconds

_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="akshare")


def _get_cached_df(key: str) -> pd.DataFrame | None:
    with _BULK_CACHE_LOCK:
        entry = _BULK_CACHE.get(key)
        if entry and (time.monotonic() - entry[0]) < _BULK_CACHE_TTL:
            return entry[1]
    return None


def _set_cached_df(key: str, df: pd.DataFrame) -> None:
    with _BULK_CACHE_LOCK:
        _BULK_CACHE[key] = (time.monotonic(), df)


def _fetch_akshare_with_timeout(func: Callable[[], pd.DataFrame], cache_key: str) -> pd.DataFrame:
    """Execute an AkShare bulk fetch with cache + thread-pool timeout."""
    cached = _get_cached_df(cache_key)
    if cached is not None:
        return cached
    future = _executor.submit(func)
    try:
        df = future.result(timeout=_AKSHARE_TIMEOUT)
        _set_cached_df(cache_key, df)
        return df
    except FuturesTimeoutError:
        future.cancel()
        # Return stale cache if available
        with _BULK_CACHE_LOCK:
            entry = _BULK_CACHE.get(cache_key)
            if entry:
                logger.warning("AkShare timeout for %s, returning stale cache", cache_key)
                return entry[1]
        raise TimeoutError(f"AkShare call timed out after {_AKSHARE_TIMEOUT}s: {cache_key}")


class RealtimeQuoteService:
    """Fetch spot quotes for CN/HK/US/FX/Futures/Crypto via AkShare."""

    def get_quote(self, symbol: str, market: str = "CN") -> dict[str, Any]:
        sym = (symbol or "").strip()
        if not sym:
            raise ValueError("Symbol is required")

        market_key = (market or "").strip().upper() or "CN"

        if market_key in {"CN", "A", "A_SHARE", "SSE", "SZSE", "SH", "SZ"}:
            return self._quote_cn(sym)
        if market_key in {"CN_INDEX", "INDEX_CN", "INDEX"}:
            return self._quote_cn_index(sym)
        if market_key in {"HK", "HKEX"}:
            return self._quote_hk(sym)
        if market_key in {"US", "NYSE", "NASDAQ"}:
            return self._quote_us(sym)
        if market_key in {"FX", "FOREX"}:
            return self._quote_fx(sym)
        if market_key in {"FUT", "FUTURES"}:
            return self._quote_futures(sym)
        if market_key in {"CRYPTO", "DIGITAL", "COIN"}:
            return self._quote_crypto(sym)

        raise ValueError(f"Unsupported market: {market}")

    # ---- Helpers ----

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().astimezone().isoformat()

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            if isinstance(value, str):
                cleaned = value.replace("%", "").replace(",", "").strip()
                if cleaned == "":
                    return None
                return float(cleaned)
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            if isinstance(value, str):
                cleaned = value.replace(",", "").strip()
                if cleaned == "":
                    return None
                return int(float(cleaned))
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _normalize_symbol(value: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", value.upper())

    @staticmethod
    def _pick(row: pd.Series, keys: Iterable[str]) -> Any:
        for key in keys:
            if key in row:
                val = row.get(key)
                if val is not None and val != "":
                    return val
        return None

    # ---- Market handlers ----

    def _quote_cn(self, symbol: str) -> dict[str, Any]:
        code = symbol.split(".")[0].strip()
        parts = self._fetch_tencent_quote(code)
        return self._build_tencent_quote(code, parts, market="CN")

    def _quote_cn_index(self, symbol: str) -> dict[str, Any]:
        parts_symbol = symbol.split(".")
        code = parts_symbol[0].strip()
        suffix = parts_symbol[1].strip().upper() if len(parts_symbol) > 1 else ""

        # Index quotes need explicit exchange prefix when suffix is provided.
        if suffix in {"SH", "SSE"}:
            parts = self._fetch_tencent_quote_with_prefix(code, "sh")
        elif suffix in {"SZ", "SZSE"}:
            parts = self._fetch_tencent_quote_with_prefix(code, "sz")
        else:
            parts = self._fetch_tencent_quote(code)
        return self._build_tencent_quote(code, parts, market="CN_INDEX")

    def _fetch_tencent_quote(self, code: str) -> list[str]:
        prefix = "sh" if code.startswith(("6", "9", "5", "68")) else "sz"
        url = f"https://qt.gtimg.cn/q={prefix}{code}"
        return self._tencent_request_with_retry(url, code)

    @staticmethod
    def _tencent_request_with_retry(url: str, code: str) -> list[str]:
        last_exc: Exception | None = None
        for attempt in range(_TENCENT_RETRIES + 1):
            try:
                resp = requests.get(url, timeout=_TENCENT_TIMEOUT)
                resp.raise_for_status()
                raw = resp.text
                if "=" not in raw:
                    raise ValueError(f"Invalid Tencent quote response for {code}")
                payload = raw.split("=", 1)[1].strip().strip('";')
                parts = payload.replace("\r", "").replace("\n", "").split("~")
                if len(parts) < 6:
                    raise ValueError(f"Symbol not found in Tencent quote: {code}")
                return parts
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                if attempt < _TENCENT_RETRIES:
                    time.sleep(_TENCENT_BACKOFF * (attempt + 1))
            except Exception as exc:
                raise exc
        raise last_exc or TimeoutError(f"Tencent quote failed after retries: {code}")

    def _build_tencent_quote(self, code: str, parts: list[str], market: str) -> dict[str, Any]:
        def to_f(idx: int) -> float | None:
            if idx >= len(parts):
                return None
            return self._to_float(parts[idx])

        price = to_f(3)
        prev_close = to_f(4)
        change = price - prev_close if (price is not None and prev_close) else None
        change_pct = (change / prev_close * 100) if (change is not None and prev_close) else None

        amount = None
        if len(parts) > 35 and "/" in parts[35]:
            try:
                amount = float(parts[35].split("/")[2])
            except Exception:
                amount = None

        return {
            "symbol": code,
            "name": parts[1] if len(parts) > 1 else None,
            "price": price,
            "last_price": price,
            "change": change,
            "change_percent": change_pct,
            "open": to_f(5),
            "high": to_f(33),
            "low": to_f(34),
            "prev_close": prev_close,
            "volume": self._to_int(parts[6]) if len(parts) > 6 else None,
            "amount": amount,
            "market": market,
            "currency": "CNY",
            "source": "tencent:qt",
            "asof": self._now_iso(),
            "delayed": False,
        }

    def _quote_hk(self, symbol: str) -> dict[str, Any]:
        code = symbol.split(".")[0].strip().zfill(5)
        # Try Tencent HK quote first (faster, individual stock)
        try:
            return self._quote_hk_tencent(code)
        except Exception:
            pass
        # Fallback to akshare bulk (cached)
        if ak is None:
            raise RuntimeError(f"AkShare not available: {_AKSHARE_IMPORT_ERROR}")
        df = _fetch_akshare_with_timeout(ak.stock_hk_spot, "hk_spot")
        if "symbol" not in df.columns:
            raise ValueError("Unexpected response for HK spot quotes")
        row = df.loc[df["symbol"].astype(str) == code]
        if row.empty:
            raise ValueError(f"Symbol not found in HK spot data: {symbol}")
        r = row.iloc[0]
        price = self._to_float(self._pick(r, ["lasttrade"]))
        return {
            "symbol": code,
            "name": self._pick(r, ["name", "engname"]),
            "price": price,
            "last_price": price,
            "change": self._to_float(self._pick(r, ["pricechange"])),
            "change_percent": self._to_float(self._pick(r, ["changepercent"])),
            "open": self._to_float(self._pick(r, ["open"])),
            "high": self._to_float(self._pick(r, ["high"])),
            "low": self._to_float(self._pick(r, ["low"])),
            "prev_close": self._to_float(self._pick(r, ["prevclose"])),
            "volume": self._to_float(self._pick(r, ["volume"])),
            "amount": self._to_float(self._pick(r, ["amount"])),
            "market": "HK",
            "currency": "HKD",
            "source": "akshare:stock_hk_spot",
            "asof": str(self._pick(r, ["ticktime"]) or self._now_iso()),
            "delayed": True,
        }

    def _quote_hk_tencent(self, code: str) -> dict[str, Any]:
        """Fetch individual HK stock quote via Tencent API (faster than bulk akshare)."""
        url = f"https://qt.gtimg.cn/q=hk{code}"
        parts = self._tencent_request_with_retry(url, code)
        if not parts[3]:
            raise ValueError(f"HK symbol not found: {code}")

        def to_f(idx: int) -> float | None:
            if idx >= len(parts):
                return None
            return self._to_float(parts[idx])

        price = to_f(3)
        prev_close = to_f(4)
        change = price - prev_close if (price is not None and prev_close) else None
        change_pct = (change / prev_close * 100) if (change is not None and prev_close) else None

        return {
            "symbol": code,
            "name": parts[1] if len(parts) > 1 else None,
            "price": price,
            "last_price": price,
            "change": change,
            "change_percent": change_pct,
            "open": to_f(5),
            "high": to_f(33) if len(parts) > 33 else None,
            "low": to_f(34) if len(parts) > 34 else None,
            "prev_close": prev_close,
            "volume": self._to_int(parts[6]) if len(parts) > 6 else None,
            "amount": None,
            "market": "HK",
            "currency": "HKD",
            "source": "tencent:qt_hk",
            "asof": self._now_iso(),
            "delayed": True,
        }

    def _quote_us(self, symbol: str) -> dict[str, Any]:
        if ak is None:
            raise RuntimeError(f"AkShare not available: {_AKSHARE_IMPORT_ERROR}")
        raw = symbol.split(".")[0].strip()
        sym = raw.upper()
        df = _fetch_akshare_with_timeout(ak.stock_us_spot_em, "us_spot")
        if "代码" not in df.columns:
            raise ValueError("Unexpected response for US spot quotes")
        codes = df["代码"].astype(str).str.upper().str.split(".").str[-1]
        row = df.loc[codes == sym]
        if row.empty:
            raise ValueError(f"Symbol not found in US spot data: {symbol}")
        r = row.iloc[0]
        price_us = self._to_float(self._pick(r, ["最新价"]))
        return {
            "symbol": sym,
            "name": self._pick(r, ["名称"]),
            "price": price_us,
            "last_price": price_us,
            "change": self._to_float(self._pick(r, ["涨跌额"])),
            "change_percent": self._to_float(self._pick(r, ["涨跌幅"])),
            "open": self._to_float(self._pick(r, ["开盘价"])),
            "high": self._to_float(self._pick(r, ["最高价"])),
            "low": self._to_float(self._pick(r, ["最低价"])),
            "prev_close": self._to_float(self._pick(r, ["昨收价"])),
            "volume": self._to_float(self._pick(r, ["成交量"])),
            "amount": self._to_float(self._pick(r, ["成交额"])),
            "market": "US",
            "currency": "USD",
            "source": "akshare:stock_us_spot_em",
            "asof": self._now_iso(),
            "delayed": True,
        }

    def _quote_fx(self, symbol: str) -> dict[str, Any]:
        if ak is None:
            raise RuntimeError(f"AkShare not available: {_AKSHARE_IMPORT_ERROR}")
        sym = self._normalize_symbol(symbol)
        df = _fetch_akshare_with_timeout(ak.forex_spot_em, "fx_spot")
        if "代码" not in df.columns:
            raise ValueError("Unexpected response for FX spot quotes")
        codes = df["代码"].astype(str).apply(self._normalize_symbol)
        row = df.loc[codes == sym]
        if row.empty:
            raise ValueError(f"Symbol not found in FX spot data: {symbol}")
        r = row.iloc[0]
        return {
            "symbol": sym,
            "name": self._pick(r, ["名称"]),
            "price": self._to_float(self._pick(r, ["最新价"])),
            "change": self._to_float(self._pick(r, ["涨跌额"])),
            "change_percent": self._to_float(self._pick(r, ["涨跌幅"])),
            "open": self._to_float(self._pick(r, ["今开"])),
            "high": self._to_float(self._pick(r, ["最高"])),
            "low": self._to_float(self._pick(r, ["最低"])),
            "prev_close": self._to_float(self._pick(r, ["昨收"])),
            "volume": None,
            "amount": None,
            "market": "FX",
            "currency": None,
            "source": "akshare:forex_spot_em",
            "asof": self._now_iso(),
            "delayed": False,
        }

    def _quote_futures(self, symbol: str) -> dict[str, Any]:
        if ak is None:
            raise RuntimeError(f"AkShare not available: {_AKSHARE_IMPORT_ERROR}")
        sym = self._normalize_symbol(symbol)
        df = _fetch_akshare_with_timeout(ak.futures_zh_spot, "futures_spot")
        if "symbol" not in df.columns:
            raise ValueError("Unexpected response for futures spot quotes")
        codes = df["symbol"].astype(str).apply(self._normalize_symbol)
        row = df.loc[codes == sym]
        if row.empty:
            raise ValueError(f"Symbol not found in futures spot data: {symbol}")
        r = row.iloc[0]
        return {
            "symbol": sym,
            "name": self._pick(r, ["name"]),
            "price": self._to_float(self._pick(r, ["current_price", "last"])),
            "change": self._to_float(self._pick(r, ["change"])),
            "change_percent": self._to_float(self._pick(r, ["change_pct"])),
            "open": self._to_float(self._pick(r, ["open"])),
            "high": self._to_float(self._pick(r, ["high"])),
            "low": self._to_float(self._pick(r, ["low"])),
            "prev_close": self._to_float(self._pick(r, ["prev_close", "pre_close"])),
            "volume": self._to_float(self._pick(r, ["volume"])),
            "amount": None,
            "market": "FUTURES",
            "currency": None,
            "source": "akshare:futures_zh_spot",
            "asof": str(self._pick(r, ["time", "date"]) or self._now_iso()),
            "delayed": False,
        }

    def _quote_crypto(self, symbol: str) -> dict[str, Any]:
        if ak is None:
            raise RuntimeError(f"AkShare not available: {_AKSHARE_IMPORT_ERROR}")
        sym = self._normalize_symbol(symbol)
        df = _fetch_akshare_with_timeout(ak.crypto_js_spot, "crypto_spot")
        if "交易品种" not in df.columns:
            raise ValueError("Unexpected response for crypto spot quotes")
        codes = df["交易品种"].astype(str).apply(self._normalize_symbol)
        row = df.loc[codes == sym]
        if row.empty:
            raise ValueError(f"Symbol not found in crypto spot data: {symbol}")
        r = row.iloc[0]
        return {
            "symbol": sym,
            "name": self._pick(r, ["市场"]),
            "price": self._to_float(self._pick(r, ["最近报价"])),
            "change": self._to_float(self._pick(r, ["涨跌额"])),
            "change_percent": self._to_float(self._pick(r, ["涨跌幅"])),
            "open": None,
            "high": self._to_float(self._pick(r, ["24小时最高", "24 小时最高"])),
            "low": self._to_float(self._pick(r, ["24小时最低", "24 小时最低"])),
            "prev_close": None,
            "volume": self._to_float(self._pick(r, ["24小时成交量", "24 小时成交量"])),
            "amount": None,
            "market": "CRYPTO",
            "currency": None,
            "source": "akshare:crypto_js_spot",
            "asof": str(self._pick(r, ["更新时间"]) or self._now_iso()),
            "delayed": False,
        }

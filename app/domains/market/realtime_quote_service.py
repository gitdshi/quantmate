"""Realtime quote fetchers for multiple markets via AkShare."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable

import akshare as ak
import pandas as pd


class RealtimeQuoteService:
    """Fetch spot quotes for CN/HK/US/FX/Futures/Crypto via AkShare."""

    def get_quote(self, symbol: str, market: str = "CN") -> dict[str, Any]:
        sym = (symbol or "").strip()
        if not sym:
            raise ValueError("Symbol is required")

        market_key = (market or "").strip().upper() or "CN"

        if market_key in {"CN", "A", "A_SHARE", "SSE", "SZSE", "SH", "SZ"}:
            return self._quote_cn(sym)
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
        df = ak.stock_zh_a_spot_em()
        if "代码" not in df.columns:
            raise ValueError("Unexpected response for CN spot quotes")
        row = df.loc[df["代码"].astype(str) == code]
        if row.empty:
            raise ValueError(f"Symbol not found in CN spot data: {symbol}")
        r = row.iloc[0]
        return {
            "symbol": code,
            "name": self._pick(r, ["名称"]),
            "price": self._to_float(self._pick(r, ["最新价"])),
            "change": self._to_float(self._pick(r, ["涨跌额"])),
            "change_percent": self._to_float(self._pick(r, ["涨跌幅"])),
            "open": self._to_float(self._pick(r, ["今开"])),
            "high": self._to_float(self._pick(r, ["最高"])),
            "low": self._to_float(self._pick(r, ["最低"])),
            "prev_close": self._to_float(self._pick(r, ["昨收"])),
            "volume": self._to_int(self._pick(r, ["成交量"])),
            "amount": self._to_float(self._pick(r, ["成交额"])),
            "market": "CN",
            "currency": "CNY",
            "source": "akshare:stock_zh_a_spot_em",
            "asof": self._now_iso(),
            "delayed": False,
        }

    def _quote_hk(self, symbol: str) -> dict[str, Any]:
        code = symbol.split(".")[0].strip().zfill(5)
        df = ak.stock_hk_spot()
        if "symbol" not in df.columns:
            raise ValueError("Unexpected response for HK spot quotes")
        row = df.loc[df["symbol"].astype(str) == code]
        if row.empty:
            raise ValueError(f"Symbol not found in HK spot data: {symbol}")
        r = row.iloc[0]
        return {
            "symbol": code,
            "name": self._pick(r, ["name", "engname"]),
            "price": self._to_float(self._pick(r, ["lasttrade"])),
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

    def _quote_us(self, symbol: str) -> dict[str, Any]:
        raw = symbol.split(".")[0].strip()
        sym = raw.upper()
        df = ak.stock_us_spot_em()
        if "代码" not in df.columns:
            raise ValueError("Unexpected response for US spot quotes")
        codes = df["代码"].astype(str).str.upper().str.split(".").str[-1]
        row = df.loc[codes == sym]
        if row.empty:
            raise ValueError(f"Symbol not found in US spot data: {symbol}")
        r = row.iloc[0]
        return {
            "symbol": sym,
            "name": self._pick(r, ["名称"]),
            "price": self._to_float(self._pick(r, ["最新价"])),
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
        sym = self._normalize_symbol(symbol)
        df = ak.forex_spot_em()
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
        sym = self._normalize_symbol(symbol)
        df = ak.futures_zh_spot()
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
        sym = self._normalize_symbol(symbol)
        df = ak.crypto_js_spot()
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

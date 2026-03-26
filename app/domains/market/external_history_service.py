"""External history K-line service for non-CN markets."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

try:
    import akshare as ak
except Exception:
    ak = None  # type: ignore[assignment]

from app.domains.market.multi_market_dao import MultiMarketDao

logger = logging.getLogger(__name__)


class ExternalHistoryService:
    """Fetch daily OHLCV data for HK/US (tushare DB) and CRYPTO/FUTURES (AkShare)."""

    def __init__(self) -> None:
        self._dao = MultiMarketDao()

    def get_history(
        self, market: str, symbol: str, start_date: date, end_date: date
    ) -> dict[str, Any]:
        market_key = market.upper()
        s = start_date.strftime("%Y%m%d")
        e = end_date.strftime("%Y%m%d")

        if market_key == "HK":
            return self._hk_history(symbol, s, e)
        if market_key == "US":
            return self._us_history(symbol, s, e)
        if market_key in {"CRYPTO", "COIN"}:
            return self._crypto_history(symbol, start_date, end_date)
        if market_key in {"FUTURES", "FUT"}:
            return self._futures_history(symbol, start_date, end_date)

        raise ValueError(f"Unsupported market for external history: {market}")

    # ── HK / US from tushare DB ────────────────────────────────────────

    def _hk_history(self, symbol: str, start: str, end: str) -> dict[str, Any]:
        ts_code = symbol if "." in symbol else f"{symbol}.HK"
        rows = self._dao.get_hk_daily(ts_code, start, end)
        bars = [self._tushare_row_to_bar(r) for r in rows]
        return {"market": "HK", "symbol": ts_code, "bars": bars}

    def _us_history(self, symbol: str, start: str, end: str) -> dict[str, Any]:
        ts_code = symbol if "." in symbol else symbol
        rows = self._dao.get_us_daily(ts_code, start, end)
        bars = [self._tushare_row_to_bar(r) for r in rows]
        return {"market": "US", "symbol": ts_code, "bars": bars}

    @staticmethod
    def _tushare_row_to_bar(row: dict[str, Any]) -> dict[str, Any]:
        td = str(row.get("trade_date", ""))
        dt = f"{td[:4]}-{td[4:6]}-{td[6:8]}T00:00:00" if len(td) == 8 else td
        return {
            "datetime": dt,
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("vol") or row.get("volume"),
        }

    # ── CRYPTO / FUTURES via AkShare ───────────────────────────────────

    def _crypto_history(self, symbol: str, start: date, end: date) -> dict[str, Any]:
        if ak is None:
            raise RuntimeError("AkShare is not available")
        try:
            df = ak.crypto_hist(
                symbol=symbol,
                period="daily",
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
            )
        except Exception as exc:
            logger.warning("crypto_hist failed for %s: %s", symbol, exc)
            raise ValueError(f"Cannot fetch crypto history for {symbol}: {exc}") from exc
        bars = self._df_to_bars(df)
        return {"market": "CRYPTO", "symbol": symbol.upper(), "bars": bars}

    def _futures_history(self, symbol: str, start: date, end: date) -> dict[str, Any]:
        if ak is None:
            raise RuntimeError("AkShare is not available")
        try:
            df = ak.futures_zh_daily_sina(symbol=symbol)
        except Exception as exc:
            logger.warning("futures_zh_daily_sina failed for %s: %s", symbol, exc)
            raise ValueError(f"Cannot fetch futures history for {symbol}: {exc}") from exc
        # Filter date range
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
            mask = (df["date"] >= start.strftime("%Y-%m-%d")) & (df["date"] <= end.strftime("%Y-%m-%d"))
            df = df.loc[mask]
        bars = self._df_to_bars(df)
        return {"market": "FUTURES", "symbol": symbol.upper(), "bars": bars}

    @staticmethod
    def _df_to_bars(df: Any) -> list[dict[str, Any]]:
        bars: list[dict[str, Any]] = []
        date_col = next((c for c in ("date", "Date", "datetime", "日期") if c in df.columns), None)
        open_col = next((c for c in ("open", "Open", "开盘价") if c in df.columns), None)
        high_col = next((c for c in ("high", "High", "最高价") if c in df.columns), None)
        low_col = next((c for c in ("low", "Low", "最低价") if c in df.columns), None)
        close_col = next((c for c in ("close", "Close", "收盘价") if c in df.columns), None)
        vol_col = next((c for c in ("volume", "Volume", "成交量", "vol") if c in df.columns), None)

        for _, row in df.iterrows():
            dt = str(row[date_col]) if date_col else ""
            if len(dt) == 10:
                dt += "T00:00:00"
            bars.append({
                "datetime": dt,
                "open": float(row[open_col]) if open_col and row.get(open_col) is not None else None,
                "high": float(row[high_col]) if high_col and row.get(high_col) is not None else None,
                "low": float(row[low_col]) if low_col and row.get(low_col) is not None else None,
                "close": float(row[close_col]) if close_col and row.get(close_col) is not None else None,
                "volume": float(row[vol_col]) if vol_col and row.get(vol_col) is not None else None,
            })
        return bars

"""Data service for market data operations."""

from datetime import date
from typing import List, Optional, Dict, Any
import pandas as pd

from app.infrastructure.config import get_settings
from app.utils.ts_utils import moving_average, pct_change

from app.domains.market.service import MarketService
from app.domains.market.realtime_quote_service import RealtimeQuoteService
from app.domains.market.realtime_quote_cache import RealtimeQuoteCache
from app.domains.system.dao.system_config_dao import SystemConfigDao

settings = get_settings()


class DataService:
    """Service for fetching and processing market data."""

    def __init__(self) -> None:
        self._market = MarketService()
        self._realtime = RealtimeQuoteService()
        self._realtime_cache = RealtimeQuoteCache()
        self._system_config = SystemConfigDao()

    def get_symbols(
        self, exchange: Optional[str] = None, keyword: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get list of available symbols."""
        return self._market.list_symbols(exchange=exchange, keyword=keyword, limit=limit, offset=offset)

    def get_history(
        self, vt_symbol: str, start_date: date, end_date: date, interval: str = "daily"
    ) -> List[Dict[str, Any]]:
        """Get historical OHLC data for a symbol."""
        return self._market.get_history(vt_symbol, start_date, end_date)

    def get_indicators(
        self, vt_symbol: str, start_date: date, end_date: date, indicators: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Compute technical indicators for a symbol."""
        # Get raw data
        bars = self.get_history(vt_symbol, start_date, end_date)
        if not bars:
            return {}

        # Convert to DataFrame
        df = pd.DataFrame(bars)
        df.set_index("datetime", inplace=True)

        result = {}

        for indicator in indicators:
            if indicator.startswith("ma_"):
                # Moving average
                try:
                    window = int(indicator.split("_")[1])
                    ma = moving_average(df["close"], window)
                    result[indicator] = [
                        {"datetime": idx, "value": v, "name": indicator} for idx, v in ma.dropna().items()
                    ]
                except (ValueError, IndexError):
                    continue

            elif indicator.startswith("ema_"):
                # Exponential moving average
                try:
                    window = int(indicator.split("_")[1])
                    ema = moving_average(df["close"], window, method="EMA")
                    result[indicator] = [
                        {"datetime": idx, "value": v, "name": indicator} for idx, v in ema.dropna().items()
                    ]
                except (ValueError, IndexError):
                    continue

            elif indicator == "returns":
                # Daily returns
                returns = pct_change(df["close"])
                result[indicator] = [
                    {"datetime": idx, "value": v, "name": indicator} for idx, v in returns.dropna().items()
                ]

            elif indicator == "volume_ma_20":
                # Volume 20-day MA
                vol_ma = moving_average(df["volume"], 20)
                result[indicator] = [
                    {"datetime": idx, "value": v, "name": indicator} for idx, v in vol_ma.dropna().items()
                ]

        return result

    def get_market_overview(self) -> Dict[str, Any]:
        """Get market overview statistics."""
        return self._market.market_overview()

    def get_sectors(self) -> List[Dict[str, Any]]:
        """Get sector information."""
        return self._market.sectors()

    def get_exchanges(self) -> List[Dict[str, Any]]:
        """Get exchange-level groupings with counts."""
        return self._market.exchanges()

    def get_symbols_by_filter(
        self,
        industry: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Get symbols filtered by industry and/or exchange (exchange derived from ts_code suffix)."""
        return self._market.symbols_by_filter(industry=industry, exchange=exchange, limit=limit)

    def get_indexes(self) -> List[Dict[str, str]]:
        """Return available index codes from akshare.index_daily with friendly labels."""
        return MarketService().list_benchmark_indexes()

    def get_realtime_quote(self, symbol: str, market: str = "CN") -> Dict[str, Any]:
        """Fetch a realtime spot quote for a single symbol."""
        if not self._realtime_enabled():
            raise PermissionError("Realtime quotes are disabled by system settings")
        if not self._market_enabled(market):
            raise PermissionError(f"Realtime quotes disabled for market: {market}")

        try:
            quote = self._realtime.get_quote(symbol=symbol, market=market)
            if self._cache_enabled():
                self._realtime_cache.record(market=market, symbol=symbol, quote=quote)
            return quote
        except Exception as e:
            if self._cache_enabled():
                cached = self._realtime_cache.get_latest(market=market, symbol=symbol)
                if cached:
                    return {
                        "symbol": symbol,
                        "market": market,
                        "price": cached["price"],
                        "asof": datetime.fromtimestamp(cached["ts"]).isoformat(),
                        "source": "cache",
                        "stale": True,
                        "error": str(e),
                    }
            raise

    def get_realtime_series(
        self, symbol: str, market: str = "CN", start_ts: int | None = None, end_ts: int | None = None
    ) -> Dict[str, Any]:
        """Return cached realtime quote series (intraday)."""
        if not self._realtime_enabled() or not self._market_enabled(market):
            return {"symbol": symbol, "market": market, "cached": False, "points": []}
        if not self._cache_enabled():
            return {"symbol": symbol, "market": market, "cached": False, "points": []}
        points = self._realtime_cache.get_series(market=market, symbol=symbol, start_ts=start_ts, end_ts=end_ts)
        return {"symbol": symbol, "market": market, "cached": True, "points": points}

    def _realtime_enabled(self) -> bool:
        cfg = self._system_config.get("realtime_quote_enabled")
        if not cfg:
            return True
        return str(cfg.get("config_value", "true")).strip().lower() in {"1", "true", "yes", "on"}

    def _cache_enabled(self) -> bool:
        cfg = self._system_config.get("realtime_quote_cache_enabled")
        if not cfg:
            return True
        return str(cfg.get("config_value", "true")).strip().lower() in {"1", "true", "yes", "on"}

    def _market_enabled(self, market: str) -> bool:
        market_key = market.strip().upper()
        if market_key in {"CN_INDEX", "INDEX_CN", "INDEX"}:
            market_key = "CN"
        cfg = self._system_config.get("realtime_quote_markets")
        if not cfg:
            return True
        raw = str(cfg.get("config_value", "")).strip()
        if not raw:
            return True
        enabled = {m.strip().upper() for m in raw.split(",") if m.strip()}
        return market_key in enabled

    def get_index_overview(self) -> Dict[str, Any]:
        """Fetch realtime index overview (CN indices)."""
        indexes = {
            "csi300": {"symbol": "000300.SH", "market": "CN_INDEX", "name": "CSI 300"},
            "sse": {"symbol": "000001.SH", "market": "CN_INDEX", "name": "SSE Composite"},
            "szse": {"symbol": "399001.SZ", "market": "CN_INDEX", "name": "SZSE Component"},
            "chinext": {"symbol": "399006.SZ", "market": "CN_INDEX", "name": "ChiNext"},
        }
        result: Dict[str, Any] = {}
        for key, meta in indexes.items():
            try:
                quote = self._realtime.get_quote(symbol=meta["symbol"], market=meta["market"])
                quote["display_name"] = meta["name"]
                result[key] = quote
            except Exception as e:
                result[key] = {"error": str(e), "display_name": meta["name"], "symbol": meta["symbol"]}
        return result

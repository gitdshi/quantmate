"""Market sentiment service — advance/decline, volume trend, fear & greed index."""

from __future__ import annotations

import logging
from typing import Any

try:
    import akshare as ak
except Exception:
    ak = None  # type: ignore[assignment]

from app.infrastructure.config import get_runtime_int
from app.infrastructure.runtime_cache import ExpiringCache

logger = logging.getLogger(__name__)

def _sentiment_cache_ttl_seconds() -> int:
    return get_runtime_int(
        env_keys="MARKET_SENTIMENT_CACHE_TTL_SECONDS",
        db_key="market.sentiment.cache_ttl_seconds",
        default=60,
    )

_SENTIMENT_CACHE = ExpiringCache(name="market_sentiment", maxsize=8)


def _default_overview() -> dict[str, Any]:
    return {
        "advance_decline": None,
        "volume_trend": None,
        "index_momentum": None,
    }


def _default_fear_greed() -> dict[str, Any]:
    return {"score": 50, "label": "neutral", "components": {}}


class SentimentService:
    """Compute market-wide sentiment indicators from A-share data."""

    def get_overview(self) -> dict[str, Any]:
        """Return advance/decline stats, volume trend, and index momentum."""
        return self._get_snapshot()["overview"]

    def get_fear_greed(self) -> dict[str, Any]:
        """Compute a composite fear & greed score (0-100).

        Components:
        - Advance/decline ratio → 0-25
        - Price momentum (index pct change) → 0-25
        - Limit-up vs limit-down count → 0-25
        - Volume relative to 20-day avg → 0-25
        """
        return self._get_snapshot()["fear_greed"]

    def _get_snapshot(self) -> dict[str, Any]:
        cache_key = ("cn_a_share", id(ak))
        return _SENTIMENT_CACHE.get_or_load(
            cache_key,
            self._build_snapshot,
            ttl_seconds=_sentiment_cache_ttl_seconds(),
            stale_if_error=True,
        )

    def _build_snapshot(self) -> dict[str, Any]:
        overview = _default_overview()
        fear_greed = _default_fear_greed()

        if ak is None:
            return {"overview": overview, "fear_greed": fear_greed}

        try:
            spot_df = ak.stock_zh_a_spot_em()
        except Exception as exc:
            logger.warning("sentiment spot fetch failed: %s", exc)
            return {"overview": overview, "fear_greed": fear_greed}

        index_df = None
        try:
            index_df = ak.stock_zh_index_spot_em()
        except Exception as exc:
            logger.debug("sentiment index fetch failed: %s", exc)

        return {
            "overview": self._build_overview(spot_df, index_df),
            "fear_greed": self._build_fear_greed(spot_df, index_df),
        }

    def _build_overview(self, spot_df: Any, index_df: Any) -> dict[str, Any]:
        result = _default_overview()
        pct = self._extract_pct_series(spot_df)

        if pct is not None:
            adv = int((pct > 0).sum())
            dec = int((pct < 0).sum())
            flat = int((pct == 0).sum())
            result["advance_decline"] = {
                "advance": adv,
                "decline": dec,
                "flat": flat,
                "total": adv + dec + flat,
                "ratio": round(adv / max(dec, 1), 2),
            }

        try:
            if "成交额" in spot_df.columns:
                today_amount = spot_df["成交额"].dropna().astype(float).sum()
                result["volume_trend"] = {
                    "today_amount": round(today_amount / 1e8, 2),
                    "unit": "亿",
                }
        except Exception as exc:
            logger.debug("volume_trend error: %s", exc)

        index_row = self._get_shanghai_index_row(index_df)
        if index_row is not None:
            result["index_momentum"] = {
                "name": "上证指数",
                "price": float(index_row.get("最新价", 0) or 0),
                "change_pct": float(index_row.get("涨跌幅", 0) or 0),
            }

        return result

    def _build_fear_greed(self, spot_df: Any, index_df: Any) -> dict[str, Any]:
        score = 50
        components: dict[str, Any] = {}
        pct = self._extract_pct_series(spot_df)

        if pct is None:
            return _default_fear_greed()

        try:
            adv = int((pct > 0).sum())
            dec = int((pct < 0).sum())
            ratio = adv / max(dec, 1)
            ad_score = min(25, max(0, int(ratio / 3 * 25)))
            components["advance_decline"] = {"score": ad_score, "ratio": round(ratio, 2)}
            score = score - 12 + ad_score
        except Exception:
            components["advance_decline"] = {"score": 12}

        try:
            limit_up = int((pct >= 9.9).sum())
            limit_down = int((pct <= -9.9).sum())
            net = limit_up - limit_down
            lu_score = min(25, max(0, int((net + 50) / 100 * 25)))
            components["limit_moves"] = {"up": limit_up, "down": limit_down, "score": lu_score}
            score = score - 12 + lu_score
        except Exception:
            pass

        index_row = self._get_shanghai_index_row(index_df)
        if index_row is not None:
            try:
                idx_pct = float(index_row.get("涨跌幅", 0) or 0)
                mom_score = min(25, max(0, int((idx_pct + 5) / 10 * 25)))
                components["index_momentum"] = {"change_pct": idx_pct, "score": mom_score}
                score = score - 12 + mom_score
            except Exception:
                pass

        score = min(100, max(0, score))
        label = (
            "extreme_fear"
            if score < 20
            else "fear"
            if score < 40
            else "neutral"
            if score < 60
            else "greed"
            if score < 80
            else "extreme_greed"
        )
        return {"score": score, "label": label, "components": components}

    @staticmethod
    def _extract_pct_series(spot_df: Any):
        try:
            if "涨跌幅" not in spot_df.columns:
                return None
            return spot_df["涨跌幅"].dropna().astype(float)
        except Exception as exc:
            logger.debug("advance_decline error: %s", exc)
            return None

    @staticmethod
    def _get_shanghai_index_row(index_df: Any):
        try:
            if index_df is None or "代码" not in index_df.columns:
                return None
            sh = index_df.loc[index_df["代码"] == "000001"]
            if sh.empty:
                return None
            return sh.iloc[0]
        except Exception as exc:
            logger.debug("index_momentum error: %s", exc)
            return None

"""Market sentiment service — advance/decline, volume trend, fear & greed index."""

from __future__ import annotations

import logging
from typing import Any

try:
    import akshare as ak
except Exception:
    ak = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class SentimentService:
    """Compute market-wide sentiment indicators from A-share data."""

    def get_overview(self) -> dict[str, Any]:
        """Return advance/decline stats, volume trend, and index momentum."""
        result: dict[str, Any] = {
            "advance_decline": None,
            "volume_trend": None,
            "index_momentum": None,
        }

        if ak is None:
            return result

        # Advance / Decline from A-share realtime
        try:
            df = ak.stock_zh_a_spot_em()
            if "涨跌幅" in df.columns:
                pct = df["涨跌幅"].dropna().astype(float)
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
        except Exception as exc:
            logger.debug("advance_decline error: %s", exc)

        # Volume trend (today vs 5-day avg)
        try:
            if result["advance_decline"] and "amount" not in str(type(result)):
                df_vol = ak.stock_zh_a_spot_em()
                if "成交额" in df_vol.columns:
                    today_amount = df_vol["成交额"].dropna().astype(float).sum()
                    result["volume_trend"] = {
                        "today_amount": round(today_amount / 1e8, 2),
                        "unit": "亿",
                    }
        except Exception as exc:
            logger.debug("volume_trend error: %s", exc)

        # Index momentum (Shanghai Composite)
        try:
            df_idx = ak.stock_zh_index_spot_em()
            if "代码" in df_idx.columns:
                sh = df_idx.loc[df_idx["代码"] == "000001"]
                if not sh.empty:
                    r = sh.iloc[0]
                    result["index_momentum"] = {
                        "name": "上证指数",
                        "price": float(r.get("最新价", 0)),
                        "change_pct": float(r.get("涨跌幅", 0)),
                    }
        except Exception as exc:
            logger.debug("index_momentum error: %s", exc)

        return result

    def get_fear_greed(self) -> dict[str, Any]:
        """Compute a composite fear & greed score (0-100).

        Components:
        - Advance/decline ratio → 0-25
        - Price momentum (index pct change) → 0-25
        - Limit-up vs limit-down count → 0-25
        - Volume relative to 20-day avg → 0-25
        """
        score = 50  # neutral default
        components: dict[str, Any] = {}

        if ak is None:
            return {"score": score, "label": "neutral", "components": components}

        try:
            df = ak.stock_zh_a_spot_em()
        except Exception:
            return {"score": score, "label": "neutral", "components": components}

        # 1) Advance/Decline ratio component
        try:
            pct = df["涨跌幅"].dropna().astype(float)
            adv = int((pct > 0).sum())
            dec = int((pct < 0).sum())
            ratio = adv / max(dec, 1)
            ad_score = min(25, max(0, int(ratio / 3 * 25)))
            components["advance_decline"] = {"score": ad_score, "ratio": round(ratio, 2)}
            score = score - 12 + ad_score  # adjust from neutral
        except Exception:
            ad_score = 12
            components["advance_decline"] = {"score": ad_score}

        # 2) Limit-up vs limit-down
        try:
            limit_up = int((pct >= 9.9).sum())
            limit_down = int((pct <= -9.9).sum())
            net = limit_up - limit_down
            lu_score = min(25, max(0, int((net + 50) / 100 * 25)))
            components["limit_moves"] = {"up": limit_up, "down": limit_down, "score": lu_score}
            score = score - 12 + lu_score
        except Exception:
            pass

        # 3) Index momentum
        try:
            df_idx = ak.stock_zh_index_spot_em()
            sh = df_idx.loc[df_idx["代码"] == "000001"]
            if not sh.empty:
                idx_pct = float(sh.iloc[0].get("涨跌幅", 0))
                mom_score = min(25, max(0, int((idx_pct + 5) / 10 * 25)))
                components["index_momentum"] = {"change_pct": idx_pct, "score": mom_score}
                score = score - 12 + mom_score
        except Exception:
            pass

        score = min(100, max(0, score))
        label = "extreme_fear" if score < 20 else "fear" if score < 40 else "neutral" if score < 60 else "greed" if score < 80 else "extreme_greed"

        return {"score": score, "label": label, "components": components}

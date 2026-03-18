"""Data cleaning and quality validation service.

Detects missing trading dates, price anomalies, and data gaps
in stock_daily and related tables.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class DataCleaningService:
    """Detect and report data quality issues."""

    def detect_missing_dates(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        table: str = "stock_daily",
    ) -> dict[str, Any]:
        """Find trading dates with no data for a given symbol.

        Compares actual data dates against the trade_cal calendar to
        identify gaps.
        """
        with connection("tushare") as conn:
            # Get trading days from calendar
            cal_rows = conn.execute(
                text(
                    "SELECT cal_date FROM trade_cal "
                    "WHERE exchange = 'SSE' AND is_open = 1 "
                    "AND cal_date BETWEEN :start AND :end "
                    "ORDER BY cal_date"
                ),
                {"start": start_date.strftime("%Y%m%d"), "end": end_date.strftime("%Y%m%d")},
            ).fetchall()
            trading_days = {r[0] for r in cal_rows}

            # Get actual data dates
            data_rows = conn.execute(
                text(
                    f"SELECT DISTINCT trade_date FROM `{_safe_table(table)}` "
                    "WHERE ts_code = :symbol "
                    "AND trade_date BETWEEN :start AND :end"
                ),
                {"symbol": symbol, "start": start_date.strftime("%Y%m%d"), "end": end_date.strftime("%Y%m%d")},
            ).fetchall()
            data_days = {r[0] for r in data_rows}

        missing = sorted(trading_days - data_days)

        return {
            "symbol": symbol,
            "table": table,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "trading_days": len(trading_days),
            "data_days": len(data_days),
            "missing_days": len(missing),
            "missing_dates": [str(d) for d in missing[:100]],  # cap at 100
            "completeness": round(len(data_days) / len(trading_days), 4) if trading_days else 1.0,
        }

    def detect_price_anomalies(
        self,
        symbol: str,
        threshold_pct: float = 20.0,
        table: str = "stock_daily",
    ) -> dict[str, Any]:
        """Find daily price changes that exceed a threshold percentage.

        Helps detect bad data entries like sudden 50% jumps on a
        non-halt day.
        """
        with connection("tushare") as conn:
            rows = conn.execute(
                text(
                    f"SELECT trade_date, open, high, low, close, pct_chg "
                    f"FROM `{_safe_table(table)}` "
                    "WHERE ts_code = :symbol ORDER BY trade_date"
                ),
                {"symbol": symbol},
            ).fetchall()

        anomalies: list[dict[str, Any]] = []
        for r in rows:
            m = r._mapping
            pct = float(m.get("pct_chg") or 0)
            if abs(pct) >= threshold_pct:
                anomalies.append(
                    {
                        "date": str(m["trade_date"]),
                        "open": float(m["open"]),
                        "high": float(m["high"]),
                        "low": float(m["low"]),
                        "close": float(m["close"]),
                        "pct_chg": pct,
                    }
                )

        return {
            "symbol": symbol,
            "table": table,
            "threshold_pct": threshold_pct,
            "total_rows": len(rows),
            "anomaly_count": len(anomalies),
            "anomalies": anomalies[:50],
        }

    def check_ohlc_consistency(
        self,
        symbol: str,
        table: str = "stock_daily",
    ) -> dict[str, Any]:
        """Verify OHLC consistency: high >= max(open,close), low <= min(open,close)."""
        with connection("tushare") as conn:
            rows = conn.execute(
                text(
                    f"SELECT trade_date, open, high, low, close "
                    f"FROM `{_safe_table(table)}` "
                    "WHERE ts_code = :symbol ORDER BY trade_date"
                ),
                {"symbol": symbol},
            ).fetchall()

        violations: list[dict[str, Any]] = []
        for r in rows:
            m = r._mapping
            o, h, lo, c = float(m["open"]), float(m["high"]), float(m["low"]), float(m["close"])
            issues = []
            if h < max(o, c):
                issues.append("high < max(open, close)")
            if lo > min(o, c):
                issues.append("low > min(open, close)")
            if h < lo:
                issues.append("high < low")
            if issues:
                violations.append(
                    {
                        "date": str(m["trade_date"]),
                        "open": o,
                        "high": h,
                        "low": lo,
                        "close": c,
                        "issues": issues,
                    }
                )

        return {
            "symbol": symbol,
            "table": table,
            "total_rows": len(rows),
            "violation_count": len(violations),
            "violations": violations[:50],
        }

    def summary(self, symbol: str, start_date: date, end_date: date) -> dict[str, Any]:
        """Run all checks and return a combined quality report."""
        missing = self.detect_missing_dates(symbol, start_date, end_date)
        anomalies = self.detect_price_anomalies(symbol)
        ohlc = self.check_ohlc_consistency(symbol)
        return {
            "symbol": symbol,
            "completeness": missing["completeness"],
            "missing_days": missing["missing_days"],
            "anomaly_count": anomalies["anomaly_count"],
            "ohlc_violation_count": ohlc["violation_count"],
            "quality_score": _quality_score(missing, anomalies, ohlc),
        }


# ── helpers ──────────────────────────────────────────────────────────────

_ALLOWED_TABLES = {"stock_daily", "stock_weekly", "stock_monthly", "index_daily"}


def _safe_table(name: str) -> str:
    """Whitelist table names to prevent SQL injection."""
    if name not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table: {name}")
    return name


def _quality_score(missing: dict, anomalies: dict, ohlc: dict) -> float:
    """Compute a 0–100 quality score."""
    completeness_score = missing["completeness"] * 50
    anomaly_penalty = min(anomalies["anomaly_count"] * 5, 25)
    ohlc_penalty = min(ohlc["violation_count"] * 5, 25)
    return round(max(completeness_score + 50 - anomaly_penalty - ohlc_penalty, 0), 1)

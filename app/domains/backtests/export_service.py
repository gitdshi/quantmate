"""Backtest report export service — PDF/HTML/CSV generation."""

from __future__ import annotations

import csv
import io
import json
from typing import Any


class BacktestExportService:
    """Export backtest results to various formats."""

    def to_csv(self, result: dict[str, Any]) -> str:
        """Export backtest result to CSV string."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Summary section
        writer.writerow(["Backtest Report"])
        writer.writerow([])
        writer.writerow(["Metric", "Value"])
        metrics = result.get("metrics", {})
        for key, value in metrics.items():
            writer.writerow([key, value])

        # Trades section
        trades = result.get("trades", [])
        if trades:
            writer.writerow([])
            writer.writerow(["Trades"])
            if trades:
                headers = list(trades[0].keys())
                writer.writerow(headers)
                for trade in trades:
                    writer.writerow([trade.get(h) for h in headers])

        return output.getvalue()

    def to_html(self, result: dict[str, Any]) -> str:
        """Export backtest result to HTML report."""
        metrics = result.get("metrics", {})
        trades = result.get("trades", [])
        params = result.get("parameters", {})

        html_parts = [
            "<!DOCTYPE html><html><head><meta charset='utf-8'>",
            "<title>Backtest Report</title>",
            "<style>body{font-family:sans-serif;margin:20px}table{border-collapse:collapse;width:100%}",
            "th,td{border:1px solid #ddd;padding:8px;text-align:left}th{background:#f4f4f4}</style></head><body>",
            "<h1>Backtest Report</h1>",
        ]

        # Parameters
        if params:
            html_parts.append("<h2>Parameters</h2><table><tr><th>Parameter</th><th>Value</th></tr>")
            for k, v in params.items():
                html_parts.append(f"<tr><td>{k}</td><td>{v}</td></tr>")
            html_parts.append("</table>")

        # Metrics
        html_parts.append("<h2>Performance Metrics</h2><table><tr><th>Metric</th><th>Value</th></tr>")
        for k, v in metrics.items():
            html_parts.append(f"<tr><td>{k}</td><td>{v}</td></tr>")
        html_parts.append("</table>")

        # Trades
        if trades:
            html_parts.append("<h2>Trades</h2><table>")
            headers = list(trades[0].keys())
            html_parts.append("<tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr>")
            for trade in trades[:100]:  # Limit to 100 trades
                html_parts.append("<tr>" + "".join(f"<td>{trade.get(h, '')}</td>" for h in headers) + "</tr>")
            html_parts.append("</table>")

        html_parts.append("</body></html>")
        return "".join(html_parts)

    def to_json(self, result: dict[str, Any]) -> str:
        """Export backtest result to formatted JSON."""
        return json.dumps(result, indent=2, default=str)

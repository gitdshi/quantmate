"""Feature Descriptor — generate structured descriptions of available data for LLM prompting.

Produces YAML/dict summaries that RD-Agent's hypothesis generator can consume
to know which data fields exist and what they represent.
"""

from __future__ import annotations

import logging
from typing import Any

from app.domains.factors.data_catalog import get_catalog_summary

logger = logging.getLogger(__name__)

_PROMPT_CONTEXT_FIELD_SAMPLE_LIMIT = 8

# Human-readable descriptions for each category
_CATEGORY_DESCRIPTIONS: dict[str, str] = {
    "price": "Stock price data including open, high, low, close, pre_close, and price change metrics",
    "volume": "Trading volume and turnover data including vol, amount, turnover_rate",
    "fundamental": "Valuation metrics including PE, PB, PS ratios and market capitalization",
    "flow": "Money flow data segmented by trader size (small/medium/large/extra-large)",
    "margin": "Margin trading data including financing balance, buy, repay, and securities lending",
    "dividend": "Dividend and bonus data including cash dividends and stock dividends",
    "technical": "Technical indicators and adjustment factors",
    "other": "Other numeric fields not classified into standard categories",
}


def build_feature_descriptor(
    include_descriptions: bool = True,
) -> dict[str, Any]:
    """Build a structured feature descriptor for LLM consumption.

    Returns a dict suitable for YAML serialization or direct inclusion
    in LLM prompts describing the factor mining feature space.
    """
    summary = get_catalog_summary()
    categories = summary.get("categories", {})

    descriptor: dict[str, Any] = {
        "available_features": {},
        "total_fields": summary.get("total_fields", 0),
        "sources": summary.get("sources", []),
    }

    for cat_name, fields in sorted(categories.items()):
        entry: dict[str, Any] = {"fields": sorted(fields), "count": len(fields)}
        if include_descriptions and cat_name in _CATEGORY_DESCRIPTIONS:
            entry["description"] = _CATEGORY_DESCRIPTIONS[cat_name]
        descriptor["available_features"][cat_name] = entry

    return descriptor


def build_prompt_context() -> str:
    """Build a natural-language context string for inclusion in LLM prompts.

    Returns a multi-line string describing available data fields.
    """
    desc = build_feature_descriptor(include_descriptions=True)
    lines = [
        "Available data fields for factor mining:",
        "Use the category counts as the full inventory. Sample fields below are representative only.",
        "",
    ]

    features = desc.get("available_features", {})
    for cat_name, info in sorted(features.items()):
        fields = info.get("fields", [])
        description = info.get("description", "")
        sample_fields = fields[:_PROMPT_CONTEXT_FIELD_SAMPLE_LIMIT]
        omitted_count = max(len(fields) - len(sample_fields), 0)

        lines.append(f"  {cat_name} ({len(fields)} fields)")
        if description:
            lines.append(f"    Description: {description}")
        if sample_fields:
            lines.append(f"    Sample fields: {', '.join(sample_fields)}")
        if omitted_count:
            lines.append(f"    Additional fields omitted from prompt: {omitted_count}")
        lines.append("")

    total = desc.get("total_fields", 0)
    sources = desc.get("sources", [])
    lines.append(f"Total: {total} numeric fields from {', '.join(sources)}")

    return "\n".join(lines)

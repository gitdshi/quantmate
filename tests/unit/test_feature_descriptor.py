"""Tests for Feature Descriptor — build_feature_descriptor and build_prompt_context."""
import pytest
from unittest.mock import patch

from app.domains.factors.feature_descriptor import (
    _CATEGORY_DESCRIPTIONS,
    build_feature_descriptor,
    build_prompt_context,
)


MOCK_SUMMARY = {
    "categories": {
        "price": ["close", "open", "high", "low"],
        "volume": ["vol", "amount"],
        "fundamental": ["pe_ttm", "pb"],
    },
    "total_fields": 8,
    "sources": ["tushare", "akshare"],
}


class TestBuildFeatureDescriptor:

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_returns_descriptor_structure(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        desc = build_feature_descriptor()
        assert "available_features" in desc
        assert "total_fields" in desc
        assert "sources" in desc
        assert desc["total_fields"] == 8
        assert len(desc["sources"]) == 2

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_categories_included(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        desc = build_feature_descriptor()
        features = desc["available_features"]
        assert "price" in features
        assert "volume" in features
        assert "fundamental" in features

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_category_entry_has_fields_and_count(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        desc = build_feature_descriptor()
        price = desc["available_features"]["price"]
        assert "fields" in price
        assert "count" in price
        assert price["count"] == 4
        assert sorted(price["fields"]) == ["close", "high", "low", "open"]

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_descriptions_included_by_default(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        desc = build_feature_descriptor(include_descriptions=True)
        price = desc["available_features"]["price"]
        assert "description" in price
        assert "price" in price["description"].lower()

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_descriptions_excluded(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        desc = build_feature_descriptor(include_descriptions=False)
        price = desc["available_features"]["price"]
        assert "description" not in price

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_empty_catalog(self, mock_summary):
        mock_summary.return_value = {"categories": {}, "total_fields": 0, "sources": []}

        desc = build_feature_descriptor()
        assert desc["total_fields"] == 0
        assert desc["available_features"] == {}

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_fields_are_sorted(self, mock_summary):
        mock_summary.return_value = {
            "categories": {"price": ["high", "close", "open", "low"]},
            "total_fields": 4,
            "sources": ["tushare"],
        }

        desc = build_feature_descriptor()
        fields = desc["available_features"]["price"]["fields"]
        assert fields == sorted(fields)


class TestBuildPromptContext:

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_returns_string(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        ctx = build_prompt_context()
        assert isinstance(ctx, str)
        assert len(ctx) > 0

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_contains_category_names(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        ctx = build_prompt_context()
        assert "price" in ctx
        assert "volume" in ctx
        assert "fundamental" in ctx

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_contains_field_names(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        ctx = build_prompt_context()
        assert "close" in ctx
        assert "vol" in ctx
        assert "pe_ttm" in ctx

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_contains_total_and_sources(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        ctx = build_prompt_context()
        assert "8" in ctx
        assert "tushare" in ctx

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_contains_descriptions(self, mock_summary):
        mock_summary.return_value = MOCK_SUMMARY

        ctx = build_prompt_context()
        # Should contain the description text from _CATEGORY_DESCRIPTIONS
        assert "Description:" in ctx

    @patch("app.domains.factors.feature_descriptor.get_catalog_summary")
    def test_empty_catalog(self, mock_summary):
        mock_summary.return_value = {"categories": {}, "total_fields": 0, "sources": []}

        ctx = build_prompt_context()
        assert isinstance(ctx, str)
        assert "0" in ctx


class TestCategoryDescriptions:
    """Tests for _CATEGORY_DESCRIPTIONS constant."""

    def test_has_standard_categories(self):
        assert "price" in _CATEGORY_DESCRIPTIONS
        assert "volume" in _CATEGORY_DESCRIPTIONS
        assert "fundamental" in _CATEGORY_DESCRIPTIONS
        assert "flow" in _CATEGORY_DESCRIPTIONS
        assert "margin" in _CATEGORY_DESCRIPTIONS
        assert "dividend" in _CATEGORY_DESCRIPTIONS
        assert "technical" in _CATEGORY_DESCRIPTIONS
        assert "other" in _CATEGORY_DESCRIPTIONS

    def test_descriptions_are_nonempty_strings(self):
        for cat, desc in _CATEGORY_DESCRIPTIONS.items():
            assert isinstance(desc, str), f"{cat} description is not a string"
            assert len(desc) > 5, f"{cat} description is too short"

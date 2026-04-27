"""Tests for Issue #12: Pagination helpers."""
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.api.pagination import PaginationParams, PaginatedResponse, PaginationMeta, paginate


class TestPaginationParams:
    """Test PaginationParams dependency."""

    def test_defaults(self):
        p = PaginationParams(page=1, page_size=20)
        assert p.page == 1
        assert p.page_size == 20

    def test_custom_values(self):
        p = PaginationParams(page=3, page_size=50)
        assert p.page == 3
        assert p.page_size == 50

    def test_offset_calculation(self):
        p = PaginationParams(page=1, page_size=20)
        assert p.offset == 0

        p = PaginationParams(page=2, page_size=20)
        assert p.offset == 20

        p = PaginationParams(page=3, page_size=10)
        assert p.offset == 20

    def test_limit_is_page_size(self):
        p = PaginationParams(page=1, page_size=50)
        assert p.limit == 50


class TestPaginateFunction:
    """Test the paginate() helper."""

    def test_basic_pagination(self):
        items = [1, 2, 3]
        params = PaginationParams(page=1, page_size=20)
        result = paginate(items, total=100, params=params)
        assert result["data"] == [1, 2, 3]
        assert result["meta"]["page"] == 1
        assert result["meta"]["page_size"] == 20
        assert result["meta"]["total"] == 100
        assert result["meta"]["total_pages"] == 5

    def test_total_pages_rounds_up(self):
        params = PaginationParams(page=1, page_size=20)
        result = paginate([], total=21, params=params)
        assert result["meta"]["total_pages"] == 2

    def test_empty_result(self):
        params = PaginationParams(page=1, page_size=20)
        result = paginate([], total=0, params=params)
        assert result["data"] == []
        assert result["meta"]["total"] == 0
        assert result["meta"]["total_pages"] == 1

    def test_last_page_partial(self):
        items = ["a"]
        params = PaginationParams(page=5, page_size=3)
        result = paginate(items, total=13, params=params)
        assert result["meta"]["page"] == 5
        assert result["meta"]["total_pages"] == 5


class TestPaginationModels:
    """Test Pydantic models for pagination."""

    def test_pagination_meta(self):
        meta = PaginationMeta(page=1, page_size=20, total=100, total_pages=5)
        assert meta.page == 1
        assert meta.total_pages == 5

    def test_paginated_response(self):
        resp = PaginatedResponse(
            data=[{"id": 1}],
            meta=PaginationMeta(page=1, page_size=20, total=1, total_pages=1),
        )
        assert len(resp.data) == 1
        assert resp.meta.total == 1


class TestPaginationEndpoint:
    """Integration test: pagination via a FastAPI endpoint."""

    @pytest.fixture
    def client(self):
        app = FastAPI()

        @app.get("/items")
        async def list_items(pagination: PaginationParams = Depends()):
            all_items = list(range(1, 56))  # 55 items
            total = len(all_items)
            page_data = all_items[pagination.offset:pagination.offset + pagination.limit]
            return paginate(page_data, total, pagination)

        return TestClient(app)

    def test_default_pagination(self, client):
        resp = client.get("/items")
        assert resp.status_code == 200
        body = resp.json()
        assert body["meta"]["page"] == 1
        assert body["meta"]["page_size"] == 20
        assert body["meta"]["total"] == 55
        assert body["meta"]["total_pages"] == 3
        assert len(body["data"]) == 20

    def test_custom_page(self, client):
        resp = client.get("/items?page=2&page_size=10")
        body = resp.json()
        assert body["meta"]["page"] == 2
        assert body["data"][0] == 11
        assert len(body["data"]) == 10

    def test_page_size_capped_at_100(self, client):
        resp = client.get("/items?page_size=200")
        assert resp.status_code == 422  # validation error

    def test_page_must_be_positive(self, client):
        resp = client.get("/items?page=0")
        assert resp.status_code == 422

    def test_last_page(self, client):
        resp = client.get("/items?page=3&page_size=20")
        body = resp.json()
        assert body["meta"]["page"] == 3
        assert len(body["data"]) == 15  # 55 - 40 = 15

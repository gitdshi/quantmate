"""Pagination utilities for QuantMate API (Issue #12)."""
from typing import Any, Generic, List, Optional, TypeVar

from fastapi import Query
from pydantic import BaseModel

T = TypeVar("T")

# Pagination defaults
DEFAULT_PAGE = 1
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100


class PaginationMeta(BaseModel):
    """Pagination metadata included in paginated responses."""
    page: int
    page_size: int
    total: int
    total_pages: int


class PaginatedResponse(BaseModel):
    """Standard paginated response envelope."""
    data: List[Any]
    meta: PaginationMeta


class PaginationParams:
    """FastAPI dependency for pagination query parameters."""

    def __init__(
        self,
        page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
        page_size: int = Query(
            DEFAULT_PAGE_SIZE,
            ge=1,
            le=MAX_PAGE_SIZE,
            description=f"Items per page (max {MAX_PAGE_SIZE})",
        ),
    ):
        self.page = page
        self.page_size = page_size

    @property
    def offset(self) -> int:
        """Calculate the SQL OFFSET from page/page_size."""
        return (self.page - 1) * self.page_size

    @property
    def limit(self) -> int:
        """Alias for page_size, for use with SQL LIMIT."""
        return self.page_size


def paginate(items: List[Any], total: int, params: PaginationParams) -> dict:
    """Build a standard paginated response dict.

    Args:
        items: The list of items for the current page.
        total: Total number of items across all pages.
        params: The pagination parameters used.

    Returns:
        Dict with ``data`` and ``meta`` keys.
    """
    total_pages = max(1, (total + params.page_size - 1) // params.page_size)
    return {
        "data": items,
        "meta": {
            "page": params.page,
            "page_size": params.page_size,
            "total": total,
            "total_pages": total_pages,
        },
    }

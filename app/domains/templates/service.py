"""Strategy template domain service."""
from __future__ import annotations

from typing import Any, Optional

from app.domains.templates.dao.template_dao import (
    StrategyTemplateDao, StrategyCommentDao, StrategyRatingDao,
)


class TemplateService:
    def __init__(self) -> None:
        self._tpl_dao = StrategyTemplateDao()
        self._comment_dao = StrategyCommentDao()
        self._rating_dao = StrategyRatingDao()

    # --- Templates ---

    def list_marketplace(self, category: Optional[str] = None,
                         limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        return self._tpl_dao.list_public(category=category, limit=limit, offset=offset)

    def count_marketplace(self, category: Optional[str] = None) -> int:
        return self._tpl_dao.count_public(category=category)

    def list_my_templates(self, user_id: int, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        return self._tpl_dao.list_for_user(user_id, limit=limit, offset=offset)

    def count_my_templates(self, user_id: int) -> int:
        return self._tpl_dao.count_for_user(user_id)

    def get_template(self, template_id: int) -> dict[str, Any]:
        row = self._tpl_dao.get(template_id)
        if not row:
            raise KeyError("Template not found")
        return row

    def create_template(self, user_id: int, name: str, code: str, **kwargs) -> dict[str, Any]:
        tpl_id = self._tpl_dao.create(user_id, name, code, **kwargs)
        return self.get_template(tpl_id)

    def update_template(self, user_id: int, template_id: int, **fields) -> dict[str, Any]:
        existing = self._tpl_dao.get(template_id)
        if not existing or existing["author_id"] != user_id:
            raise KeyError("Template not found")
        self._tpl_dao.update(template_id, user_id, **fields)
        return self.get_template(template_id)

    def delete_template(self, user_id: int, template_id: int) -> None:
        if not self._tpl_dao.delete(template_id, user_id):
            raise KeyError("Template not found")

    def clone_template(self, user_id: int, template_id: int) -> dict[str, Any]:
        """Clone a public template into user's own templates."""
        source = self._tpl_dao.get(template_id)
        if not source:
            raise KeyError("Template not found")
        self._tpl_dao.increment_downloads(template_id)
        new_id = self._tpl_dao.create(
            author_id=user_id,
            name=f"{source['name']} (copy)",
            code=source.get("code", ""),
            category=source.get("category"),
            description=source.get("description"),
            visibility="private",
        )
        return self.get_template(new_id)

    # --- Comments ---

    def list_comments(self, template_id: int) -> list[dict[str, Any]]:
        return self._comment_dao.list_for_template(template_id)

    def add_comment(self, template_id: int, user_id: int, content: str, parent_id: Optional[int] = None) -> int:
        self.get_template(template_id)  # existence check
        return self._comment_dao.create(template_id, user_id, content, parent_id)

    def delete_comment(self, comment_id: int, user_id: int) -> None:
        if not self._comment_dao.delete(comment_id, user_id):
            raise KeyError("Comment not found")

    # --- Ratings ---

    def get_ratings(self, template_id: int) -> dict[str, Any]:
        return self._rating_dao.get_for_template(template_id)

    def rate_template(self, template_id: int, user_id: int, rating: int, review: Optional[str] = None) -> dict[str, Any]:
        self.get_template(template_id)  # existence check
        if not 1 <= rating <= 5:
            raise ValueError("Rating must be between 1 and 5")
        self._rating_dao.upsert(template_id, user_id, rating, review)
        return self._rating_dao.get_for_template(template_id)

    def list_reviews(self, template_id: int) -> list[dict[str, Any]]:
        return self._rating_dao.list_for_template(template_id)

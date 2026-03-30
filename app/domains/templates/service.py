"""Strategy template domain service."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from app.domains.templates.dao.template_dao import (
    StrategyTemplateDao,
    StrategyCommentDao,
    StrategyRatingDao,
)
from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
from app.domains.composite.dao.composite_strategy_dao import CompositeStrategyDao


class TemplateService:
    def __init__(self) -> None:
        self._tpl_dao = StrategyTemplateDao()
        self._comment_dao = StrategyCommentDao()
        self._rating_dao = StrategyRatingDao()
        self._comp_dao = StrategyComponentDao()
        self._composite_dao = CompositeStrategyDao()

    # --- Templates ---

    def list_marketplace(
        self, category: Optional[str] = None, template_type: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return self._tpl_dao.list_public(category=category, template_type=template_type, limit=limit, offset=offset)

    def count_marketplace(self, category: Optional[str] = None, template_type: Optional[str] = None) -> int:
        return self._tpl_dao.count_public(category=category, template_type=template_type)

    def list_my_templates(
        self, user_id: int, source: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return self._tpl_dao.list_for_user(user_id, source=source, limit=limit, offset=offset)

    def count_my_templates(self, user_id: int, source: Optional[str] = None) -> int:
        return self._tpl_dao.count_for_user(user_id, source=source)

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
        """Clone a public template into user's own resources.

        Dispatches by template_type:
        - standalone → clone into strategy_templates (private)
        - component  → create a new strategy_component for the user
        - composite  → create a new composite_strategy + bindings from blueprint
        """
        source = self._tpl_dao.get(template_id)
        if not source:
            raise KeyError("Template not found")
        self._tpl_dao.increment_downloads(template_id)

        ttype = source.get("template_type", "standalone")

        if ttype == "component":
            return self._clone_as_component(user_id, source)
        elif ttype == "composite":
            return self._clone_as_composite(user_id, source)
        else:
            return self._clone_as_standalone(user_id, source)

    # -- private clone helpers --

    def _clone_as_standalone(self, user_id: int, source: dict[str, Any]) -> dict[str, Any]:
        new_id = self._tpl_dao.create(
            author_id=user_id,
            name=source["name"],
            code=source.get("code", ""),
            category=source.get("category"),
            description=source.get("description"),
            default_params=source.get("default_params"),
            visibility="private",
            source_template_id=source["id"],
            source="marketplace",
            template_type="standalone",
        )
        return {"target_type": "template", "target_id": new_id, "template": self.get_template(new_id)}

    def _clone_as_component(self, user_id: int, source: dict[str, Any]) -> dict[str, Any]:
        new_id = self._tpl_dao.create(
            author_id=user_id,
            name=source["name"],
            code=source.get("code", ""),
            category=source.get("category"),
            description=source.get("description"),
            default_params=source.get("default_params"),
            visibility="private",
            source_template_id=source["id"],
            source="marketplace",
            template_type="component",
        )
        return {"target_type": "template", "target_id": new_id, "template": self.get_template(new_id)}

    def _clone_as_composite(self, user_id: int, source: dict[str, Any]) -> dict[str, Any]:
        now = datetime.utcnow()
        composite_cfg = source.get("composite_config") or {}
        if isinstance(composite_cfg, str):
            composite_cfg = json.loads(composite_cfg)
        bindings_blueprint = composite_cfg.get("bindings", {})

        composite_id = self._composite_dao.insert(
            user_id=user_id,
            name=source["name"],
            description=source.get("description"),
            portfolio_config_json=None,
            market_constraints_json=None,
            execution_mode="backtest",
            created_at=now,
            updated_at=now,
        )

        # Resolve sub_type references → user's existing component ids
        user_components = self._comp_dao.list_for_user(user_id)
        sub_type_to_comp: dict[str, dict[str, Any]] = {}
        for c in user_components:
            sub_type_to_comp.setdefault(c["sub_type"], c)

        ordinal = 0
        for layer in ("universe", "trading", "risk"):
            sub_types = bindings_blueprint.get(layer, [])
            for st in sub_types:
                comp = sub_type_to_comp.get(st)
                if comp:
                    ordinal += 1
                    self._composite_dao.add_binding(
                        composite_id=composite_id,
                        binding={
                            "component_id": comp["id"],
                            "layer": layer,
                            "ordinal": ordinal,
                        },
                    )
        return {"target_type": "composite", "target_id": composite_id}

    # --- Comments ---

    def publish_template(self, user_id: int, template_id: int) -> dict[str, Any]:
        """Publish a personal template by creating a public copy in the marketplace."""
        original = self._tpl_dao.get(template_id)
        if not original or original["author_id"] != user_id:
            raise KeyError("Template not found")
        if original["visibility"] == "public":
            raise ValueError("Template is already public")
        new_id = self._tpl_dao.create(
            author_id=user_id,
            name=original["name"],
            code=original.get("code", ""),
            category=original.get("category"),
            description=original.get("description"),
            params_schema=original.get("params_schema") if isinstance(original.get("params_schema"), dict) else None,
            default_params=original.get("default_params") if isinstance(original.get("default_params"), dict) else None,
            visibility="public",
            source_template_id=template_id,
            source="personal",
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

    def rate_template(
        self, template_id: int, user_id: int, rating: int, review: Optional[str] = None
    ) -> dict[str, Any]:
        self.get_template(template_id)  # existence check
        if not 1 <= rating <= 5:
            raise ValueError("Rating must be between 1 and 5")
        self._rating_dao.upsert(template_id, user_id, rating, review)
        return self._rating_dao.get_for_template(template_id)

    def list_reviews(self, template_id: int) -> list[dict[str, Any]]:
        return self._rating_dao.list_for_template(template_id)

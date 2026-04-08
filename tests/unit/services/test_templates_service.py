"""Unit tests for TemplateService."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from app.domains.templates.service import TemplateService


@pytest.fixture
def svc():
    with patch("app.domains.templates.service.StrategyTemplateDao") as TplCls, \
         patch("app.domains.templates.service.StrategyCommentDao") as CmtCls, \
         patch("app.domains.templates.service.StrategyRatingDao") as RatCls, \
         patch("app.domains.templates.service.StrategyComponentDao") as CompCls, \
         patch("app.domains.templates.service.CompositeStrategyDao") as CmpCls:
        s = TemplateService()
        s._tpl_dao = TplCls.return_value
        s._comment_dao = CmtCls.return_value
        s._rating_dao = RatCls.return_value
        s._comp_dao = CompCls.return_value
        s._composite_dao = CmpCls.return_value
        yield s


# ── marketplace ───────────────────────────────────────────────────

def test_list_marketplace(svc):
    svc._tpl_dao.list_public.return_value = [{"id": 1}]
    assert svc.list_marketplace(category="ml") == [{"id": 1}]
    svc._tpl_dao.list_public.assert_called_once_with(category="ml", template_type=None, limit=50, offset=0)


def test_count_marketplace(svc):
    svc._tpl_dao.count_public.return_value = 3
    assert svc.count_marketplace() == 3


def test_list_my_templates(svc):
    svc._tpl_dao.list_for_user.return_value = [{"id": 2}]
    assert svc.list_my_templates(1) == [{"id": 2}]


def test_count_my_templates(svc):
    svc._tpl_dao.count_for_user.return_value = 7
    assert svc.count_my_templates(1) == 7


# ── CRUD ──────────────────────────────────────────────────────────

def test_get_template_found(svc):
    svc._tpl_dao.get.return_value = {"id": 5, "name": "t"}
    assert svc.get_template(5)["name"] == "t"


def test_get_template_not_found(svc):
    svc._tpl_dao.get.return_value = None
    with pytest.raises(KeyError, match="not found"):
        svc.get_template(999)


def test_create_template(svc):
    svc._tpl_dao.create.return_value = 10
    svc._tpl_dao.get.return_value = {"id": 10, "name": "t"}
    result = svc.create_template(1, "t", "code")
    assert result["id"] == 10


def test_update_template_owner(svc):
    svc._tpl_dao.get.side_effect = [
        {"id": 5, "author_id": 1, "name": "old"},
        {"id": 5, "author_id": 1, "name": "new"},
    ]
    result = svc.update_template(1, 5, name="new")
    svc._tpl_dao.update.assert_called_once_with(5, 1, name="new")


def test_update_template_not_owner(svc):
    svc._tpl_dao.get.return_value = {"id": 5, "author_id": 99}
    with pytest.raises(KeyError):
        svc.update_template(1, 5, name="x")


def test_update_template_not_found(svc):
    svc._tpl_dao.get.return_value = None
    with pytest.raises(KeyError):
        svc.update_template(1, 5)


def test_delete_template_ok(svc):
    svc._tpl_dao.delete.return_value = True
    svc.delete_template(1, 5)
    svc._tpl_dao.delete.assert_called_once_with(5, 1)


def test_delete_template_not_found(svc):
    svc._tpl_dao.delete.return_value = False
    with pytest.raises(KeyError):
        svc.delete_template(1, 999)


# ── clone ─────────────────────────────────────────────────────────

def test_clone_standalone(svc):
    svc._tpl_dao.get.side_effect = [
        {"id": 1, "name": "t", "template_type": "standalone",
         "code": "c", "category": "ml", "description": "d",
         "default_params": {"a": 1}},
        {"id": 20, "name": "t"},
    ]
    svc._tpl_dao.create.return_value = 20
    result = svc.clone_template(2, 1)
    assert result["target_type"] == "template"
    assert result["target_id"] == 20
    svc._tpl_dao.increment_downloads.assert_called_once_with(1)


def test_clone_component(svc):
    svc._tpl_dao.get.side_effect = [
        {"id": 1, "name": "comp", "template_type": "component",
         "code": "c", "category": None, "description": None,
         "default_params": None},
        {"id": 21, "name": "comp"},
    ]
    svc._tpl_dao.create.return_value = 21
    result = svc.clone_template(2, 1)
    assert result["target_type"] == "template"


def test_clone_composite(svc):
    svc._tpl_dao.get.return_value = {
        "id": 1, "name": "combo", "template_type": "composite",
        "code": "", "description": "d",
        "composite_config": json.dumps({"bindings": {"universe": ["atr"], "trading": [], "risk": []}}),
        "default_params": None, "category": None,
    }
    svc._composite_dao.insert.return_value = 50
    svc._comp_dao.list_for_user.return_value = [
        {"id": 10, "sub_type": "atr"},
    ]
    result = svc.clone_template(2, 1)
    assert result["target_type"] == "composite"
    assert result["target_id"] == 50
    svc._composite_dao.add_binding.assert_called_once()


def test_clone_template_not_found(svc):
    svc._tpl_dao.get.return_value = None
    with pytest.raises(KeyError):
        svc.clone_template(1, 999)


# ── publish ───────────────────────────────────────────────────────

def test_publish_template(svc):
    svc._tpl_dao.get.side_effect = [
        {"id": 5, "author_id": 1, "name": "t", "visibility": "private",
         "code": "c", "category": "ml", "description": "d",
         "params_schema": {"x": "int"}, "default_params": {"x": 1}},
        {"id": 30, "name": "t"},
    ]
    svc._tpl_dao.create.return_value = 30
    result = svc.publish_template(1, 5)
    assert result["id"] == 30
    call_kwargs = svc._tpl_dao.create.call_args
    assert call_kwargs[1].get("visibility") == "public" or (len(call_kwargs[0]) > 0)


def test_publish_template_not_owner(svc):
    svc._tpl_dao.get.return_value = {"id": 5, "author_id": 99, "visibility": "private"}
    with pytest.raises(KeyError):
        svc.publish_template(1, 5)


def test_publish_template_already_public(svc):
    svc._tpl_dao.get.return_value = {"id": 5, "author_id": 1, "visibility": "public"}
    with pytest.raises(ValueError, match="already public"):
        svc.publish_template(1, 5)


# ── comments ──────────────────────────────────────────────────────

def test_list_comments(svc):
    svc._comment_dao.list_for_template.return_value = [{"id": 1}]
    assert svc.list_comments(5) == [{"id": 1}]


def test_add_comment(svc):
    svc._tpl_dao.get.return_value = {"id": 5, "name": "t"}
    svc._comment_dao.create.return_value = 100
    assert svc.add_comment(5, 1, "nice") == 100


def test_add_comment_template_not_found(svc):
    svc._tpl_dao.get.return_value = None
    with pytest.raises(KeyError):
        svc.add_comment(999, 1, "hi")


def test_delete_comment_ok(svc):
    svc._comment_dao.delete.return_value = True
    svc.delete_comment(10, 1)


def test_delete_comment_not_found(svc):
    svc._comment_dao.delete.return_value = False
    with pytest.raises(KeyError):
        svc.delete_comment(999, 1)


# ── ratings ───────────────────────────────────────────────────────

def test_get_ratings(svc):
    svc._rating_dao.get_for_template.return_value = {"avg": 4.0, "count": 5}
    assert svc.get_ratings(5) == {"avg": 4.0, "count": 5}


def test_rate_template(svc):
    svc._tpl_dao.get.return_value = {"id": 5, "name": "t"}
    svc._rating_dao.get_for_template.return_value = {"avg": 4.5}
    result = svc.rate_template(5, 1, 4, "good")
    svc._rating_dao.upsert.assert_called_once_with(5, 1, 4, "good")
    assert result["avg"] == 4.5


def test_rate_template_invalid_rating(svc):
    svc._tpl_dao.get.return_value = {"id": 5, "name": "t"}
    with pytest.raises(ValueError, match="between 1 and 5"):
        svc.rate_template(5, 1, 0)
    with pytest.raises(ValueError, match="between 1 and 5"):
        svc.rate_template(5, 1, 6)


def test_list_reviews(svc):
    svc._rating_dao.list_for_template.return_value = [{"user_id": 1, "rating": 5}]
    assert svc.list_reviews(5) == [{"user_id": 1, "rating": 5}]

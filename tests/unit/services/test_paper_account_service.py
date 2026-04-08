"""Unit tests for app.domains.trading.paper_account_service."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import app.domains.trading.paper_account_service as _mod


@pytest.fixture()
def svc(monkeypatch):
    with patch.object(_mod, "PaperAccountDao") as MockDao:
        dao = MockDao.return_value
        s = _mod.PaperAccountService()
        s._dao = dao
        yield s, dao


class TestPaperAccountService:
    def test_create_account(self, svc):
        s, dao = svc
        dao.create.return_value = 42
        result = s.create_account(user_id=1, name="Test", initial_capital=100000)
        assert result["success"] is True
        assert result["account_id"] == 42
        dao.create.assert_called_once()

    def test_create_account_negative_capital(self, svc):
        s, dao = svc
        result = s.create_account(user_id=1, name="Bad", initial_capital=-100)
        assert result["success"] is False

    def test_create_account_bad_market(self, svc):
        s, dao = svc
        result = s.create_account(user_id=1, name="Bad", initial_capital=100000, market="XX")
        assert result["success"] is False

    def test_list_accounts(self, svc):
        s, dao = svc
        dao.list_by_user.return_value = [{"id": 1}, {"id": 2}]
        result = s.list_accounts(user_id=1)
        assert len(result) == 2

    def test_list_accounts_with_status(self, svc):
        s, dao = svc
        dao.list_by_user.return_value = [{"id": 1, "status": "active"}]
        result = s.list_accounts(user_id=1, status="active")
        assert len(result) == 1

    def test_get_account(self, svc):
        s, dao = svc
        dao.get_by_id.return_value = {"id": 1, "user_id": 1}
        result = s.get_account(account_id=1, user_id=1)
        assert result["id"] == 1

    def test_get_account_not_found(self, svc):
        s, dao = svc
        dao.get_by_id.return_value = None
        result = s.get_account(account_id=99, user_id=1)
        assert result is None

    def test_close_account(self, svc):
        s, dao = svc
        dao.close_account.return_value = True
        assert s.close_account(account_id=1, user_id=1) is True

    def test_freeze_funds(self, svc):
        s, dao = svc
        dao.freeze_funds.return_value = True
        assert s.freeze_funds(account_id=1, amount=5000) is True

    def test_release_funds(self, svc):
        s, dao = svc
        dao.release_funds.return_value = True
        assert s.release_funds(account_id=1, amount=5000) is True

    def test_settle_buy(self, svc):
        s, dao = svc
        dao.settle_buy.return_value = True
        assert s.settle_buy(account_id=1, frozen_amount=5000, actual_cost=4800) is True

    def test_settle_sell(self, svc):
        s, dao = svc
        dao.settle_sell.return_value = True
        assert s.settle_sell(account_id=1, proceeds=6000) is True

    def test_get_equity_curve(self, svc):
        s, dao = svc
        dao.get_by_id.return_value = {"id": 1}
        dao.get_equity_curve.return_value = [{"equity": 100000}]
        result = s.get_equity_curve(account_id=1, user_id=1)
        assert len(result) == 1

    def test_get_equity_curve_no_account(self, svc):
        s, dao = svc
        dao.get_by_id.return_value = None
        result = s.get_equity_curve(account_id=99, user_id=1)
        assert result == []

    def test_market_currency_mapping(self):
        assert _mod._MARKET_CURRENCY["CN"] == "CNY"
        assert _mod._MARKET_CURRENCY["HK"] == "HKD"
        assert _mod._MARKET_CURRENCY["US"] == "USD"

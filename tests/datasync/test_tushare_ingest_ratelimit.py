"""Unit tests for Tushare rate limit handling in tushare_ingest.

Target: P1-DSYNC-CODE-001 - Init 断点续跑 + 自适应限流实现
"""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Set required environment variables before importing tushare_ingest
os.environ.setdefault('TUSHARE_DATABASE_URL', 'mysql+pymysql://test:test@localhost:3306/testdb')
os.environ.setdefault('TUSHARE_TOKEN', 'test_token_123')

# Import the module under test
from app.datasync.service.tushare_ingest import (
    parse_retry_after,
    parse_rate_limit_scope,
    _is_rate_limit_error,
    call_pro,
    TushareQuotaExceededError,
)


class TestParseRetryAfter:
    """Test parse_retry_after extracts wait times from various error formats."""

    def test_english_seconds(self):
        msg = "Rate limit exceeded. Please retry after 5 seconds."
        assert parse_retry_after(msg) == 5.0

    def test_english_seconds_short(self):
        msg = "Too many requests. Retry after 10 sec."
        assert parse_retry_after(msg) == 10.0

    def test_english_minutes(self):
        msg = "Rate limit hit. Please wait 2 minutes before retrying."
        assert parse_retry_after(msg) == 120.0

    def test_english_milliseconds(self):
        msg = "Retry after 500ms"
        assert parse_retry_after(msg) == 0.5

    def test_chinese_seconds(self):
        msg = "接口访问太频繁，请60秒后重试"
        assert parse_retry_after(msg) == 60.0

    def test_chinese_minutes(self):
        msg = "请求过于频繁，请2分钟后重试"
        assert parse_retry_after(msg) == 120.0

    def test_number_only_with_unit(self):
        msg = "retry after 30 seconds"
        assert parse_retry_after(msg) == 30.0

    def test_no_wait_time(self):
        msg = "Some other error"
        assert parse_retry_after(msg) is None

    def test_empty_message(self):
        assert parse_retry_after("") is None
        assert parse_retry_after(None) is None

    def test_case_insensitive(self):
        msg = "RETRY AFTER 15 SECONDS"
        assert parse_retry_after(msg) == 15.0

    def test_chinese_quota_per_minute(self):
        msg = "抱歉，您每分钟最多访问该接口50次"
        assert parse_retry_after(msg) == 1.2

    def test_chinese_quota_per_hour(self):
        msg = "抱歉，您每小时最多访问该接口1次"
        assert parse_retry_after(msg) == 3600.0

    def test_chinese_quota_per_day(self):
        msg = "抱歉，您每天最多访问该接口50次"
        assert parse_retry_after(msg) == 1728.0


class TestParseRateLimitScope:
    def test_detects_daily_scope(self):
        assert parse_rate_limit_scope("抱歉，您每天最多访问该接口50次") == "day"

    def test_detects_hourly_scope(self):
        assert parse_rate_limit_scope("抱歉，您每小时最多访问该接口1次") == "hour"

    def test_detects_minute_scope(self):
        assert parse_rate_limit_scope("抱歉，您每分钟最多访问该接口5次") == "minute"


class TestIsRateLimitError:
    """Test _is_rate_limit_error identifies rate limit errors correctly."""

    def test_detects_rate_limit_phrase(self):
        assert _is_rate_limit_error("rate limit exceeded") is True

    def test_detects_too_many_requests(self):
        assert _is_rate_limit_error("Too Many Requests") is True

    def test_detects_chinese_phrases(self):
        assert _is_rate_limit_error("接口访问太频繁") is True
        assert _is_rate_limit_error("访问频率过高，请稍后重试") is True  # contains "频率" and "后重试"
        assert _is_rate_limit_error("抱歉，您每天最多访问该接口50次") is True

    def test_negative_case(self):
        assert _is_rate_limit_error("Connection timeout") is False
        assert _is_rate_limit_error("Invalid token") is False
        assert _is_rate_limit_error("") is False

    def test_frequency_token(self):
        assert _is_rate_limit_error("访问频率过高") is True


class TestCallProRateLimitBehavior:
    """Test call_pro handles rate limit errors with adaptive wait."""

    @patch('app.datasync.service.tushare_ingest.pro')
    def test_ratelimit_uses_parsed_wait(self, mock_pro):
        """When rate limit error includes retry-after, should use it."""
        # Arrange
        mock_func = MagicMock()
        mock_func.side_effect = [
            Exception("Rate limit. Retry after 2 seconds."),
            pd.DataFrame({'ts_code': ['000001.SZ']}),
        ]
        mock_pro.stock_basic = mock_func

        # Patch _is_rate_limit_error to return True
        with patch('app.datasync.service.tushare_ingest._is_rate_limit_error', return_value=True):
            with patch('app.datasync.service.tushare_ingest.time.sleep') as mock_sleep:
            # Act
                result = call_pro('stock_basic', max_retries=2)

            # Assert
            assert result is not None
            assert mock_func.call_count == 2
            mock_sleep.assert_called_once()
            assert mock_sleep.call_args_list[0][0][0] >= 2.0

    @patch('app.datasync.service.tushare_ingest.pro')
    def test_ratelimit_fallback_to_exponential_backoff(self, mock_pro):
        """When rate limit error but no parseable wait time, use exponential backoff."""
        # Arrange
        mock_func = MagicMock()
        mock_func.side_effect = [
            Exception("Too many requests"),
            pd.DataFrame({'ts_code': ['000001.SZ']}),
        ]
        mock_pro.stock_basic = mock_func

        with patch('app.datasync.service.tushare_ingest._is_rate_limit_error', return_value=True):
            with patch('app.datasync.service.tushare_ingest.parse_retry_after', return_value=None):
                with patch('time.sleep') as mock_sleep:
                    # Act
                    result = call_pro('stock_basic', max_retries=2, backoff_base=5)

                    # Assert
                    assert result is not None
                    assert mock_func.call_count == 2
                    # First backoff should be base * 2^(0) = 5 seconds
                    mock_sleep.assert_called()
                    first_sleep = mock_sleep.call_args_list[0][0][0]
                    assert first_sleep >= 5.0

    @patch('app.datasync.service.tushare_ingest.pro')
    def test_success_no_retry(self, mock_pro):
        """On first success, no retries should happen."""
        # Arrange
        mock_func = MagicMock(return_value=pd.DataFrame({'ts_code': ['000001.SZ']}))
        mock_pro.stock_basic = mock_func

        # Act
        with patch('app.datasync.service.tushare_ingest.time.sleep') as mock_sleep:
            result = call_pro('stock_basic')

        # Assert
        assert result is not None
        assert mock_func.call_count == 1
        mock_sleep.assert_not_called()

    @patch('time.sleep')
    @patch('app.datasync.service.tushare_ingest.pro')
    def test_rate_limit_respects_max_retries(self, mock_pro, mock_sleep):
        """After max_retries, should raise the last exception."""
        # Arrange
        mock_func = MagicMock()
        mock_func.side_effect = [
            Exception("Rate limit. Retry after 1 second.") for _ in range(4)
        ]
        mock_pro.stock_basic = mock_func

        with patch('app.datasync.service.tushare_ingest._is_rate_limit_error', return_value=True):
            with patch('app.datasync.service.tushare_ingest.parse_retry_after', return_value=1.0):
                # Act / Assert
                with pytest.raises(Exception):
                    call_pro('stock_basic', max_retries=2)
                # Should have attempted 2 times (initial + 1 retry)
                assert mock_func.call_count == 2

    @patch('app.datasync.service.tushare_ingest.pro')
    def test_daily_quota_pauses_without_retry(self, mock_pro):
        mock_func = MagicMock(side_effect=Exception("抱歉，您每天最多访问该接口50次"))
        mock_pro.stock_basic = mock_func

        with patch('app.datasync.service.tushare_ingest.time.sleep') as mock_sleep:
            with pytest.raises(TushareQuotaExceededError) as exc_info:
                call_pro('stock_basic', max_retries=3)

        assert exc_info.value.scope == 'day'
        assert mock_func.call_count == 1
        mock_sleep.assert_not_called()

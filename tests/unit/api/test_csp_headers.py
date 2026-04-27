"""Tests for Issue #4: CSP Security Headers.

Validates that nginx configs contain the required security headers.
"""
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

REQUIRED_HEADERS = [
    "X-Frame-Options",
    "X-Content-Type-Options",
    "X-XSS-Protection",
    "Referrer-Policy",
    "Content-Security-Policy",
    "Strict-Transport-Security",
    "Permissions-Policy",
]

NGINX_CONFIGS = [
    REPO_ROOT / "nginx" / "staging.conf",
]

# Portal config is in a sibling repo
PORTAL_CONFIG = REPO_ROOT.parent / "quantmate-portal" / "nginx.conf"


class TestStagingNginxHeaders:
    @pytest.fixture(autouse=True)
    def load_config(self):
        path = NGINX_CONFIGS[0]
        assert path.exists(), f"Config not found: {path}"
        with path.open() as f:
            self.content = f.read()

    @pytest.mark.parametrize("header", REQUIRED_HEADERS)
    def test_header_present(self, header):
        assert header in self.content, f"Missing header: {header}"

    def test_frame_options_deny(self):
        assert 'X-Frame-Options "DENY"' in self.content or "X-Frame-Options 'DENY'" in self.content

    def test_csp_includes_self(self):
        assert "default-src 'self'" in self.content

    def test_csp_frame_ancestors_none(self):
        assert "frame-ancestors 'none'" in self.content


class TestPortalNginxHeaders:
    @pytest.fixture(autouse=True)
    def load_config(self):
        if not PORTAL_CONFIG.exists():
            pytest.skip("Portal nginx.conf not found")
        with PORTAL_CONFIG.open() as f:
            self.content = f.read()

    @pytest.mark.parametrize("header", REQUIRED_HEADERS)
    def test_header_present(self, header):
        assert header in self.content, f"Missing header: {header}"

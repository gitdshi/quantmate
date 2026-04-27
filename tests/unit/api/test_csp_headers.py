"""Tests for Issue #4: CSP Security Headers.

Validates that nginx configs contain the required security headers.
"""
import os
import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

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
    os.path.join(REPO_ROOT, "nginx", "staging.conf"),
]

# Portal config is in a sibling repo
PORTAL_CONFIG = os.path.abspath(
    os.path.join(REPO_ROOT, "..", "quantmate-portal", "nginx.conf")
)


class TestStagingNginxHeaders:
    @pytest.fixture(autouse=True)
    def load_config(self):
        path = NGINX_CONFIGS[0]
        assert os.path.exists(path), f"Config not found: {path}"
        with open(path) as f:
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
        if not os.path.exists(PORTAL_CONFIG):
            pytest.skip("Portal nginx.conf not found")
        with open(PORTAL_CONFIG) as f:
            self.content = f.read()

    @pytest.mark.parametrize("header", REQUIRED_HEADERS)
    def test_header_present(self, header):
        assert header in self.content, f"Missing header: {header}"

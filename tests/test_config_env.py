"""Tests for environment-driven configuration (direct-access hosts, bastion names)."""

import pytest

from datasuite import helpers
from datasuite.helpers import direct_access_hosts, getenv_any, is_direct_access_host


def test_no_direct_hosts_by_default(monkeypatch):
    monkeypatch.delenv("DATASUITE_DIRECT_HOSTS", raising=False)
    assert direct_access_hosts() == ()
    assert is_direct_access_host("any-machine") is False


def test_direct_hosts_from_env(monkeypatch):
    monkeypatch.setenv("DATASUITE_DIRECT_HOSTS", "analytics-jupyter, etl-runner ,")
    assert direct_access_hosts() == ("analytics-jupyter", "etl-runner")
    assert is_direct_access_host("analytics-jupyter-7f9c") is True
    assert is_direct_access_host("laptop-01") is False


def test_direct_hosts_from_code(monkeypatch):
    monkeypatch.delenv("DATASUITE_DIRECT_HOSTS", raising=False)
    monkeypatch.setattr(helpers, "DIRECT_ACCESS_HOSTS", ("db-gateway",))
    assert is_direct_access_host("db-gateway-02") is True


def test_getenv_any_prefers_first_non_empty(monkeypatch):
    monkeypatch.setenv("NEW_NAME", "")
    monkeypatch.setenv("OLD_NAME", "legacy")
    assert getenv_any("NEW_NAME", "OLD_NAME") == "legacy"
    monkeypatch.setenv("NEW_NAME", "current")
    assert getenv_any("NEW_NAME", "OLD_NAME") == "current"
    assert getenv_any("MISSING_A", "MISSING_B", default="x") == "x"


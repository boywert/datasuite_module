"""Tests for helpers.py — get_hostname."""

import socket
from datasuite.helpers import get_hostname


def test_get_hostname_returns_string():
    result = get_hostname()
    assert isinstance(result, str)
    assert len(result) > 0


def test_get_hostname_matches_socket():
    assert get_hostname() == socket.gethostname()

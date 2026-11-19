# helpers — internal utility functions shared across datasuite modules
#
# This module is intentionally NOT exported from __init__.py.
# Other modules import it as:  from .helpers import get_hostname

import os
import socket

# Hosts that can reach the databases directly, so no SSH tunnel is needed.
# Matched as a substring of the machine's hostname.  Configure them with the
# DATASUITE_DIRECT_HOSTS environment variable (comma-separated), or extend this
# tuple in code.  Used by db_connect and parquet_store via is_direct_access_host().
DIRECT_ACCESS_HOSTS: tuple = ()


def getenv_any(*names, default=None):
    """Return the first non-empty environment variable among *names*."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def direct_access_hosts() -> tuple:
    """``DIRECT_ACCESS_HOSTS`` plus the hosts listed in ``DATASUITE_DIRECT_HOSTS``.

    The environment variable is read on every call, so it can be set after import.
    """
    raw = os.getenv("DATASUITE_DIRECT_HOSTS", "")
    from_env = tuple(h.strip() for h in raw.split(",") if h.strip())
    return tuple(DIRECT_ACCESS_HOSTS) + from_env


def get_hostname():
    """Return the machine's hostname."""
    return socket.gethostname()


def is_direct_access_host(hostname=None):
    """Return True if this machine can reach the databases without a tunnel.

    True when any configured direct-access host (see ``direct_access_hosts()``)
    appears in the hostname.  With nothing configured, always False, so
    connections go through the SSH tunnel.
    """
    if hostname is None:
        hostname = get_hostname()
    return any(h in hostname for h in direct_access_hosts())


def duckdb_to_arrow(con, sql):
    """Run *sql* on DuckDB connection *con* and return a ``pyarrow.Table``.

    duckdb 2.x removed ``.arrow()`` / ``.fetch_arrow_table()`` in favour of
    ``.to_arrow_table()``; this picks whichever the installed version has.
    The query goes through ``con.sql()`` (a relation), which duckdb 2.x
    converts to Arrow without the extra copy a DB-API result needs.
    """
    rel = con.sql(sql)
    if rel is None:
        raise ValueError("Statement returned no result set to convert to Arrow")
    for method in ("to_arrow_table", "fetch_arrow_table", "arrow"):
        fn = getattr(rel, method, None)
        if fn is not None:
            return fn()
    raise AttributeError("This duckdb version has no Arrow fetch method")

# db_connect — convenience classes for DB2 and Impala queries
#
# Provides DB2Connect and ImpalaConnect, which handle SSH tunnelling,
# credential resolution (via dbc_info / env vars), chunked fetching,
# and pandas ↔ polars output — so callers just write:
#
#     db2 = DB2Connect()
#     df  = db2.query("SELECT ...")
#
# The shared connection factories ``open_db2_connection()`` and
# ``open_impala_connection()`` are also defined here.  They return
# a raw (connection, tunnel) pair and are reused by DataStore in
# parquet_store.py to avoid duplicating the tunnelling logic.
#
# All heavy optional imports (ibm_db_dbi, impyla, sshtunnel) are
# graceful: missing packages set the binding to None so that the
# rest of the library can still be imported.  A clear error is
# raised only when the missing feature is actually used.

import pandas as pd
import polars as pl
import sys
import os
from typing import Optional, Union


# ibm_db_dbi requires the IBM CLIDRIVER DLLs to be on the path.
# On Windows the DLLs live inside the site-packages directory.
# We attempt a normal import first; if it fails we add the DLL directory and retry.
# If ibm_db is not installed at all, we set ibm_db_dbi to None so the rest of the
# module can still be imported (DB2Connect will fail at runtime instead).
ibm_db_dbi = None
try:
    import ibm_db_dbi
except Exception:
    try:
        dll_path = sys.executable.replace('python.exe', 'Lib\\site-packages\\clidriver\\bin')
        if os.path.isdir(dll_path):
            os.add_dll_directory(dll_path)
        import ibm_db_dbi
    except Exception:
        ibm_db_dbi = None

try:
    from impala.dbapi import connect
    from impala.util import as_pandas
except ImportError:
    connect = None
    as_pandas = None

try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    SSHTunnelForwarder = None

from .ssh_tunnel_connect import SSHTunnelConnect
from .helpers import is_direct_access_host, getenv_any
from .framework_config import _resolve_engine


class dbc_info:
    """
    Database connection defaults.

    All values are read from environment variables at import time so that
    credentials are never hard-coded in scripts.

    Environment variables
    ---------------------
    DB2_SERVER, DB2_USER, DB2_PASS          — IBM DB2 / IIAS target
    IMPALA_SERVER, IMPALA_USER, IMPALA_PASS — Impala / Hive target
    BASTION_SERVER, BASTION_USER            — SSH bastion / jump host
    BASTION_PRIVATE_KEY                     — path to the bastion private key file
    (the older names RETNL_SERVER / RETNL_USER / RETNL_PRIVATE_KEY are still
    read as a fallback)
    """
    db2_server = os.getenv('DB2_SERVER')
    db2_server_port = 50000
    db2_user = os.getenv('DB2_USER')
    db2_pass = os.getenv('DB2_PASS')

    impala_server = os.getenv('IMPALA_SERVER')
    impala_server_port = 21050
    impala_user = os.getenv('IMPALA_USER')
    impala_pass = os.getenv('IMPALA_PASS')

    local_host = '127.0.0.1'   # tunnel local-end address
    local_port = None           # assigned dynamically by SSHTunnelConnect

    remote_host = getenv_any('BASTION_SERVER', 'RETNL_SERVER')
    remote_port = 22
    remote_user = getenv_any('BASTION_USER', 'RETNL_USER')
    remote_private_key = getenv_any('BASTION_PRIVATE_KEY', 'RETNL_PRIVATE_KEY')


def _make_dbc(kwargs, owner):
    """Return a per-instance copy of ``dbc_info`` with *kwargs* applied.

    Overrides are set on a new ``dbc_info`` instance, so the class-level
    defaults (and every other connection object) are left untouched.
    Unknown names raise ``TypeError`` instead of being silently ignored.
    """
    dbc = dbc_info()
    for key, value in kwargs.items():
        if key.startswith('_') or not hasattr(dbc_info, key):
            raise TypeError("%s() got an unexpected keyword argument %r" % (owner, key))
        setattr(dbc, key, value)
    return dbc


# ─── shared connection factories ─────────────────────────────────────────────
# These return (raw_dbapi_connection, tunnel_or_None) and are used by both the
# convenience classes below AND by DataStore in parquet_store.py.

def open_db2_connection(dbc=None):
    """Open a raw DB2 DB-API connection using *dbc* (defaults to ``dbc_info``).

    On a direct-access host (``helpers.DIRECT_ACCESS_HOSTS``) connects directly; everywhere else tunnels via the
    bastion host.  Returns ``(connection, tunnel_or_None)``.
    """
    if dbc is None:
        dbc = dbc_info
    tunnel = None
    host, port = dbc.db2_server, dbc.db2_server_port
    try:
        if not is_direct_access_host():
            tunnel = SSHTunnelConnect(
                dbc.db2_server, dbc.db2_server_port,
                dbc.remote_host, dbc.remote_port,
                dbc.remote_user, dbc.remote_private_key,
                tunnel_type='ssh',
            )
            tunnel.connect()
            host, port = dbc.local_host, tunnel.local_port

        conn = ibm_db_dbi.connect(
            "DATABASE=BLUDB;HOSTNAME=%s;PORT=%d;PROTOCOL=TCPIP;UID=%s;PWD=%s"
            % (host, port, dbc.db2_user, dbc.db2_pass), '', '',
        )
        return conn, tunnel
    except Exception:
        if tunnel and tunnel.tunnel:
            tunnel.close()
        raise


def open_impala_connection(dbc=None):
    """Open a raw Impala DB-API connection using *dbc* (defaults to ``dbc_info``).

    On a direct-access host (``helpers.DIRECT_ACCESS_HOSTS``) connects directly; everywhere else tunnels via the
    bastion host.  Returns ``(connection, tunnel_or_None)``.
    """
    if dbc is None:
        dbc = dbc_info
    tunnel = None
    host, port = dbc.impala_server, dbc.impala_server_port
    try:
        if not is_direct_access_host():
            tunnel = SSHTunnelConnect(
                dbc.impala_server, dbc.impala_server_port,
                dbc.remote_host, dbc.remote_port,
                dbc.remote_user, dbc.remote_private_key,
                tunnel_type='python',
            )
            tunnel.connect()
            host, port = dbc.local_host, tunnel.local_port

        conn = connect(
            host=host, port=port,
            user=dbc.impala_user, password=dbc.impala_pass,
            auth_mechanism='LDAP',
        )
        return conn, tunnel
    except Exception:
        if tunnel and tunnel.tunnel:
            tunnel.close()
        raise


# ─── convenience query classes ───────────────────────────────────────────────

class DB2Connect:
    """
    Query IBM DB2 (IIAS) and return results as a DataFrame.

    Connection strategy
    -------------------
    - On a direct-access host (helpers.DIRECT_ACCESS_HOSTS): connects directly (the server is on the same network).
    - Everywhere else: opens an SSH tunnel via the bastion host first, then connects
      through the tunnel's local port.

    Credentials are read from dbc_info (env vars) but can be overridden by passing
    keyword arguments that match dbc_info attribute names to __init__().

    Example
    -------
    db2 = DB2Connect()
    df = db2.query("SELECT * FROM schema.table FETCH FIRST 100 ROWS ONLY")

    # Polars LazyFrame
    lf = db2.query("SELECT * FROM schema.table", engine="polars")
    """

    def __init__(self, **kwargs):
        # Allow callers to override individual dbc_info fields at init time,
        # e.g. DB2Connect(db2_server="other-host").  Only this instance changes.
        self.dbc = _make_dbc(kwargs, "DB2Connect")

    @staticmethod
    def _fetch_db2(conn, iias_sql, polars_mode=False):
        """
        Internal: execute SQL on an open DB2 connection and return the result.

        Reads in chunks of 100 000 rows to avoid loading huge result sets into
        memory all at once.  Does not close *conn*.

        Parameters
        ----------
        polars_mode : bool
            When True, each pandas chunk is immediately converted to a Polars
            DataFrame and the pandas copy is discarded. This keeps peak memory
            lower than converting the fully-concatenated pandas DataFrame at the end.
        """
        if polars_mode:
            frames = []
            for chunk in pd.read_sql(iias_sql, con=conn, chunksize=100000):
                frames.append(pl.from_pandas(chunk))
            return pl.concat(frames).lazy() if frames else pl.LazyFrame()
        dfs = []
        for chunk in pd.read_sql(iias_sql, con=conn, chunksize=100000):
            dfs.append(chunk)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def query_db2(self, iias_sql, host, port, user, password, polars_mode=False):
        """
        Open a DB2 connection to *host*:*port*, run *iias_sql*, and close it.

        Kept for backward compatibility; ``query()`` reuses the connection
        from ``open_db2_connection()`` instead of calling this.
        """
        conn = ibm_db_dbi.connect(
            "DATABASE=BLUDB;HOSTNAME=%s;PORT=%d;PROTOCOL=TCPIP;UID=%s;PWD=%s" % (host, port, user, password), '', ''
        )
        try:
            return self._fetch_db2(conn, iias_sql, polars_mode=polars_mode)
        finally:
            conn.close()

    def query(self, sql: str, engine: Optional[str] = None) -> Union[pd.DataFrame, pl.LazyFrame]:
        """
        Run a SQL query against DB2 and return the results.

        Parameters
        ----------
        sql : str
            DB2-compatible SQL statement.
        engine : str, optional
            "pandas" → pd.DataFrame  |  "polars" → pl.LazyFrame
            Defaults to FrameworkConfig.dataframe_engine.
        """
        resolved = _resolve_engine(engine)
        conn, tunnel = None, None
        try:
            conn, tunnel = open_db2_connection(self.dbc)
            return self._fetch_db2(conn, sql, polars_mode=(resolved == 'polars'))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if tunnel and tunnel.tunnel:
                tunnel.close()


class ImpalaConnect:
    """
    Query Impala / Hive and return results as a DataFrame.

    Connection strategy mirrors DB2Connect: direct on a direct-access host, tunnelled
    elsewhere. Authentication uses LDAP.

    Example
    -------
    impala = ImpalaConnect()
    df = impala.query("SELECT * FROM db.table LIMIT 100")

    # Polars — no pandas intermediate, fetched in chunks of 100k rows
    lf = impala.query("SELECT * FROM db.table", engine="polars")
    """

    def __init__(self, **kwargs):
        self.dbc = _make_dbc(kwargs, "ImpalaConnect")

    def query(self, sql: str, engine: Optional[str] = None) -> Union[pd.DataFrame, pl.LazyFrame]:
        """
        Run a SQL query against Impala and return the results.

        Parameters
        ----------
        sql : str
            Impala-compatible SQL statement.
        engine : str, optional
            "pandas" → pd.DataFrame  |  "polars" → pl.LazyFrame
            Defaults to FrameworkConfig.dataframe_engine.
        """
        resolved = _resolve_engine(engine)
        fetch_fn = self._fetch_impala_polars if resolved == 'polars' else self._fetch_impala
        conn, tunnel = None, None
        try:
            conn, tunnel = open_impala_connection(self.dbc)
            return fetch_fn(conn, sql)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if tunnel and tunnel.tunnel:
                tunnel.close()

    @staticmethod
    def _open(host, port, user, password):
        return connect(host=host, port=port, user=user, password=password, auth_mechanism='LDAP')

    def query_impala(self, sql, host, port, user, password):
        """Open a connection, fetch all rows as a pandas DataFrame, and close it."""
        conn = self._open(host, port, user, password)
        try:
            return self._fetch_impala(conn, sql)
        finally:
            conn.close()

    def query_impala_polars(self, sql, host, port, user, password):
        """Open a connection, fetch rows as a Polars LazyFrame, and close it."""
        conn = self._open(host, port, user, password)
        try:
            return self._fetch_impala_polars(conn, sql)
        finally:
            conn.close()

    @staticmethod
    def _fetch_impala(conn, sql):
        """Fetch all rows from an open Impala connection as a pandas DataFrame."""
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return as_pandas(cursor)

    @staticmethod
    def _fetch_impala_polars(conn, sql):
        """
        Fetch from an open Impala connection in 100k-row chunks and build a
        Polars LazyFrame directly.

        Compared to _fetch_impala(), this avoids creating a pandas DataFrame entirely.
        Each batch of raw rows is converted to a pl.DataFrame and the raw row list
        is freed before the next batch is fetched — keeping peak memory lower.
        """
        with conn.cursor() as cursor:
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            frames = []
            while True:
                rows = cursor.fetchmany(100000)
                if not rows:
                    break
                frames.append(pl.DataFrame(
                    {col: [row[i] for row in rows] for i, col in enumerate(cols)}
                ))
        return pl.concat(frames).lazy() if frames else pl.LazyFrame()

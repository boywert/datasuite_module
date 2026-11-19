"""Regression tests for the 2026-11 bug-fix batch (no real DB / SSH needed)."""

import io
import json
import os
import subprocess
import sys

import pandas as pd
import polars as pl
import pytest

from datasuite import db_connect, ssh_tunnel_connect
from datasuite.db_connect import DB2Connect, ImpalaConnect, dbc_info
from datasuite.export_excel import export_excel
from datasuite.local_query import local_query
from datasuite.parquet_store import DataStore, _print, _quote_ident, _stream_file_polars
from datasuite.sftp_data_loader import SFTPDataLoader
from datasuite.ssh_tunnel_connect import SSHTunnelConnect


# ─── duckdb .arrow() removal ─────────────────────────────────────────────────

def test_local_query_polars_engine():
    df = pd.DataFrame({"x": [1, 2, 3]})  # noqa: F841 — discovered by local_query
    lf = local_query("SELECT SUM(x) AS s FROM df", engine="polars")
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect()["s"][0] == 6


def test_datastore_query_polars_engine(tmp_path):
    store = DataStore(str(tmp_path / "store"))
    store.save_df("t", pd.DataFrame({"x": [1, 2]}))
    assert store.query("SELECT COUNT(*) AS n FROM t", engine="polars").collect()["n"][0] == 2


# ─── SSH tunnel ──────────────────────────────────────────────────────────────

def _tunnel(**kw):
    args = dict(dest_server="db", dest_port=50000, remote_server="bastion",
                remote_server_port=22, remote_user="me", remote_private_key="/k",
                tunnel_type="ssh", connect_timeout=2)
    args.update(kw)
    return SSHTunnelConnect(**args)


def test_ssh_command_is_argument_list_without_fork():
    t = _tunnel()
    t.local_port = 8801
    cmd = t._ssh_command()
    assert isinstance(cmd, list)          # works without shell=True on Linux
    assert "-f" not in cmd                # Popen handle must be the tunnel itself
    assert "-N" in cmd
    assert cmd[cmd.index("-L") + 1] == "8801:db:50000"
    assert cmd[-1] == "me@bastion"


def test_ssh_tunnel_failure_raises_and_cleans_up(monkeypatch):
    # A process that exits at once stands in for ssh failing to connect.
    real_popen = subprocess.Popen
    monkeypatch.setattr(ssh_tunnel_connect.subprocess, "Popen",
                        lambda cmd, **kw: real_popen(
                            [sys.executable, "-c", "import sys; sys.stderr.write('auth failed'); sys.exit(255)"], **kw))
    t = _tunnel()
    monkeypatch.setattr(t, "port_selection", lambda: 8899)
    with pytest.raises(ConnectionError, match="auth failed"):
        t.connect()
    assert t.tunnel is None


def test_ssh_tunnel_close_kills_process(monkeypatch):
    real_popen = subprocess.Popen
    monkeypatch.setattr(ssh_tunnel_connect.subprocess, "Popen",
                        lambda cmd, **kw: real_popen(
                            [sys.executable, "-c", "import time; time.sleep(60)"], **kw))
    monkeypatch.setattr(ssh_tunnel_connect, "_port_is_open", lambda port, host="127.0.0.1": True)
    t = _tunnel()
    monkeypatch.setattr(t, "port_selection", lambda: 8899)
    t.connect()
    proc = t.tunnel
    t.close()
    assert proc.poll() is not None
    assert t.tunnel is None


def test_tunnel_no_free_port_raises(monkeypatch):
    t = _tunnel()
    monkeypatch.setattr(t, "port_selection", lambda: False)
    with pytest.raises(RuntimeError, match="No free local port"):
        t.connect()


# ─── db_connect ──────────────────────────────────────────────────────────────

def test_kwargs_do_not_mutate_shared_dbc_info():
    before = dbc_info.db2_server
    a = DB2Connect(db2_server="host-a")
    b = DB2Connect()
    assert a.dbc.db2_server == "host-a"
    assert b.dbc.db2_server == before
    assert dbc_info.db2_server == before


def test_unknown_kwarg_raises():
    with pytest.raises(TypeError, match="db2_sever"):
        DB2Connect(db2_sever="typo")


@pytest.mark.parametrize("cls, opener", [(DB2Connect, "open_db2_connection"),
                                         (ImpalaConnect, "open_impala_connection")])
def test_query_errors_propagate(monkeypatch, cls, opener):
    def boom(dbc=None):
        raise ConnectionError("cannot reach db")
    monkeypatch.setattr(db_connect, opener, boom)
    with pytest.raises(ConnectionError, match="cannot reach db"):
        cls().query("SELECT 1")


# ─── polars CSV reader ──────────────────────────────────────────────────────

def test_stream_file_polars_skiprows_zero_and_dtype(tmp_path):
    csv = tmp_path / "d.csv"
    csv.write_text("a,b\n1,x\n2,y\n")
    tables = list(_stream_file_polars(str(csv), None, 100, skiprows=0, dtypes={"a": pl.Utf8}))
    out = pl.from_arrow(tables[0])
    assert out.columns == ["a", "b"]
    assert out.schema["a"] == pl.Utf8


def test_save_file_use_polars_chunks(tmp_path):
    csv = tmp_path / "big.csv"
    pd.DataFrame({"a": range(25), "b": ["x"] * 25}).to_csv(csv, index=False)
    store = DataStore(str(tmp_path / "store"))
    store.save_file("big", str(csv), chunk_size=10, use_polars=True, usecols="a")
    assert store.info("big")["rows"] == 25
    assert list(store.query("SELECT * FROM big").columns) == ["a"]


def test_stream_file_polars_non_utf8_falls_back(tmp_path):
    csv = tmp_path / "thai.csv"
    csv.write_bytes("name\nสวัสดี\n".encode("cp874"))
    tables = list(_stream_file_polars(str(csv), None, 100, encoding="cp874"))
    assert tables[0].column("name").to_pylist() == ["สวัสดี"]


def test_stream_file_polars_conflicting_aliases(tmp_path):
    csv = tmp_path / "d.csv"
    csv.write_text("a\n1\n")
    with pytest.raises(TypeError):
        list(_stream_file_polars(str(csv), None, 100, sep=",", delimiter=","))


# ─── export_excel ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("df", [pd.DataFrame({"a": [], "b": []}), pd.DataFrame()])
def test_export_excel_empty_dataframe(tmp_path, df):
    path = tmp_path / "empty.xlsx"
    export_excel(str(path), [{"sheetname": "S", "dataframe": df}])
    assert path.exists()


def test_export_excel_non_string_column_names(tmp_path):
    path = tmp_path / "ints.xlsx"
    export_excel(str(path), [{"sheetname": "S", "dataframe": pd.DataFrame({0: [1.5], 1: ["x"]})}])
    assert path.exists()


# ─── DataStore: names, paths, console ───────────────────────────────────────

def test_dataset_name_needing_quotes(tmp_path):
    store = DataStore(str(tmp_path / "store"))
    store.save_df("my-data", pd.DataFrame({"x": [1, 2]}))
    store.save_df("select", pd.DataFrame({"x": [3]}))
    assert store.query(f"SELECT COUNT(*) AS n FROM {_quote_ident('my-data')}")["n"][0] == 2
    assert store.query('SELECT x FROM "select"')["x"][0] == 3
    store.drop("my-data")
    assert "my-data" not in store._catalog


@pytest.mark.parametrize("bad", ["../escape", "a/b", "a\\b", ".hidden", ""])
def test_dataset_name_rejects_paths(tmp_path, bad):
    store = DataStore(str(tmp_path / "store"))
    with pytest.raises(ValueError):
        store.save_df(bad, pd.DataFrame({"x": [1]}))


def test_catalog_paths_relative_to_store(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = DataStore("store")                     # relative base_dir
    store.save_df("t", pd.DataFrame({"x": [1]}))
    assert store._catalog["t"]["path"] == "t"
    # Re-open from a different working directory using an absolute path.
    other = tmp_path / "elsewhere"
    other.mkdir()
    monkeypatch.chdir(other)
    reopened = DataStore(str(tmp_path / "store"))
    assert reopened.query("SELECT x FROM t")["x"][0] == 1


def test_legacy_cwd_relative_catalog_still_loads(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = DataStore("store")
    store.save_df("t", pd.DataFrame({"x": [7]}))
    cat_path = tmp_path / "store" / ".catalog.json"
    cat = json.loads(cat_path.read_text())
    cat["t"]["path"] = "store\\t"                 # what old versions wrote on Windows
    cat_path.write_text(json.dumps(cat))
    reopened = DataStore("store")
    assert reopened.query("SELECT x FROM t")["x"][0] == 7


def test_print_falls_back_on_cp874(monkeypatch):
    buf = io.BytesIO()
    stream = io.TextIOWrapper(buf, encoding="cp874", errors="strict")
    monkeypatch.setattr(sys, "stdout", stream)
    _print("✓  'x' ready  —  1 rows  →  /tmp")
    stream.flush()
    out = buf.getvalue().decode("cp874")
    assert out.startswith("OK  'x' ready  -  1 rows  ->  /tmp")


# ─── SFTP ────────────────────────────────────────────────────────────────────

class _Closable:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _FakeRemoteFile(io.BytesIO):
    def prefetch(self):
        pass


class _FakeOpenSFTP:
    def __init__(self, files):
        self.files = files

    def open(self, path, mode="r"):
        return _FakeRemoteFile(self.files[path])


def test_read_remote_infers_file_type_from_path(tmp_path):
    ld = SFTPDataLoader(hostname="h", username="u", cache_dir=str(tmp_path / "cache"))
    ld.sftp = _FakeOpenSFTP({"/data/a.csv": b"x,y\n1,2\n"})
    df = ld.read_remote("/data/a.csv")
    assert list(df.columns) == ["x", "y"]
    # second call is served from the Parquet cache
    assert ld.read_remote("/data/a.csv")["x"][0] == 1


def test_read_remote_without_extension_needs_file_type(tmp_path):
    ld = SFTPDataLoader(hostname="h", username="u", cache_dir=str(tmp_path / "cache"))
    ld.sftp = _FakeOpenSFTP({"/data/export": b"x\n1\n"})
    with pytest.raises(ValueError, match="file_type"):
        ld.read_remote("/data/export")
    assert ld.read_remote("/data/export", file_type="csv")["x"][0] == 1


def test_sftp_close_resets_bastion():
    ld = SFTPDataLoader(hostname="h", username="u")
    ld.sftp, ld.client, ld.bastion_client = _Closable(), _Closable(), _Closable()
    handles = (ld.sftp, ld.client, ld.bastion_client)
    ld.close()
    assert all(h.closed for h in handles)
    assert ld.sftp is None and ld.client is None and ld.bastion_client is None


# ─── magic reader options ────────────────────────────────────────────────────

def test_parse_reader_options():
    from datasuite.parquet_store import _parse_reader_options
    opts = _parse_reader_options([
        "sep=,", "# comment", "", "skiprows=2", "usecols=a,b", "header=none",
        "encoding=cp874", 'quotechar="\'"', 'na_values=" "',
    ])
    assert opts == {"sep": ",", "skiprows": 2, "usecols": ["a", "b"], "header": None,
                    "encoding": "cp874", "quotechar": "'", "na_values": " "}
    assert _parse_reader_options([r"sep=\t"]) == {"sep": "\t"}   # backslash-t text → tab
    assert _parse_reader_options(['sep=" "']) == {"sep": " "}
    assert _parse_reader_options(["sep=;"]) == {"sep": ";"}
    with pytest.raises(ValueError):
        _parse_reader_options(["no_equals_sign"])

"""
parquet_store — stream large queries to chunked Parquet, query via DuckDB
=========================================================================

Streams query results from BigQuery, Impala, or DB2 into partitioned Parquet
files on disk, then registers each dataset as a DuckDB view so users can query
across all chunks transparently with plain SQL.

Quick start
-----------
    from parquet_store import DataStore

    store = DataStore("/home/jovyan/work/datasets")

    # ── save from BigQuery ─────────────────────────────────────────────────
    from google.cloud import bigquery
    bq = bigquery.Client(project="my-project")
    store.save("sales", "SELECT * FROM `proj.ds.sales`", conn=bq)

    # ── save from Impala ───────────────────────────────────────────────────
    from impala.dbapi import connect
    impala = connect(host="impala-host", port=21050)
    store.save("inventory", "SELECT * FROM warehouse.inventory", conn=impala)

    # ── save from DB2 ──────────────────────────────────────────────────────
    import ibm_db_dbi
    db2 = ibm_db_dbi.connect("DATABASE=mydb;HOSTNAME=h;PORT=50000;UID=u;PWD=p;", "", "")
    store.save("orders", "SELECT * FROM SCHEMA.ORDERS", conn=db2)

    # ── use connections.yaml with SSH tunneling ──────────────────────────
    # connections.yaml:
    #   impala_prod:
    #     type: impala
    #     host: impala-host
    #     port: 21050
    #     user: ${IMPALA_USER}
    #     password: ${IMPALA_PASS}
    #     ssh:
    #       host: bastion.example.com
    #       port: 22
    #       user: ${BASTION_USER}
    #       private_key: ${BASTION_PRIVATE_KEY}
    #       tunnel_type: python          # or "ssh" (default: python for impala, ssh for db2)
    conn = store.connect("impala_prod")  # tunnel opened automatically
    store.save("inventory", "SELECT * FROM warehouse.inventory", conn=conn)
    store.disconnect("impala_prod")      # close the tunnel when done

    # ── enable %%dql SQL magic with autocomplete ─────────────────────────
    store.enable_magic()

    # then in any cell:
    # %%dql
    # SELECT year, SUM(amount) FROM sales GROUP BY year ORDER BY year

    # ── or query from Python ──────────────────────────────────────────────
    df = store.query("SELECT s.*, i.stock FROM sales s JOIN inventory i USING (sku)")

    # ── manage datasets ────────────────────────────────────────────────────
    store.list()                    # DataFrame of all saved datasets
    store.drop("orders")            # unregister view only
    store.drop("orders", files=True)  # also delete parquet files

File layout
-----------
    datasets/
        .catalog.json               ← registry of all saved datasets
        sales/
            part-0000.parquet
            part-0001.parquet
            ...
        inventory/
            part-0000.parquet
            ...
"""

from __future__ import annotations

import glob as _glob_mod
import json
import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


# ─── connection config ────────────────────────────────────────────────────────

_ENV_VAR_RE = re.compile(r"\$\{(\w+)\}")

def _expand_env_recursive(obj):
    """Recursively expand ${VAR} placeholders in strings, dicts, and lists."""
    if isinstance(obj, str):
        return _ENV_VAR_RE.sub(lambda m: os.environ.get(m.group(1), ""), obj)
    if isinstance(obj, dict):
        return {k: _expand_env_recursive(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_recursive(v) for v in obj]
    return obj


def _load_connections_file(path: str | Path) -> dict:
    """Load and env-expand a connections.yaml file."""
    p = Path(path)
    if not p.exists():
        return {}
    raw = yaml.safe_load(p.read_text()) or {}
    expanded = {}
    for name, cfg in raw.items():
        expanded[name] = _expand_env_recursive(cfg)
    return expanded


def _maybe_open_tunnel(cfg: dict):
    """Open an SSH tunnel if the config has an ``ssh`` section and we are not
    on a direct-access host (``helpers.DIRECT_ACCESS_HOSTS``).

    Returns ``(host, port, tunnel_or_None)``.  When no tunnel is needed the
    original host/port are returned and the tunnel is ``None``.
    """
    from .helpers import is_direct_access_host

    db_type = cfg.get("type", "").lower()
    host = cfg.get("host", "")
    port = int(cfg.get("port", _DEFAULT_PORTS.get(db_type, 0)))

    ssh_cfg = cfg.get("ssh")
    if not ssh_cfg:
        return host, port, None

    # On the production CI host we have direct network access — skip tunnel.
    if is_direct_access_host():
        return host, port, None

    from .ssh_tunnel_connect import SSHTunnelConnect

    tunnel_type = ssh_cfg.get("tunnel_type", _DEFAULT_TUNNEL_TYPES.get(db_type, "python"))
    tunnel = SSHTunnelConnect(
        dest_server=host,
        dest_port=port,
        remote_server=ssh_cfg["host"],
        remote_server_port=int(ssh_cfg.get("port", 22)),
        remote_user=ssh_cfg["user"],
        remote_private_key=ssh_cfg.get("private_key"),
        tunnel_type=tunnel_type,
    )
    tunnel.connect()
    return "127.0.0.1", tunnel.local_port, tunnel


def _has_explicit_credentials(cfg: dict) -> bool:
    """Return True if the config provides its own host + credentials."""
    return bool(cfg.get("host") and cfg.get("user"))


def _create_connection(cfg: dict):
    """Create a database connection object from a config dict.

    When the config provides explicit ``host`` / ``user`` / ``password`` fields,
    a direct connection is made (with optional SSH tunnel from the ``ssh`` block).

    When credentials are **omitted**, the function falls back to the shared
    ``open_db2_connection`` / ``open_impala_connection`` factories in
    ``db_connect`` — using the same ``dbc_info`` env vars and
    direct-access host detection as ``DB2Connect`` / ``ImpalaConnect``.

    Returns ``(connection, tunnel_or_None)``.  The caller must close the tunnel
    when the connection is no longer needed.
    """
    db_type = cfg.get("type", "").lower()

    if db_type == "bigquery":
        from google.cloud import bigquery
        kwargs = {}
        if cfg.get("project"):
            kwargs["project"] = cfg["project"]
        return bigquery.Client(**kwargs), None

    # ── fast path: no explicit credentials → use dbc_info (same as DB2Connect / ImpalaConnect)
    if not _has_explicit_credentials(cfg):
        if db_type == "db2":
            from .db_connect import open_db2_connection
            return open_db2_connection()
        if db_type == "impala":
            from .db_connect import open_impala_connection
            return open_impala_connection()

    # ── explicit credentials → use them directly, with optional SSH tunnel
    host, port, tunnel = _maybe_open_tunnel(cfg)

    if db_type == "impala":
        from impala.dbapi import connect
        kwargs = {
            "host": host,
            "port": port,
            "database": cfg.get("database", "default"),
            "auth_mechanism": cfg.get("auth_mechanism", "NOSASL"),
        }
        if cfg.get("user"):
            kwargs["user"] = cfg["user"]
        if cfg.get("password"):
            kwargs["password"] = cfg["password"]
        return connect(**kwargs), tunnel

    if db_type == "db2":
        import ibm_db_dbi
        parts = [
            f"DATABASE={cfg.get('database', 'BLUDB')}",
            f"HOSTNAME={host}",
            f"PORT={port}",
            "PROTOCOL=TCPIP",
            f"UID={cfg['user']}",
            f"PWD={cfg['password']}",
        ]
        return ibm_db_dbi.connect(";".join(parts) + ";", "", ""), tunnel

    if tunnel:
        tunnel.close()
    raise ValueError(f"Unsupported connection type: {db_type!r}. Use bigquery, impala, or db2.")


# ─── source detection ─────────────────────────────────────────────────────────

def _is_bq_client(conn) -> bool:
    try:
        from google.cloud import bigquery
        return isinstance(conn, bigquery.Client)
    except ImportError:
        return False


def _source_name(conn) -> str:
    if _is_bq_client(conn):
        return "bigquery"
    cls = type(conn).__module__
    if "impala" in cls:
        return "impala"
    if "ibm_db" in cls:
        return "db2"
    return "dbapi"


# ─── streaming backends ───────────────────────────────────────────────────────

def _stream_bq(query: str, conn, chunk_size: int, job_config=None):
    """Yield pyarrow Tables in pages from BigQuery."""
    job = conn.query(query, job_config=job_config)
    for page in job.result(page_size=chunk_size).pages:
        yield page.to_arrow()


def _stream_dbapi(query: str, conn, chunk_size: int):
    """Yield pyarrow Tables in fetchmany batches from any DB-API 2.0 connection."""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        columns = [d[0] for d in cursor.description]
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            df = pd.DataFrame(rows, columns=columns)
            yield pa.Table.from_pandas(df, preserve_index=False)
    finally:
        cursor.close()


def _stream(query: str, conn, chunk_size: int, **kwargs):
    if _is_bq_client(conn):
        return _stream_bq(query, conn, chunk_size, kwargs.get("job_config"))
    return _stream_dbapi(query, conn, chunk_size)


# ─── file streaming backends ─────────────────────────────────────────────────

def _resolve_ext(file_path: str, file_type: str | None) -> str:
    """Return a normalised extension like ``.csv``."""
    if file_type is not None:
        return f".{file_type.lower().strip('.')}"
    return Path(file_path).suffix.lower()


def _stream_file(file_path: str, file_type: str | None, chunk_size: int, **kwargs):
    """Yield ``pa.Table`` chunks from a local file.

    CSV / TXT  — uses ``pd.read_csv(chunksize=)`` for true streaming.
    Parquet    — uses ``pyarrow.parquet.ParquetFile.iter_batches``.
    Excel      — reads eagerly then yields DataFrame slices.
    """
    ext = _resolve_ext(file_path, file_type)

    if ext in (".csv", ".txt"):
        for chunk_df in pd.read_csv(file_path, chunksize=chunk_size, **kwargs):
            yield pa.Table.from_pandas(chunk_df, preserve_index=False)

    elif ext in (".parquet", ".pqt"):
        pf = pq.ParquetFile(file_path)
        for batch in pf.iter_batches(batch_size=chunk_size, **kwargs):
            yield pa.Table.from_batches([batch])

    elif ext in (".xlsx", ".xls", ".xlsm", ".odf", ".ods"):
        df = pd.read_excel(file_path, **kwargs)
        for offset in range(0, len(df), chunk_size):
            yield pa.Table.from_pandas(
                df.iloc[offset : offset + chunk_size], preserve_index=False,
            )

    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _stream_file_polars(file_path: str, file_type: str | None, chunk_size: int, **kwargs):
    """Yield ``pa.Table`` chunks using Polars' batched reader (faster for CSV).

    CSV / TXT  — uses ``pl.scan_csv().collect_batches()`` (Polars >= 2) or
                 ``pl.read_csv_batched()`` (older Polars): Rust-based
                 multi-threaded parser, ~2-5× faster than the pandas path.
    Parquet    — uses ``pyarrow.parquet.ParquetFile.iter_batches`` (already
                 optimal; Polars adds no benefit here).
    Excel      — falls back to pandas (Polars has no batched Excel reader).
    """
    import polars as pl

    ext = _resolve_ext(file_path, file_type)

    if ext in (".csv", ".txt"):
        # Map common pandas kwargs to Polars equivalents
        # (pandas name(s) → Polars name).  Keys are tested with ``in`` so that
        # falsy values such as skiprows=0 are passed through, not dropped.
        pl_kwargs = {}
        aliases = {
            "separator": ("sep", "delimiter", "separator"),
            "encoding": ("encoding",),
            "skip_rows": ("skiprows", "skip_rows"),
            "columns": ("usecols", "columns"),
            # ``dtypes=`` is deprecated in Polars; ``schema_overrides=`` replaces it.
            "schema_overrides": ("dtype", "dtypes", "schema_overrides"),
        }
        for pl_name, names in aliases.items():
            given = [n for n in names if n in kwargs]
            if len(given) > 1:
                raise TypeError(f"Pass only one of {given} for {pl_name!r}")
            if given:
                pl_kwargs[pl_name] = kwargs.pop(given[0])
        if isinstance(pl_kwargs.get("columns"), str):
            pl_kwargs["columns"] = [c.strip() for c in pl_kwargs["columns"].split(",")]
        # Warn about unmapped kwargs
        if kwargs:
            _print(f"  Note: kwargs not mapped to Polars, ignored: {list(kwargs.keys())}")

        if hasattr(pl.LazyFrame, "collect_batches"):
            # Polars >= 2 removed read_csv_batched; scan + streaming batches
            # replaces it.  scan_csv only decodes UTF-8, so other encodings
            # fall back to the pandas reader.
            encoding = str(pl_kwargs.pop("encoding", "utf8")).lower().replace("-", "")
            if encoding not in ("utf8", "utf8lossy"):
                pd_kwargs = {"encoding": encoding}
                if "separator" in pl_kwargs:
                    pd_kwargs["sep"] = pl_kwargs["separator"]
                if "skip_rows" in pl_kwargs:
                    pd_kwargs["skiprows"] = pl_kwargs["skip_rows"]
                if "columns" in pl_kwargs:
                    pd_kwargs["usecols"] = pl_kwargs["columns"]
                if "schema_overrides" in pl_kwargs:
                    _print("  Note: dtype overrides are ignored for non-UTF-8 files")
                yield from _stream_file(file_path, ext.lstrip("."), chunk_size, **pd_kwargs)
                return
            if encoding == "utf8lossy":
                pl_kwargs["encoding"] = "utf8-lossy"
            columns = pl_kwargs.pop("columns", None)
            lf = pl.scan_csv(file_path, **pl_kwargs)
            if columns is not None:
                lf = lf.select(columns)
            for batch in lf.collect_batches(chunk_size=chunk_size):
                yield batch.to_arrow()
            return

        reader = pl.read_csv_batched(
            file_path, batch_size=chunk_size, **pl_kwargs
        )
        while True:
            batches = reader.next_batches(1)
            if not batches:
                break
            yield batches[0].to_arrow()

    elif ext in (".parquet", ".pqt"):
        # PyArrow native — already optimal
        pf = pq.ParquetFile(file_path)
        for batch in pf.iter_batches(batch_size=chunk_size, **kwargs):
            yield pa.Table.from_batches([batch])

    elif ext in (".xlsx", ".xls", ".xlsm", ".odf", ".ods"):
        # No Polars batched reader for Excel — fall back to pandas
        df = pd.read_excel(file_path, **kwargs)
        for offset in range(0, len(df), chunk_size):
            yield pa.Table.from_pandas(
                df.iloc[offset : offset + chunk_size], preserve_index=False,
            )

    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _has_glob(path: str) -> bool:
    """Return True if *path* contains wildcard characters."""
    return any(c in path for c in ("*", "?", "["))


def _stream_files_glob(
    pattern: str, file_type: str | None, chunk_size: int,
    use_polars: bool = False, **kwargs,
):
    """Expand a glob pattern and yield ``pa.Table`` chunks from every match.

    Files are processed in sorted order (alphabetical) so the resulting
    Parquet part numbering is deterministic.  Raises ``FileNotFoundError``
    if the pattern matches nothing.
    """
    matched = sorted(_glob_mod.glob(pattern, recursive=True))
    if not matched:
        raise FileNotFoundError(f"No files matched pattern: {pattern}")
    streamer = _stream_file_polars if use_polars else _stream_file
    _print(f"Matched {len(matched)} file(s) for pattern: {pattern}")
    for fp in matched:
        _print(f"  Reading: {fp}")
        yield from streamer(fp, file_type, chunk_size, **kwargs)


# ─── DataFrame streaming backend ────────────────────────────────────────────

def _stream_dataframe(df, chunk_size: int):
    """Yield ``pa.Table`` chunks from a pandas/polars DataFrame or LazyFrame.

    pl.LazyFrame  → collected to pl.DataFrame first (unavoidable — Polars has
                    no chunked-collect API), then sliced to Arrow batches.
    pl.DataFrame  → converted to Arrow (zero-copy), then sliced.
    pd.DataFrame  → sliced directly, each slice converted to Arrow.
    """
    import polars as pl

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    if isinstance(df, pl.DataFrame):
        arrow = df.to_arrow()
        for offset in range(0, arrow.num_rows, chunk_size):
            yield arrow.slice(offset, chunk_size)
    elif isinstance(df, pd.DataFrame):
        for offset in range(0, len(df), chunk_size):
            yield pa.Table.from_pandas(
                df.iloc[offset : offset + chunk_size], preserve_index=False,
            )
    else:
        raise TypeError(
            f"Expected pd.DataFrame, pl.DataFrame, or pl.LazyFrame; got {type(df).__name__}"
        )


# ─── writer ───────────────────────────────────────────────────────────────────

def _next_part_number(dest_dir: Path) -> int:
    """Return the next available part number by inspecting existing files."""
    existing = sorted(dest_dir.glob("part-*.parquet"))
    if not existing:
        return 0
    last = existing[-1].stem  # "part-0003"
    return int(last.split("-")[1]) + 1


def _write_from_iter(
    tables,
    dest_dir: Path,
    compression: str,
    start_part: int = 0,
) -> tuple[int, int]:
    """Write an iterable of ``pa.Table`` to ``dest_dir/part-NNNN.parquet``.

    This is the single writer used by every input mode (SQL, file, DataFrame).
    Returns ``(total_rows, num_chunks)``.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    total_rows, part = 0, start_part
    for table in tables:
        out = dest_dir / f"part-{part:04d}.parquet"
        pq.write_table(table, out, compression=compression)
        total_rows += len(table)
        part += 1
        _print(f"\r  part-{part-1:04d}.parquet  —  {total_rows:,} rows…", end="", flush=True)

    _print()
    return total_rows, part - start_part


def _safe_write(tables, dest_dir: Path, compression: str, is_append: bool,
                is_replace: bool = False):
    """Atomic write wrapper around ``_write_from_iter``.

    Chunks are always written to a temporary sibling directory first.  If
    anything fails mid-stream the temp directory is removed and the existing
    dataset is left untouched.

    On success:

    * **new**     — the temp files are moved into ``dest_dir``.
    * **replace** — the existing ``dest_dir`` is renamed aside, the temp dir
                    is renamed into its place, then the old copy is deleted.
                    If the swap fails, the old directory is restored.
    * **append**  — new parts (numbered after the existing ones) are moved
                    into ``dest_dir``; existing part files are never touched.

    Returns ``(total_rows, num_chunks)``.
    """
    start_part = _next_part_number(dest_dir) if is_append and dest_dir.exists() else 0

    # Create the temp dir as a sibling of dest_dir so os.rename is same-fs (atomic).
    parent = dest_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(dir=parent, prefix=f".{dest_dir.name}_tmp_"))

    try:
        total_rows, num_chunks = _write_from_iter(
            tables, tmp_dir, compression, start_part=start_part,
        )
    except BaseException:
        # ── Failure: clean up temp dir, leave existing data untouched ─
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    if is_replace and dest_dir.exists():
        # ── Swap: old → backup, tmp → dest, then delete backup ────────
        backup = Path(tempfile.mkdtemp(dir=parent, prefix=f".{dest_dir.name}_old_"))
        backup.rmdir()  # only needed a unique name; os.rename needs it absent
        os.rename(dest_dir, backup)
        try:
            os.rename(tmp_dir, dest_dir)
        except BaseException:
            os.rename(backup, dest_dir)  # restore the original dataset
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise
        shutil.rmtree(backup, ignore_errors=True)
        return total_rows, num_chunks

    if total_rows == 0 and num_chunks == 0:
        # Nothing to write — clean up and return
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return 0, 0

    # ── Success: move files to final destination ──────────────
    dest_dir.mkdir(parents=True, exist_ok=True)
    for f in sorted(tmp_dir.glob("part-*.parquet")):
        shutil.move(str(f), str(dest_dir / f.name))

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return total_rows, num_chunks


# ─── magic value parser ──────────────────────────────────────────────────────

def _parse_magic_value(raw: str):
    """Convert a string value from ``%%save --file`` cell kwargs to a Python
    object.

    Rules:
      * ``true`` / ``false`` → bool
      * ``none``             → None
      * digits               → int
      * contains ``,``       → list[str]  (split on comma)
      * otherwise            → str
    """
    low = raw.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if low == "none":
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    if "," in raw:
        return [v.strip() for v in raw.split(",")]
    return raw


# Reader options whose value is always text (often a single punctuation
# character), so ``sep=,`` must not become a list and ``sep=;`` stays a string.
_TEXT_OPTIONS = {
    "sep", "delimiter", "separator", "quotechar", "quote_char", "escapechar",
    "lineterminator", "eol_char", "decimal", "thousands", "comment",
    "comment_prefix", "encoding",
}

_ESCAPES = {"\\t": "\t", "\\n": "\n", "\\r": "\r"}


def _parse_reader_options(lines) -> dict:
    """Parse ``key=value`` reader-option lines from a ``%%save`` cell body.

    * blank lines and ``#`` comments are skipped
    * a value in matching quotes (``sep=" "``) is used literally
    * text options (``sep``, ``encoding``, …) stay strings; ``\\t`` / ``\\n``
      / ``\\r`` become tab / newline / carriage return
    * everything else goes through ``_parse_magic_value``
    """
    opts = {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, raw = line.partition("=")
        key, raw = key.strip(), raw.strip()
        if not sep or not key:
            raise ValueError(f"Reader option must be key=value; got {line!r}")
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "\"'":
            opts[key] = raw[1:-1]
        elif key in _TEXT_OPTIONS:
            opts[key] = _ESCAPES.get(raw, raw)
        else:
            opts[key] = _parse_magic_value(raw)
    return opts


# ─── SQL quoting / console output ───────────────────────────────────────────

def _quote_ident(name: str) -> str:
    """Quote *name* as a DuckDB identifier (handles spaces, dashes, keywords)."""
    return '"' + str(name).replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """Quote *value* as a DuckDB string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def _validate_dataset_name(name: str) -> None:
    """Reject names that would escape ``base_dir`` or collide with internals."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Dataset name must be a non-empty string")
    if any(c in name for c in ("/", "\\", ":")) or name.startswith(".") or name != name.strip():
        raise ValueError(
            f"Invalid dataset name {name!r}: no path separators, ':', "
            "leading '.', or surrounding spaces"
        )


_ASCII_FALLBACK = str.maketrans({
    "✓": "OK", "✗": "X", "→": "->", "←": "<-", "—": "-", "–": "-",
    "…": "...", "×": "x", "⚠": "!",
})


def _print(*args, **kwargs):
    """``print`` that survives consoles whose code page lacks ✓ / → (e.g. cp874).

    Falls back to ASCII replacements, then to ``?`` for anything else.
    """
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        sep = kwargs.pop("sep", " ")
        text = sep.join(str(a) for a in args).translate(_ASCII_FALLBACK)
        import sys
        enc = getattr(sys.stdout, "encoding", None) or "ascii"
        text = text.encode(enc, errors="replace").decode(enc, errors="replace")
        print(text, **kwargs)


# ─── DataStore ────────────────────────────────────────────────────────────────

class DataStore:
    """Persistent store that maps dataset names to chunked Parquet on disk
    and exposes each as a DuckDB view for ad-hoc SQL queries."""

    _DEFAULT_CONN_FILE = "/etc/datastore/connections.yaml"

    def __init__(
        self,
        base_dir: str = "/home/jovyan/work/datasets",
        connections_file: Optional[str] = None,
    ):
        import duckdb
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_path = self.base_dir / ".catalog.json"
        self._catalog: dict = self._load_catalog()
        self.ddb = duckdb.connect()
        self._register_all()
        # Connection config: explicit path > env var > system default > store dir
        self._conn_file = Path(
            connections_file
            or os.environ.get("DATASTORE_CONNECTIONS", "")
            or self._DEFAULT_CONN_FILE
        )
        self._connections: dict = _load_connections_file(self._conn_file)
        self._active_tunnels: dict = {}  # name → SSHTunnelConnect

    # ── catalog I/O ───────────────────────────────────────────────────────

    def _load_catalog(self) -> dict:
        if self._catalog_path.exists():
            return json.loads(self._catalog_path.read_text(encoding="utf-8"))
        return {}

    def _save_catalog(self):
        self._catalog_path.write_text(
            json.dumps(self._catalog, indent=2, default=str, ensure_ascii=False),
            encoding="utf-8",
        )

    def _catalog_entry_path(self, dest: Path) -> str:
        """Path to store in the catalog: relative to ``base_dir`` (POSIX style)
        when the dataset lives inside it, so the store can be moved or opened
        from any working directory / OS."""
        try:
            return Path(dest).resolve().relative_to(self.base_dir.resolve()).as_posix()
        except ValueError:
            return Path(dest).resolve().as_posix()

    def _dataset_path(self, name: str) -> Path:
        """Resolve a catalog entry's directory.

        New entries are relative to ``base_dir``.  Older catalogs stored the
        path relative to the working directory at save time (often with
        Windows backslashes); those are still found when they exist.
        """
        raw = str(self._catalog[name].get("path") or name).replace("\\", "/")
        p = Path(raw)
        if p.is_absolute():
            return p
        candidate = self.base_dir / p
        if candidate.exists():
            return candidate
        if p.exists():                        # legacy cwd-relative entry
            return p.resolve()
        return self.base_dir / name

    # ── DuckDB view management ────────────────────────────────────────────

    def _register(self, name: str, path: Path):
        glob = str(Path(path) / "*.parquet").replace("\\", "/")
        # union_by_name=true: unify schemas across all part files so that
        # columns with differing precision/nullability (e.g. DECIMAL(11,2) vs
        # DECIMAL(12,2) written by different upstream queries) are merged into
        # the widest compatible type instead of raising a BinderException.
        self.ddb.execute(
            f"CREATE OR REPLACE VIEW {_quote_ident(name)} AS "
            f"SELECT * FROM read_parquet({_quote_literal(glob)}, union_by_name=true)"
        )

    def _register_all(self):
        for name in self._catalog:
            p = self._dataset_path(name)
            if p.exists() and any(p.glob("*.parquet")):
                self._register(name, p)

    # ── catalog helpers ─────────────────────────────────────────────────

    def _update_catalog(self, name, dest, new_rows, new_chunks, is_append,
                        *, source, query, compression, chunk_size):
        """Update the catalog and register the DuckDB view after a save."""
        if is_append:
            prev = self._catalog[name]
            self._catalog[name] = {
                **prev,
                "path": self._catalog_entry_path(dest),
                "rows": prev["rows"] + new_rows,
                "chunks": prev["chunks"] + new_chunks,
                "appended_at": datetime.now().isoformat(timespec="seconds"),
            }
            verb = f"appended {new_rows:,} rows ({prev['rows']:,} → {self._catalog[name]['rows']:,} total)"
        else:
            self._catalog[name] = {
                "path": self._catalog_entry_path(dest),
                "query": query,
                "rows": new_rows,
                "chunks": new_chunks,
                "source": source,
                "compression": compression,
                "chunk_size": chunk_size,
                "created_at": datetime.now().isoformat(timespec="seconds"),
            }
            verb = f"{new_rows:,} rows in {new_chunks} files"

        self._save_catalog()
        self._register(name, dest)
        _print(f"✓  '{name}' ready  —  {verb}  →  {dest}")

    # ── connections ────────────────────────────────────────────────────────

    # Built-in connection types that work without connections.yaml by
    # falling back to dbc_info env vars — same as DB2Connect / ImpalaConnect.
    _BUILTIN_TYPES = {"db2", "impala"}

    def connect(self, name: str):
        """Create a database connection.

        Parameters
        ----------
        name:  Either a connection name from ``connections.yaml``
               **or** one of the built-in types ``"db2"`` / ``"impala"``.

               Built-in types use the same ``dbc_info`` env vars and
               direct-access host detection as ``DB2Connect`` /
               ``ImpalaConnect`` — no ``connections.yaml`` entry needed.

        If the connection has an ``ssh`` section, an SSH tunnel is opened
        automatically (skipped on a direct-access host).  The tunnel
        is tracked internally and closed when ``disconnect()`` is called or
        when the DataStore is garbage-collected.

        Returns a connection object usable with ``save()`` and ``%%save --conn``.
        """
        if name in self._connections:
            cfg = self._connections[name]
        elif name in self._BUILTIN_TYPES:
            # No connections.yaml entry — synthesise a minimal config that
            # triggers the dbc_info fallback path in _create_connection.
            cfg = {"type": name}
        else:
            avail = sorted(set(self._connections.keys()) | self._BUILTIN_TYPES)
            raise KeyError(f"Connection '{name}' not found. Available: {avail}")

        conn, tunnel = _create_connection(cfg)
        if tunnel:
            self._active_tunnels[name] = tunnel
        return conn

    def disconnect(self, name: str = None):
        """Close SSH tunnel(s) opened by ``connect()``.

        Parameters
        ----------
        name:  Connection name to disconnect.  If ``None``, all active
               tunnels are closed.
        """
        if name is not None:
            tunnel = self._active_tunnels.pop(name, None)
            if tunnel:
                tunnel.close()
        else:
            for n, tunnel in list(self._active_tunnels.items()):
                tunnel.close()
            self._active_tunnels.clear()

    def connections(self) -> pd.DataFrame:
        """List available connections (without showing passwords)."""
        if not self._connections:
            _print(f"No connections configured. Edit {self._conn_file}")
            return pd.DataFrame()
        rows = []
        for name, cfg in self._connections.items():
            ssh_cfg = cfg.get("ssh")
            rows.append({
                "name": name,
                "type": cfg.get("type", ""),
                "host": cfg.get("host", cfg.get("project", "")),
                "port": cfg.get("port", ""),
                "database": cfg.get("database", ""),
                "ssh": ssh_cfg.get("host", "") if ssh_cfg else "",
                "tunnel_active": name in self._active_tunnels,
            })
        return pd.DataFrame(rows)

    # ── shared save logic ─────────────────────────────────────────────────

    def _do_save(self, name: str, tables, mode: str, compression: str,
                 chunk_size: int, source: str, query: str, label: str) -> "DataStore":
        """Common save logic shared by save / save_file / save_df / save_remote.

        Validates mode, checks for existing datasets, writes chunks atomically
        via ``_safe_write``, and updates the catalog + DuckDB view on success.
        On failure, partial files are cleaned up automatically.
        """
        if mode not in ("error", "replace", "append"):
            raise ValueError(f"mode must be 'error', 'replace', or 'append'; got {mode!r}")

        _validate_dataset_name(name)
        dest = self.base_dir / name
        exists = name in self._catalog and dest.exists()

        if exists and mode == "error":
            raise FileExistsError(
                f"Dataset '{name}' already exists at {dest}. "
                "Use mode='replace' to overwrite or mode='append' to add rows."
            )

        is_append = exists and mode == "append"
        is_replace = exists and mode == "replace"
        _print(f"{'Appending to' if is_append else 'Saving'} '{name}'{label}…")
        # The old dataset (if any) is only removed after the new write succeeds.
        new_rows, new_chunks = _safe_write(tables, dest, compression, is_append,
                                           is_replace=is_replace)

        self._update_catalog(name, dest, new_rows, new_chunks, is_append,
                             source=source, query=query,
                             compression=compression, chunk_size=chunk_size)
        return self

    # ── public API ────────────────────────────────────────────────────────

    def save(
        self,
        name: str,
        query: str,
        conn,
        chunk_size: int = 100_000,
        compression: str = "snappy",
        mode: str = "error",
        **kwargs,
    ) -> "DataStore":
        """Stream a query to chunked Parquet and register the result as a DuckDB view.

        Writes are **atomic**: chunks go to a temp directory first and are only
        moved to the final location on success.  A network drop or error
        mid-stream leaves the existing dataset untouched.

        Parameters
        ----------
        name:        Alias used in DuckDB SQL (e.g. ``"sales"``).
        query:       SQL query string.
        conn:        ``bigquery.Client``, Impala connection, DB2 connection,
                     or any DB-API 2.0 connection.
        chunk_size:  Rows per Parquet file (default 100 000).
        compression: snappy | gzip | zstd | none
        mode:        What to do when the dataset already exists:
                     ``"error"``   — raise FileExistsError (default, safe).
                     ``"replace"`` — write the new data, then swap it in for the
                                     old files (old data kept if the write fails).
                     ``"append"``  — add new part files after the existing ones;
                                     the DuckDB view is updated automatically.
        """
        tables = _stream(query, conn, chunk_size, **kwargs)
        return self._do_save(name, tables, mode, compression, chunk_size,
                             source=_source_name(conn), query=query, label="")

    def save_file(
        self,
        name: str,
        file_path: str,
        file_type: str = None,
        chunk_size: int = 100_000,
        compression: str = "snappy",
        mode: str = "error",
        use_polars: bool = False,
        **read_kwargs,
    ) -> "DataStore":
        """Read local file(s) and save as a chunked Parquet dataset.

        Reads in chunks so that large files never need to fit in memory at once
        (CSV/TXT and Parquet are streamed natively; Excel is read eagerly then
        chunked on write).  Writes are **atomic** — on failure, no partial
        files are left behind.

        **Glob support:** ``file_path`` may contain wildcards (``*``, ``?``,
        ``[...]``).  All matching files are read in sorted order and combined
        into a single dataset.  Examples::

            store.save_file("logs", "/data/logs_*.csv")
            store.save_file("sales", "/data/2024/sales_??.parquet")
            store.save_file("all", "/data/**/report.csv")   # recursive

        Parameters
        ----------
        name:        Alias used in DuckDB SQL (e.g. ``"sales"``).
        file_path:   Path or glob pattern (CSV, XLSX, XLS, XLSM, ODS, TXT, Parquet).
        file_type:   Force file format.  Inferred from extension when omitted.
        chunk_size:  Rows per Parquet file (default 100 000).
        compression: snappy | gzip | zstd | none
        mode:        ``"error"`` | ``"replace"`` | ``"append"``
        use_polars:  When True, use Polars' Rust-based batched reader for
                     CSV/TXT (~2-5× faster).  Falls back to PyArrow for
                     Parquet and pandas for Excel.  Default False.
        **read_kwargs:  Extra arguments forwarded to the file reader
                        (e.g. ``sheet_name=``, ``sep=``, ``encoding=``).
        """
        streamer = _stream_file_polars if use_polars else _stream_file
        if _has_glob(file_path):
            tables = _stream_files_glob(
                file_path, file_type, chunk_size,
                use_polars=use_polars, **read_kwargs,
            )
        else:
            tables = streamer(file_path, file_type, chunk_size, **read_kwargs)
        return self._do_save(name, tables, mode, compression, chunk_size,
                             source="file", query=f"file://{file_path}",
                             label=f" from {file_path}")

    def save_df(
        self,
        name: str,
        df,
        chunk_size: int = 100_000,
        compression: str = "snappy",
        mode: str = "error",
    ) -> "DataStore":
        """Save a DataFrame or LazyFrame as a chunked Parquet dataset.

        Accepts ``pd.DataFrame``, ``pl.DataFrame``, or ``pl.LazyFrame``.
        The data is written in chunks through the same pipeline as ``save()``
        and ``save_file()``, then registered as a DuckDB view automatically.
        Writes are **atomic**.

        Parameters
        ----------
        name:        Alias used in DuckDB SQL (e.g. ``"regions"``).
        df:          The data to save.
        chunk_size:  Rows per Parquet file (default 100 000).
        compression: snappy | gzip | zstd | none
        mode:        ``"error"`` | ``"replace"`` | ``"append"``

        Example
        -------
        lf = db2.query("SELECT * FROM SCHEMA.REGIONS", engine="polars")
        store.save_df("regions", lf)

        df = pd.read_csv("data.csv")
        store.save_df("data", df)
        """
        src_type = type(df).__name__
        tables = _stream_dataframe(df, chunk_size)
        return self._do_save(name, tables, mode, compression, chunk_size,
                             source="dataframe", query=f"from {src_type}",
                             label=f" from {src_type}")

    def save_remote(
        self,
        name: str,
        loader,
        remote_path: str,
        file_type: str = None,
        chunk_size: int = 100_000,
        compression: str = "snappy",
        mode: str = "error",
        **read_kwargs,
    ) -> "DataStore":
        """Download a remote file via SFTP and save it as a chunked Parquet dataset.

        The file is downloaded to a temporary directory, streamed to chunked
        Parquet through the same pipeline as ``save_file()``, then the temp
        file is cleaned up.  Writes are **atomic** — on failure, no partial
        Parquet files are left behind (the downloaded temp file is also removed).

        Parameters
        ----------
        name:        Alias used in DuckDB SQL (e.g. ``"customers"``).
        loader:      An ``SFTPDataLoader`` instance (connected or not — auto-connects).
        remote_path: Absolute path of the file on the remote server.
        file_type:   Force file format.  Inferred from the remote filename when omitted.
        chunk_size:  Rows per Parquet file (default 100 000).
        compression: snappy | gzip | zstd | none
        mode:        ``"error"`` | ``"replace"`` | ``"append"``
        **read_kwargs:  Extra arguments forwarded to the file reader
                        (e.g. ``sep=``, ``sheet_name=``, ``encoding=``).

        Example
        -------
        loader = SFTPDataLoader(hostname="sftp.example.com", username="user", pkey="/key")
        store.save_remote("customers", loader, "/data/customers.csv")
        store.save_remote("sales", loader, "/data/sales.csv", sep="|", skiprows=1)
        loader.close()
        """
        # Auto-connect if the loader has no active SFTP session
        if loader.sftp is None:
            loader.connect()

        # Download to a temp file, preserving the original extension
        remote_filename = Path(remote_path).name
        with tempfile.TemporaryDirectory() as dl_dir:
            local_tmp = os.path.join(dl_dir, remote_filename)
            _print(f"Downloading {remote_path} → {local_tmp}")
            loader.sftp.get(remote_path, local_tmp)

            tables = _stream_file(local_tmp, file_type, chunk_size, **read_kwargs)
            return self._do_save(
                name, tables, mode, compression, chunk_size,
                source="sftp", query=f"sftp://{loader.hostname}{remote_path}",
                label=f" from sftp://{loader.hostname}{remote_path}",
            )

    def enable_magic(self) -> None:
        """Load %%parqstore, %%dql, %%save, and %%query magics in the current
        Jupyter notebook.

        After calling this once per session, the unified ``%%parqstore``
        magic is available with ``--save`` and ``--query`` actions.
        ``%%save``, ``%%query``, and ``%%dql`` are shorthand aliases.

            # ── %%parqstore --query ──────────────────────────────────
            %%parqstore --query --var df_summary
            SELECT region, SUM(amount) FROM sales GROUP BY region

            # ── %%parqstore --save: SQL ──────────────────────────────
            %%parqstore --save sales_2024 --conn db2
            SELECT * FROM SCHEMA.SALES WHERE YEAR = 2024

            # ── %%parqstore --save: file ─────────────────────────────
            %%parqstore --save products --file --file-type xlsx
            /data/products.xlsx
            sheet_name=Sheet2

            # ── %%parqstore --save: DataFrame ────────────────────────
            %%parqstore --save regions --df lf_regions

            # ── %%parqstore --save: SFTP ─────────────────────────────
            %%parqstore --save customers --remote my_loader
            /data/customers.csv

            # ── shorthand aliases (same behaviour) ───────────────────
            %%query --var df_summary
            SELECT region, SUM(amount) FROM sales GROUP BY region

            %%save sales_2024 --conn db2
            SELECT * FROM SCHEMA.SALES WHERE YEAR = 2024

            %%dql
            SELECT year, SUM(amount) FROM sales GROUP BY year

        Must be called inside a running Jupyter kernel.
        """
        try:
            from IPython import get_ipython
            from IPython.core.magic import register_cell_magic
            ip = get_ipython()
            if ip is None:
                raise RuntimeError("Not running inside a Jupyter kernel.")
        except ImportError:
            raise RuntimeError("IPython is not available.")

        # ── %%dql magic (magic-duckdb) ────────────────────────────────────
        ip.user_ns["_store_ddb"] = self.ddb
        ip.run_line_magic("load_ext", "magic_duckdb")
        ip.run_line_magic("dql", "-co _store_ddb")

        # Save a reference to the original magic-duckdb %%dql so we can
        # delegate to it when --polars is not used.
        _original_dql = ip.magics_manager.magics.get("cell", {}).get("dql")

        # ── core dispatch functions ──────────────────────────────────────
        store = self  # capture for closure

        def _do_save_magic(line, cell):
            """Dispatch logic for the save action."""
            import argparse
            parser = argparse.ArgumentParser(prog="%%parqstore --save", add_help=False)
            parser.add_argument("name")
            parser.add_argument("--mode", default="error")
            parser.add_argument("--chunk-size", type=int, default=100_000)
            parser.add_argument("--compression", default="snappy")
            parser.add_argument("--conn", default=None)
            parser.add_argument("--file", action="store_true")
            parser.add_argument("--file-type", default=None)
            parser.add_argument("--polars", action="store_true")
            parser.add_argument("--df", default=None)
            parser.add_argument("--remote", default=None)
            args = parser.parse_args(line.split())

            if args.remote:
                if args.remote not in ip.user_ns:
                    raise NameError(
                        f"Variable '{args.remote}' not found in the notebook namespace."
                    )
                loader = ip.user_ns[args.remote]
                lines = [l.strip() for l in cell.strip().splitlines()]
                remote_path = lines[0]
                read_kwargs = _parse_reader_options(lines[1:])
                store.save_remote(
                    name=args.name,
                    loader=loader,
                    remote_path=remote_path,
                    file_type=args.file_type,
                    chunk_size=args.chunk_size,
                    compression=args.compression,
                    mode=args.mode,
                    **read_kwargs,
                )
            elif args.df:
                if args.df not in ip.user_ns:
                    raise NameError(
                        f"Variable '{args.df}' not found in the notebook namespace."
                    )
                store.save_df(
                    name=args.name,
                    df=ip.user_ns[args.df],
                    chunk_size=args.chunk_size,
                    compression=args.compression,
                    mode=args.mode,
                )
            elif args.file:
                lines = [l.strip() for l in cell.strip().splitlines()]
                file_path = lines[0]
                read_kwargs = _parse_reader_options(lines[1:])
                store.save_file(
                    name=args.name,
                    file_path=file_path,
                    file_type=args.file_type,
                    chunk_size=args.chunk_size,
                    compression=args.compression,
                    mode=args.mode,
                    use_polars=args.polars,
                    **read_kwargs,
                )
            else:
                if args.conn is None:
                    conn = store.ddb
                elif args.conn in store._connections or args.conn in store._BUILTIN_TYPES:
                    conn = store.connect(args.conn)
                elif args.conn in ip.user_ns:
                    conn = ip.user_ns[args.conn]
                else:
                    avail = sorted(set(store._connections.keys()) | store._BUILTIN_TYPES)
                    raise ValueError(
                        f"'{args.conn}' is not a connection name, built-in type, "
                        f"or notebook variable. "
                        f"Available: {avail}"
                    )

                store.save(
                    name=args.name,
                    query=cell.strip(),
                    conn=conn,
                    chunk_size=args.chunk_size,
                    compression=args.compression,
                    mode=args.mode,
                )

        def _do_query_magic(line, cell, implicit_dql=False):
            """Dispatch logic for the query action.

            Parameters
            ----------
            line : str
                The magic line arguments.
            cell : str
                The cell body (SQL).
            implicit_dql : bool
                When True, ``--dql`` behaviour is on by default (used by the
                ``%%dql`` alias so that bare ``%%dql`` delegates to magic-duckdb).
            """
            import argparse
            parser = argparse.ArgumentParser(prog="%%query", add_help=False)
            parser.add_argument("--var", default=None)
            parser.add_argument("--polars", action="store_true")
            parser.add_argument("--dql", action="store_true")
            parser.add_argument("--create", default=None)
            parser.add_argument("--persist", default=None)
            parser.add_argument("--mode", default="error",
                                choices=["error", "replace", "append"])
            parser.add_argument("--chunk-size", type=int, default=100_000)
            parser.add_argument("--compression", default="snappy")
            args = parser.parse_args(line.split() if line.strip() else [])

            use_dql = args.dql or implicit_dql
            sql = cell.strip()

            # --persist: query → chunked Parquet → register DuckDB view
            if args.persist:
                result_df = store.ddb.execute(sql).df()
                store.save_df(
                    args.persist, result_df,
                    chunk_size=args.chunk_size,
                    compression=args.compression,
                    mode=args.mode,
                )
                if use_dql and _original_dql is not None:
                    return _original_dql("", f"SELECT * FROM {_quote_ident(args.persist)}")
                from IPython.display import display
                display(store.query(f"SELECT * FROM {_quote_ident(args.persist)}"))
                return

            # --create: save as DuckDB in-memory table (queryable in subsequent cells)
            if args.create:
                store.ddb.execute(
                    f"CREATE OR REPLACE TABLE {_quote_ident(args.create)} AS {sql}"
                )
                row_count = store.ddb.execute(
                    f"SELECT COUNT(*) FROM {_quote_ident(args.create)}"
                ).fetchone()[0]
                _print(f"Created DuckDB table '{args.create}'  ({row_count:,} rows)")

            # --dql / implicit_dql with no capture flags: delegate entirely
            # to original magic-duckdb (fastest path, full rendering).
            # When --var or --polars is set we must run our own path to
            # execute and capture the result; dql rendering happens after.
            if use_dql and _original_dql is not None and not args.var and not args.polars:
                display_sql = f"SELECT * FROM {_quote_ident(args.create)}" if args.create else sql
                return _original_dql("", display_sql)

            engine = "polars" if args.polars else "pandas"
            result = store.query(sql, engine=engine)

            if args.var:
                ip.user_ns[args.var] = result
                if args.polars:
                    import polars as pl
                    collected = result.collect()
                    _print(f"Result → {args.var}  ({len(collected):,} rows, "
                          f"{len(collected.columns)} columns)  [pl.LazyFrame]")
                else:
                    _print(f"Result → {args.var}  ({len(result):,} rows, "
                          f"{len(result.columns)} columns)  [pd.DataFrame]")

            # Render: explicit --dql → magic-duckdb; otherwise IPython display
            if args.dql and _original_dql is not None:
                display_sql = f"SELECT * FROM {_quote_ident(args.create)}" if args.create else sql
                return _original_dql("", display_sql)

            from IPython.display import display
            display(result.collect() if args.polars else result)
            return result

        def _do_list_magic(line):
            """List / search table files on SFTP via ``loader.list_remote``.

            Shows up to ``--limit`` rows (default 200) instead of pandas'
            60-row truncation.  ``--var NAME`` also stores the DataFrame.
            """
            from IPython.display import display
            from .sftp_data_loader import _list_remote_from_line

            df, limit, var = _list_remote_from_line(line, ip.user_ns)
            if var:
                ip.user_ns[var] = df
                _print(f"Result → {var}")
            with pd.option_context("display.max_rows", limit,
                                   "display.min_rows", min(limit, 20)):
                display(df)

        # ── %%parqstore / %parqstore — unified magic ─────────────────────
        # Registered as both line and cell magic so body-less calls like
        # ``%parqstore --save regions --df lf_regions`` work.
        def _parqstore_magic(line, cell=""):
            """Unified DataStore magic.

            %%parqstore --save <name> [save options...]
            %%parqstore --query [query options...]

            --save   Stream data to chunked Parquet (same as %%save).
            --query  Query datasets and return a DataFrame (same as %%query).
            --list   List / search table files on SFTP:
                     %parqstore --list <loader> [dir] [pattern] [-r]
                         [--max-depth N] [--types csv,parquet | --all-files]
                         [--sort modified|name|size|path] [--limit N] [--var NAME]

            Examples:
                %parqstore --list my_loader /data/exports
                %parqstore --list my_loader /data "*sales*" -r --sort name --var files

                %%parqstore --save sales_2024 --conn db2
                SELECT * FROM SCHEMA.SALES WHERE YEAR = 2024

                %%parqstore --save products --file
                /data/products.csv
                sep=|

                %parqstore --save regions --df lf_regions

                %%parqstore --query --var df_summary
                SELECT region, SUM(amount) FROM sales GROUP BY region
            """
            parts = line.strip().split(None, 1)
            if not parts:
                raise ValueError(
                    "Usage: %%parqstore --save <name> [options]  or  "
                    "%%parqstore --query [options]"
                )

            action = parts[0]
            rest = parts[1] if len(parts) > 1 else ""

            if action == "--save":
                _do_save_magic(rest, cell)
            elif action == "--query":
                return _do_query_magic(rest, cell)
            elif action == "--list":
                _do_list_magic(rest)
            else:
                raise ValueError(
                    f"Unknown action '{action}'. "
                    "Use --save, --query, or --list."
                )

        ip.register_magic_function(_parqstore_magic, magic_kind="line", magic_name="parqstore")
        ip.register_magic_function(_parqstore_magic, magic_kind="cell", magic_name="parqstore")

        # ── %%save / %save alias ──────────────────────────────────────
        # Registered as both line AND cell magic so that body-less calls
        # like ``%save regions --df lf_regions`` work (IPython rejects
        # cell magics with an empty body before our code can run).
        def _save_magic(line, cell=""):
            """Alias for %%parqstore --save. See %%parqstore for full docs."""
            _do_save_magic(line, cell)

        ip.register_magic_function(_save_magic, magic_kind="line", magic_name="save")
        ip.register_magic_function(_save_magic, magic_kind="cell", magic_name="save")

        # ── %%query / %query alias ────────────────────────────────────
        def _query_magic(line, cell=""):
            """Alias for %%parqstore --query. See %%parqstore for full docs."""
            return _do_query_magic(line, cell)

        ip.register_magic_function(_query_magic, magic_kind="line", magic_name="query")
        ip.register_magic_function(_query_magic, magic_kind="cell", magic_name="query")

        # ── %%dql override ──────────────────────────────────────────────
        # Wraps the original magic-duckdb %%dql.  Without flags it
        # delegates to magic-duckdb; with flags it routes through the
        # same _do_query_magic used by %%query (with implicit_dql=True).
        @register_cell_magic
        def dql(line, cell):
            """DuckDB query magic — superset of magic-duckdb %%dql.

            Without flags, behaves exactly like the original magic-duckdb %%dql.
            With any flag, uses the same engine as %%query (implicit --dql).

            Flags (same as %%query):
                --var <name>       Store the result in a notebook variable
                --polars           Return a pl.LazyFrame instead of pd.DataFrame
                --create <name>    Create a DuckDB in-memory table
                --persist <name>   Save to chunked Parquet + register DuckDB view
                --mode             error | replace | append  (for --persist)
                --chunk-size N     Rows per Parquet part  (for --persist)
                --compression TYPE snappy | gzip | zstd   (for --persist)

            Examples:
                %%dql
                SELECT * FROM sales LIMIT 10

                %%dql --var lf_sales --polars
                SELECT * FROM sales WHERE amount > 1000

                %%dql --persist sales_summary
                SELECT region, SUM(amount) FROM sales GROUP BY region
            """
            parts = line.strip().split()
            has_flags = any(p.startswith("--") for p in parts)

            # No flags → delegate to original magic-duckdb %%dql
            if not has_flags and _original_dql is not None:
                return _original_dql(line, cell)

            return _do_query_magic(line, cell, implicit_dql=True)

        datasets = list(self._catalog.keys())
        conns = list(self._connections.keys())
        _print(f"%%parqstore, %%dql, %%save, and %%query magic enabled — {len(datasets)} dataset(s): {datasets}")
        if conns:
            _print(f"Connections: {conns}")

    def query(self, sql: str, engine: Optional[str] = None):
        """Execute SQL against any registered dataset.

        Parameters
        ----------
        sql : str
            DuckDB-compatible SQL statement.
        engine : str, optional
            ``"pandas"`` → ``pd.DataFrame`` (default)
            ``"polars"`` → ``pl.LazyFrame``

        Returns
        -------
        pd.DataFrame or pl.LazyFrame
        """
        import polars as pl
        from .framework_config import _resolve_engine
        from .helpers import duckdb_to_arrow

        resolved = _resolve_engine(engine)
        if resolved == "polars":
            return pl.from_arrow(duckdb_to_arrow(self.ddb, sql)).lazy()
        return self.ddb.execute(sql).df()

    def sql(self, sql: str):
        """Execute SQL and return a DuckDB Relation (lazy; call .df() / .show())."""
        return self.ddb.sql(sql)

    def list(self) -> pd.DataFrame:
        """Return a DataFrame describing all saved datasets."""
        if not self._catalog:
            _print("No datasets saved yet.")
            return pd.DataFrame()
        rows = [
            {
                "name": k,
                "rows": v["rows"],
                "chunks": v["chunks"],
                "source": v["source"],
                "chunk_size": v["chunk_size"],
                "compression": v["compression"],
                "created_at": v["created_at"],
                "path": str(self._dataset_path(k)),
            }
            for k, v in self._catalog.items()
        ]
        return pd.DataFrame(rows)

    def drop(self, name: str, files: bool = False):
        """Unregister a dataset. Pass ``files=True`` to also delete the Parquet files."""
        if name in self._catalog:
            if files:
                path = self._dataset_path(name)
                shutil.rmtree(path, ignore_errors=True)
                _print(f"Deleted files at {path}")
            del self._catalog[name]
            self._save_catalog()
        self.ddb.execute(f"DROP VIEW IF EXISTS {_quote_ident(name)}")
        _print(f"Dropped '{name}'")

    def clear_all(self, files: bool = False):
        """Drop ALL datasets from the store.

        Parameters
        ----------
        files : bool
            When True, also delete all Parquet files from disk.
            When False (default), only unregister the DuckDB views and clear
            the catalog — the Parquet files remain on disk for manual recovery.
        """
        if not self._catalog:
            _print("Store is already empty.")
            return

        names = list(self._catalog.keys())
        for name in names:
            if files:
                shutil.rmtree(self._dataset_path(name), ignore_errors=True)
            self.ddb.execute(f"DROP VIEW IF EXISTS {_quote_ident(name)}")

        count = len(names)
        self._catalog.clear()
        self._save_catalog()
        verb = "Cleared" if files else "Unregistered"
        _print(f"{verb} {count} dataset(s): {names}")

    def reload(self, name: str):
        """Re-register a view from disk (useful after adding files manually)."""
        if name not in self._catalog:
            raise KeyError(f"'{name}' not in catalog. Use save() to add it first.")
        self._register(name, self._dataset_path(name))
        _print(f"Reloaded '{name}'")

    def info(self, name: str) -> dict:
        """Return catalog metadata for a dataset."""
        if name not in self._catalog:
            raise KeyError(f"'{name}' not in catalog.")
        return self._catalog[name]

    def __del__(self):
        self.disconnect()

    def __repr__(self) -> str:  # noqa: D105
        names = list(self._catalog)
        return f"DataStore('{self.base_dir}', datasets={names})"

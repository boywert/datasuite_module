# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`datasuite` is a Python utility library for data operations. It provides secure database connectivity (IBM DB2, Impala/Hive) via SSH tunneling, local file I/O, in-memory SQL, SFTP data loading with caching, Excel export, and a persistent Parquet data store with DuckDB querying.

**Python**: >= 3.12 | **Version**: see `setup.py`

## Installation & Development

```bash
pip install -e .
```

All dependencies (including `duckdb`, `pyyaml`, `paramiko`, `google-cloud-bigquery`, `ipython`, `magic-duckdb`) are listed in `setup.py` `install_requires`.
Versions are ranges: floor = tested version, cap = below next major (next minor for 0.x). For the exact tested environment: `pip install -e ".[dev]" -c constraints-py312.txt` (regenerate with `pip freeze --exclude-editable` after a passing test run when upgrading).

## Tests

```bash
python -m pytest tests -q
```

- pytest suite in `tests/` (~153 tests, all passing); no real DB/SFTP needed — SFTP tests use an in-memory fake (`tests/test_sftp_data_loader.py`); `tests/test_bugfixes.py` covers the 2026-11 fixes (tunnel, dbc_info, quoting, catalog paths, cp874).
- Minimal deps to run them: `duckdb pyarrow pandas polars pyyaml pytest openpyxl xlsxwriter psutil` (+ `ipython magic-duckdb` for magic checks).
- Version compatibility: duckdb 2.x removed `.arrow()` — always go through `helpers.duckdb_to_arrow(con, sql)`; polars 2.x removed `read_csv_batched` — `_stream_file_polars` uses `scan_csv().collect_batches()` when available.

## Documentation

**Rule: every time code in `datasuite/` changes (new feature, changed behaviour, renamed/removed API, new option), update the docs in the same piece of work — not as a follow-up.** Check each of these and update whichever the change touches:

| File | What to keep in sync |
|---|---|
| `docs/manual.md` | The user manual for every feature: signatures, options, defaults, examples, error messages, troubleshooting table. |
| `docs/datastore.md` | Only a pointer into the manual's DataStore section — update its links if manual headings change. |
| `README.md` | Feature summary, install command, imports. |
| `examples/datasuite_tour.ipynb` | Runnable tour of every feature (offline; `LIVE_*` flags for DB/SSH/SFTP; SFTP demo uses a local-folder stand-in for paramiko's SFTP client). Add/adjust cells, then **re-execute it end to end with 0 errors** and save it **without outputs** (outputs would leak local paths). |
| `examples/test_magic_features.ipynb` | Older magic test notebook — fix any cell the change breaks. |
| `docs/intro_deck.html` | Slide deck for SQL users new to Python (`<section class="slide">` per slide). Update code examples / commands it shows; keep the yellow `<span class="todo">` placeholders for the owner to fill. Republish it as an Artifact if it was published in the session. |
| `CLAUDE.md` | This file — architecture notes, test count, compatibility notes. |

When finishing a change, say which docs were updated (or why none needed it). Never add company/bank names or internal hostnames to any of these files (the repo is meant to be published).

## Module Architecture

All public classes and functions are exported from `datasuite/__init__.py` via wildcard imports.

### SSH Tunneling (`ssh_tunnel_connect.py`)
- `SSHTunnelConnect(tunnel_type, ...)` — wraps both subprocess SSH (`'ssh'`) and `SSHTunnelForwarder` (`'python'`) tunnel types
- Auto-selects an available local port in the range **8801–8899**
- Exposes `.connect()`, `.close()`, and `.port_selection()`; `connect()` raises on failure (after cleanup)
- `'ssh'` type runs `ssh -N -L` in the foreground (no `-f`) from an argument list, waits until the local port accepts connections (`connect_timeout`), and a `weakref.finalize` kills it on GC/exit

### Database Connectivity (`db_connect.py`)
- `dbc_info` — reads DB credentials from environment variables (see below); `DB2Connect(**kw)` / `ImpalaConnect(**kw)` apply overrides to a per-instance copy (unknown names raise `TypeError`)
- `DB2Connect` — IBM DB2 via SSH tunnel; uses `'ssh'` tunnel type
- `ImpalaConnect` — Impala/Hive via SSH tunnel; uses `'python'` tunnel type with LDAP auth
- Both classes auto-detect the hostname via `helpers.is_direct_access_host()`: on a host listed in `DATASUITE_DIRECT_HOSTS` (env, comma-separated) or `helpers.DIRECT_ACCESS_HOSTS` (code, empty by default) they connect directly; otherwise they tunnel automatically
- `query()` reuses the connection returned by `open_db2_connection()` / `open_impala_connection()` and closes it; errors propagate (no more `None` return); `query_db2` / `query_impala*` remain for backward compatibility
- Queries return `pandas.DataFrame`; chunked at 100k rows for memory efficiency

### SFTP Data Loader (`sftp_data_loader.py`)
- `SFTPDataLoader` — lazy-connecting SFTP client supporting direct or bastion/jump-host proxied connections
- Caches remote files locally as Parquet using MD5(host+path+params) as the cache key; default TTL = 1 hour
- Supports context manager (`with` statement) and password or key-based auth
- Key method: `read_remote(remote_path, file_type, **kwargs)` → `pd.DataFrame`; `file_type` is inferred from the remote path's extension when omitted
- `list_remote(remote_dir, pattern, recursive, max_depth, file_types, sort, tz, verbose)` → DataFrame of table files (`name, folder, file_type, size, modified, path, size_bytes`), newest first, with a summary line and recursive progress
- `_list_remote_from_line()` backs the `%parqstore --list <loader> [dir] [pattern] ...` magic

### Local File I/O
- `read_local(filepath_or_buffer, file_type, **kwargs)` — reads CSV, TXT, XLSX, XLS, XLSM, ODS, Parquet into a DataFrame; file type auto-detected from extension
- `export_excel(filepath, sheet_df, pretty_format=True)` — multi-sheet Excel export with autofilter, frozen header row, auto-width columns, number formatting (uses `xlsxwriter`)

### In-Memory SQL (`local_query.py`)
- `local_query(sql, **kwargs)` — runs SQL via DuckDB; auto-discovers `pd.DataFrame` variables from the **caller's local scope** and registers them as DuckDB tables; extra DataFrames can be passed as kwargs

### Configuration (`framework_config.py`)
- `FrameworkConfig` — pluggable engine config (default: DuckDB + pandas); used internally by `read_local`

### Parquet Data Store (`parquet_store.py`)
- `DataStore` — persistent store that streams query results (BigQuery, Impala, DB2) to chunked Parquet on disk and exposes each dataset as a DuckDB view
- Supports save modes: `error` (default), `replace`, `append`; all writes go to a temp dir first — `replace` swaps the old directory out only after the new write succeeds (`_safe_write`)
- Views use `read_parquet(..., union_by_name=true)` so part files with differing schemas merge
- `enable_magic()` registers `%%parqstore` (`--save` / `--query` / `--list`), `%%save`, `%%query`, and wraps magic-duckdb's `%%dql` (note: `%save` overrides IPython's built-in)
- Connection configs loaded from `connections.yaml` (path via `DATASTORE_CONNECTIONS` env var or `/etc/datastore/connections.yaml`); supports `${VAR}` env expansion
- Catalog stored as `.catalog.json` in the base directory; each dataset is a subdirectory of Parquet part files
- Catalog `path` is stored relative to `base_dir` (POSIX); `_dataset_path()` resolves it and still accepts old cwd-relative / backslash entries. Dataset names are quoted with `_quote_ident()` in all SQL and validated (no `/ \ :`, no leading `.`)
- All console output goes through `_print()`, which falls back to ASCII (`✓`→`OK`, `→`→`->`) on code pages like cp874
- **Not yet exported** from `__init__.py` — must be imported directly: `from datasuite.parquet_store import DataStore`
- Additional dependencies beyond `setup.py`: `pyyaml`, `duckdb`, `google-cloud-bigquery` (optional)

## Environment Variables

| Variable | Purpose |
|---|---|
| `DB2_SERVER` | DB2 host |
| `DB2_USER` / `DB2_PASS` | DB2 credentials |
| `IMPALA_SERVER` | Impala host |
| `IMPALA_USER` / `IMPALA_PASS` | Impala credentials |
| `BASTION_SERVER` | Bastion/jump host |
| `BASTION_USER` | Bastion username |
| `BASTION_PRIVATE_KEY` | Path to SSH private key |
| `DATASUITE_DIRECT_HOSTS` | Comma-separated hostnames with direct DB access (no tunnel) |
| `DATASTORE_CONNECTIONS` | Path to `connections.yaml` for `DataStore` |

Legacy bastion names `RETNL_SERVER` / `RETNL_USER` / `RETNL_PRIVATE_KEY` are still read as a fallback (`helpers.getenv_any`); document only the `BASTION_*` names.

## Key Design Decisions

- **Host-aware connections**: `helpers.direct_access_hosts()` (= `DIRECT_ACCESS_HOSTS` + `DATASUITE_DIRECT_HOSTS`, read per call) is the single source of truth for hosts with direct DB access (no tunnel); no internal hostnames are hard-coded; both `db_connect` and `parquet_store` use `helpers.is_direct_access_host()`.
- **Port range 8801–8899**: SSH tunnel local ports are chosen from this range; `psutil` is used to check occupancy.
- **DuckDB caller-scope discovery**: `local_query` uses `inspect` to walk the caller's frame and register all `pd.DataFrame` locals automatically — no manual registration needed for simple notebook usage.

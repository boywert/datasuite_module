# DataSuite Manual

DataSuite is a Python toolkit for everyday data work. For a runnable walk-through of every
feature, open [`examples/datasuite_tour.ipynb`](../examples/datasuite_tour.ipynb).


| Feature | Module | Main entry points |
|---|---|---|
| Query IBM DB2 and Impala/Hive, with automatic SSH tunnelling | `db_connect`, `ssh_tunnel_connect` | `DB2Connect`, `ImpalaConnect`, `SSHTunnelConnect` |
| Read local files (CSV, TXT, Excel, ODS, Parquet) | `read_local` | `read_local()` |
| Run SQL over in-memory DataFrames | `local_query` | `local_query()` |
| Export formatted multi-sheet Excel workbooks | `export_excel` | `export_excel()` |
| Browse and load files from SFTP servers (with bastion + cache) | `sftp_data_loader` | `SFTPDataLoader` |
| Persistent Parquet data store with DuckDB SQL and Jupyter magics | `parquet_store` | `DataStore` |
| Library-wide defaults (pandas vs polars output) | `framework_config` | `FrameworkConfig` |

```
 DB2 / Impala / BigQuery ─┐
 SFTP files ──────────────┤                         ┌─► pandas / polars DataFrame
 local files ─────────────┼─► DataSuite readers ────┤
 DataFrames ──────────────┘                         ├─► Excel workbook (export_excel)
                                                    └─► DataStore: chunked Parquet ─► DuckDB SQL / %%dql
```

---

## Table of Contents

1. [Installation](#1-installation)
2. [Configuration](#2-configuration)
   - [FrameworkConfig — pandas or polars](#frameworkconfig--pandas-or-polars)
   - [Environment variables](#environment-variables)
   - [Direct access vs. SSH tunnel](#direct-access-vs-ssh-tunnel)
3. [Database queries — DB2Connect / ImpalaConnect](#3-database-queries--db2connect--impalaconnect)
4. [SSH tunnels — SSHTunnelConnect](#4-ssh-tunnels--sshtunnelconnect)
5. [Reading local files — read_local](#5-reading-local-files--read_local)
6. [SQL on DataFrames — local_query](#6-sql-on-dataframes--local_query)
7. [Excel export — export_excel](#7-excel-export--export_excel)
8. [SFTP — SFTPDataLoader](#8-sftp--sftpdataloader)
   - [Connecting](#connecting)
   - [Finding files — list_remote](#finding-files--list_remote)
   - [Loading files — read_remote](#loading-files--read_remote)
9. [DataStore — persistent Parquet store](#9-datastore--persistent-parquet-store)
   - [Quick start](#quick-start)
   - [Constructor](#constructor)
   - [Connections](#connections)
   - [Saving data](#saving-data)
   - [Querying data](#querying-data)
   - [Managing datasets](#managing-datasets)
   - [Jupyter magics](#jupyter-magics)
   - [File layout](#file-layout)
10. [Error handling and console output](#10-error-handling-and-console-output)
11. [API reference](#11-api-reference)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Installation

Requires **Python 3.12+**.

```bash
pip install -e .                 # library
pip install -e ".[dev]"          # + pytest (for tests)
```

All runtime dependencies are declared in `setup.py` as version ranges: the
floor is the version the test suite passed on, the cap is below the next major
release. To reproduce the exact tested environment, install with the lock file:

```bash
pip install -e ".[dev]" -c constraints-py312.txt
```

> **IBM DB2 driver:** `ibm_db` needs the IBM CLI driver. On Windows the DLLs are
> found automatically in `site-packages/clidriver/bin`. If `ibm_db` cannot be
> loaded, the library still imports — `DB2Connect` fails only when used.

**Imports**

```python
from datasuite import (
    DB2Connect, ImpalaConnect, SSHTunnelConnect, SFTPDataLoader,
    read_local, local_query, export_excel, FrameworkConfig,
)
from datasuite.parquet_store import DataStore     # not re-exported from datasuite
```

**Running the tests**

```bash
python -m pytest tests -q        # no database, SSH or SFTP server needed
```

---

## 2. Configuration

### FrameworkConfig — pandas or polars

Every function that returns a table takes an `engine=` argument:

| `engine` | Returns |
|---|---|
| `"pandas"` (default) | `pd.DataFrame` |
| `"polars"` | `pl.LazyFrame` (call `.collect()` to materialise) |

Set the default once for the whole session:

```python
from datasuite import FrameworkConfig
FrameworkConfig.dataframe_engine = "polars"     # every reader now returns pl.LazyFrame
```

A per-call `engine=` always wins over the global default. Any value other than
`"pandas"` / `"polars"` raises `ValueError`.

Applies to: `DB2Connect.query`, `ImpalaConnect.query`, `read_local`,
`local_query`, `SFTPDataLoader.read_remote`, `DataStore.query`.

### Environment variables

Credentials are never hard-coded; they come from the environment.

| Variable | Used by | Purpose |
|---|---|---|
| `DB2_SERVER`, `DB2_USER`, `DB2_PASS` | `DB2Connect`, DataStore `"db2"` | DB2 host and credentials (port 50000, database `BLUDB`) |
| `IMPALA_SERVER`, `IMPALA_USER`, `IMPALA_PASS` | `ImpalaConnect`, DataStore `"impala"` | Impala host and LDAP credentials (port 21050) |
| `BASTION_SERVER`, `BASTION_USER`, `BASTION_PRIVATE_KEY` | tunnels | SSH bastion (jump host), user, private key path |
| `DATASUITE_DIRECT_HOSTS` | all DB connections | Comma-separated hostnames that reach the databases directly (no tunnel) |
| `DATASTORE_CONNECTIONS` | `DataStore` | Path to `connections.yaml` |

> The `DB2_*` / `IMPALA_*` / `BASTION_*` values are read **when `datasuite` is
> imported**. Set them before the import (or restart the kernel), or pass
> overrides to the constructor — see [section 3](#3-database-queries--db2connect--impalaconnect).
>
> Older setups that use `RETNL_SERVER`, `RETNL_USER` and `RETNL_PRIVATE_KEY`
> keep working: those names are read when the `BASTION_*` ones are not set.

### Direct access vs. SSH tunnel

By default every database connection goes through the SSH bastion. On
machines that can reach the databases directly, list their hostnames
(matched as a substring of the machine's hostname) in `DATASUITE_DIRECT_HOSTS`:

```bash
export DATASUITE_DIRECT_HOSTS="analytics-jupyter,etl-runner"
```

On those machines `DB2Connect`, `ImpalaConnect` and `DataStore.connect()`
connect straight to the database. The variable is read on every connection,
so it can also be set from a notebook (`os.environ[...] = ...`). Hosts can
also be added in code by extending `datasuite.helpers.DIRECT_ACCESS_HOSTS`.

```python
from datasuite.helpers import is_direct_access_host
is_direct_access_host()          # True / False for this machine
```

---

## 3. Database queries — DB2Connect / ImpalaConnect

```python
from datasuite import DB2Connect, ImpalaConnect

db2 = DB2Connect()
df = db2.query("SELECT * FROM SCHEMA.SALES FETCH FIRST 100 ROWS ONLY")

impala = ImpalaConnect()
lf = impala.query("SELECT * FROM db.orders", engine="polars")   # pl.LazyFrame
```

Each `query()` call:

1. opens a tunnel if this is not a direct-access host,
2. connects (DB2: `ibm_db`; Impala: `impyla` with LDAP auth),
3. fetches the result in **100 000-row chunks**,
4. closes the connection and the tunnel — even if the query fails.

**Overriding connection settings** — pass any `dbc_info` attribute as a
keyword. The override applies to that object only:

```python
prod = DB2Connect()
test = DB2Connect(db2_server="db2-test.example.com", db2_user="tester")
```

| Keyword | Default |
|---|---|
| `db2_server`, `db2_user`, `db2_pass` | `DB2_SERVER`, `DB2_USER`, `DB2_PASS` |
| `db2_server_port` | `50000` |
| `impala_server`, `impala_user`, `impala_pass` | `IMPALA_SERVER`, `IMPALA_USER`, `IMPALA_PASS` |
| `impala_server_port` | `21050` |
| `remote_host`, `remote_port`, `remote_user`, `remote_private_key` | `BASTION_SERVER`, `22`, `BASTION_USER`, `BASTION_PRIVATE_KEY` |

A misspelt keyword raises `TypeError` (e.g. `DB2Connect(db2_sever=...)`).

**Errors** — connection and SQL errors are raised, not printed:

```python
try:
    df = db2.query("SELECT ...")
except Exception as e:
    print("query failed:", e)
```

**Low-level helpers** — `open_db2_connection(dbc=None)` and
`open_impala_connection(dbc=None)` return a raw DB-API connection plus the
tunnel (or `None`). You must close both:

```python
from datasuite.db_connect import open_db2_connection
conn, tunnel = open_db2_connection()
try:
    cur = conn.cursor(); cur.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1"); print(cur.fetchall())
finally:
    conn.close()
    if tunnel: tunnel.close()
```

Older methods (`DB2Connect.query_db2(...)`, `ImpalaConnect.query_impala(...)`,
`ImpalaConnect.query_impala_polars(...)`) still work and take an explicit
host/port/user/password.

---

## 4. SSH tunnels — SSHTunnelConnect

Used internally by the classes above; use it directly to reach any service
behind a bastion host.

```python
from datasuite import SSHTunnelConnect

tunnel = SSHTunnelConnect(
    dest_server="db.internal", dest_port=5432,        # where you want to go
    remote_server="bastion.example.com", remote_server_port=22,
    remote_user="me", remote_private_key="~/.ssh/id_rsa",
    tunnel_type="ssh",                                 # or "python"
    connect_timeout=30,
)
tunnel.connect()             # picks a free local port in 8801–8899
print(tunnel.local_port)     # connect your client to 127.0.0.1:<local_port>
tunnel.close()
```

| `tunnel_type` | How | Default for |
|---|---|---|
| `"ssh"` | system `ssh -N -L` process | DB2 |
| `"python"` | `sshtunnel.SSHTunnelForwarder` (paramiko) | Impala |

- **`"ssh"` type** — `connect()` waits until the local port accepts connections.
  If ssh exits (bad key, host unreachable) it raises `ConnectionError` with
  ssh's own message; if the port does not open within `connect_timeout`
  seconds it raises `TimeoutError`. The ssh process is stopped by `close()`,
  and also when the object is garbage-collected or Python exits.
- **No free port** in 8801–8899 raises `RuntimeError`.
- On failure `connect()` always cleans up before raising.
- The `ssh` command must be on `PATH` (built into Windows 10+, macOS, Linux).

---

## 5. Reading local files — read_local

```python
from datasuite import read_local

df = read_local("/data/sales.csv")                         # pd.DataFrame
df = read_local("/data/sales.txt", sep="|", encoding="utf-8")
df = read_local("/data/catalog.xlsx", sheet_name="Sheet2")
lf = read_local("/data/events.parquet", engine="polars")   # pl.LazyFrame (lazy scan)
```

| Extension | pandas reader | polars reader |
|---|---|---|
| `.csv`, `.txt` | `pd.read_csv` | `pl.scan_csv` (lazy) |
| `.xlsx`, `.xls`, `.xlsm`, `.ods`, `.odf` | `pd.read_excel` | `pl.read_excel(...).lazy()` (needs `fastexcel`) |
| `.parquet`, `.pqt` | `pd.read_parquet` | `pl.scan_parquet` (lazy) |

- The type comes from the extension; override with `file_type="csv"` etc.
- Extra keyword arguments go straight to the reader, so use **pandas**
  option names with the pandas engine and **polars** names with polars
  (`sep=` vs `separator=`).
- Also accepts an open file object (bytes) — then `file_type` is required.

---

## 6. SQL on DataFrames — local_query

Run DuckDB SQL over DataFrames that are already in memory. Every
`pd.DataFrame`, `pl.DataFrame` and `pl.LazyFrame` in the **calling scope** is
available as a table under its variable name.

```python
from datasuite import local_query

sales = read_local("sales.csv")
regions = read_local("regions.xlsx")

top = local_query("""
    SELECT r.region_name, SUM(s.amount) AS total
    FROM sales s JOIN regions r USING (region_id)
    GROUP BY 1 ORDER BY total DESC
""")

lf = local_query("SELECT * FROM sales WHERE amount > 0", engine="polars")
```

- **Scope:** only the caller's *local* variables are scanned. In a notebook
  cell or script top level that means all your variables; inside a function,
  only that function's locals. Pass anything else explicitly:
  `local_query("SELECT * FROM t", t=some_function())`.
- `pl.LazyFrame` inputs are collected before querying.
- Each call uses a fresh in-memory DuckDB connection — nothing persists
  between calls. For persistent tables use [DataStore](#9-datastore--persistent-parquet-store).

---

## 7. Excel export — export_excel

```python
from datasuite import export_excel

export_excel("report.xlsx", [
    {"sheetname": "Sales",   "dataframe": df_sales},     # pandas
    {"sheetname": "Summary", "dataframe": lf_summary},   # polars LazyFrame is fine
])
```

With `pretty_format=True` (default) each sheet gets:

- autofilter on the header row,
- a frozen header row,
- columns sized to fit the widest value,
- `#,##0.00` number format on float columns.

`pretty_format=False` writes plain data. Empty DataFrames (including ones
with no columns) and non-text column names are handled. Sheet names must
follow Excel's rules (max 31 characters, no `[]:*?/\`).

---

## 8. SFTP — SFTPDataLoader

### Connecting

```python
from datasuite import SFTPDataLoader

# Direct, key-based
loader = SFTPDataLoader(hostname="sftp.example.com", username="user", pkey="/path/to/key")

# Password
loader = SFTPDataLoader(hostname="sftp.example.com", username="user", password="...")

# Through a bastion (jump host)
loader = SFTPDataLoader(
    hostname="internal-sftp", username="user", pkey="/path/to/key",
    bastion_hostname="bastion.example.com", bastion_username="jump",
    bastion_pkey="/path/to/bastion_key",
)

# As a context manager — connects on entry, closes on exit
with SFTPDataLoader(hostname="sftp.example.com", username="user", pkey="/key") as loader:
    df = loader.read_remote("/data/customers.csv")
```

| Parameter | Default | Description |
|---|---|---|
| `hostname`, `username` | — | Target SFTP server and login |
| `password` / `pkey` | `None` | Password or private-key path for the target |
| `bastion_hostname`, `bastion_username`, `bastion_password`, `bastion_pkey` | `None` | Optional jump host |
| `cache_dir` | `"./sftp_cache"` | Local Parquet cache folder |
| `cache_ttl` | `3600` | Seconds a cached file stays fresh |

The connection opens lazily on first use (or call `loader.connect()`).
If the connection drops, call `loader.connect()` again — it discards the dead
session (including the bastion) and reconnects. `loader.close()` closes the
SFTP session, the SSH connection and the bastion connection.

### Finding files — list_remote

```python
loader.list_remote("/data/exports")                                # table files, newest first
loader.list_remote("/data", pattern="*sales*", recursive=True)     # search sub-folders
loader.list_remote("/data", recursive=True, max_depth=1)           # one level down only
loader.list_remote("/data", file_types=["parquet"], sort="size")   # largest Parquet first
loader.list_remote("/data", file_types=None)                       # every file type
loader.list_remote("/data", tz="Asia/Bangkok")                     # show times in a zone
```

| Parameter | Default | Description |
|---|---|---|
| `remote_dir` | `"."` | Folder to list (login folder by default) |
| `pattern` | `"*"` | Wildcard on the file name: `*`, `?`, `[abc]` |
| `recursive` | `False` | Search sub-folders (symlinked folders are skipped); shows a live counter |
| `max_depth` | `None` | With `recursive`, how many levels down (`1` = direct sub-folders) |
| `file_types` | table formats | Extensions to keep (`["csv", ".parquet"]`); `None` = all files |
| `case_sensitive` | `False` | Case-sensitive pattern / extension match |
| `sort` | `"modified"` | `modified` (newest first), `name`, `size` (largest first), `path` |
| `ascending` | `None` | Override the sort direction |
| `tz` | `None` | Time zone for `modified`; `None` = this machine's local time |
| `verbose` | `True` | Print the summary line (and progress when recursive) |

Returns a DataFrame with columns `name, folder, file_type, size, modified,
path, size_bytes` and prints a summary such as
`12 file(s), 3.4 GB in /data  (8 folder(s) searched)`. Folders that cannot be
read are skipped and listed; the top folder itself must be readable.

```python
files = loader.list_remote("/data", pattern="txn_2024*.csv", sort="name")
big = files[files.size_bytes > 1e9]
for p in files["path"]:
    store.save_remote("txn_2024", loader, p, mode="append")
```

In Jupyter the same listing is available as `%parqstore --list` — see
[Jupyter magics](#parqstore---list--browse-sftp).

### Loading files — read_remote

```python
df = loader.read_remote("/data/customers.csv")                       # type from extension
df = loader.read_remote("/data/sales.txt", sep="|", encoding="utf-8")
lf = loader.read_remote("/data/events.parquet", engine="polars")
df = loader.read_remote("/data/export", file_type="csv")             # no extension → say it
df = loader.read_remote("/data/big.csv", use_cache=False)            # always download
```

| Parameter | Default | Description |
|---|---|---|
| `remotepath` | — | File path on the server |
| `use_cache` | `True` | Reuse a local Parquet copy younger than `cache_ttl` |
| `use_prefetch` | `True` | Prefetch the whole file (much faster for large files) |
| `engine` | config default | `"pandas"` or `"polars"` |
| `file_type` | from extension | Required when the path has no extension |
| `**kwargs` | — | Passed to [`read_local`](#5-reading-local-files--read_local) |

**Caching:** the cache key is a hash of host + path + engine + reader options,
so changing `sep=` or the engine downloads again. Delete the `cache_dir`
folder to clear the cache.

---

## 9. DataStore — persistent Parquet store

`DataStore` streams query results, files and DataFrames into chunked Parquet on
disk and registers every dataset as a DuckDB view, so you can query and join
them with SQL — without loading full datasets into memory. Datasets survive
kernel restarts.

```
source (SQL / CSV / Excel / Parquet / SFTP / DataFrame)
  │
  ▼  chunked streaming
part-0000.parquet
part-0001.parquet    ──►  DuckDB view  ──►  store.query("SELECT ...")
part-0002.parquet                            %%dql  SELECT ...
  ...
```

### Quick start

```python
from datasuite.parquet_store import DataStore

store = DataStore("/home/jovyan/work/datasets")

# From DB2 / Impala — no config file needed, uses the same env vars as DB2Connect
conn = store.connect("db2")
store.save("sales", "SELECT * FROM SCHEMA.SALES", conn=conn)
store.disconnect("db2")

# From a named connection in connections.yaml
conn = store.connect("impala_prod")
store.save("orders", "SELECT * FROM db.orders", conn=conn)
store.disconnect("impala_prod")

# From a local file and a DataFrame
store.save_file("customers", "/data/customers.csv")
store.save_df("targets", df_targets)

# Query across everything
df = store.query("SELECT s.*, c.name FROM sales s JOIN customers c USING (cust_id)")
```

### Constructor

```python
DataStore(base_dir="/home/jovyan/work/datasets", connections_file=None)
```

| Parameter | Description |
|---|---|
| `base_dir` | Folder holding all datasets and the `.catalog.json` registry. Created if missing. Relative paths are allowed. |
| `connections_file` | Path to `connections.yaml`. Resolution: this argument → `DATASTORE_CONNECTIONS` env var → `/etc/datastore/connections.yaml`. A missing file just means no named connections. |

On start-up the store loads the catalog, opens an in-memory DuckDB
connection, registers every saved dataset as a view and reads
`connections.yaml`.

### Connections

#### Built-in types (no config needed)

`"db2"` and `"impala"` work without `connections.yaml`. They use the same
environment variables and direct-access/tunnel logic as `DB2Connect` and
`ImpalaConnect`:

```python
conn = store.connect("db2")        # DB2_* + BASTION_* env vars, tunnel if needed
store.save("my_table", "SELECT * FROM SCHEMA.TABLE", conn=conn)
store.disconnect("db2")
```

#### connections.yaml

Named connections with their own hosts and credentials. All string values
support `${ENV_VAR}` expansion (an unset variable becomes an empty string).

```yaml
bigquery:
  type: bigquery
  project: my-gcp-project

impala_prod:
  type: impala
  host: impala-host
  port: 21050                    # optional, default 21050
  database: default              # optional, default "default"
  auth_mechanism: LDAP           # optional, default NOSASL
  user: ${IMPALA_USER}
  password: ${IMPALA_PASS}

db2_prod:
  type: db2
  host: ${DB2_SERVER}
  port: 50000                    # optional, default 50000
  database: BLUDB                # optional, default BLUDB
  user: ${DB2_USER}
  password: ${DB2_PASS}
```

**Supported types:** `bigquery`, `impala`, `db2`. A `db2` / `impala` entry
without `host` + `user` falls back to the built-in env-var behaviour.

```python
store.connections()              # list connections (passwords hidden)
conn = store.connect("impala_prod")
store.disconnect("impala_prod")  # close that tunnel
store.disconnect()               # close all tunnels
```

#### SSH tunnelling

Add an `ssh` block to tunnel through a bastion. The tunnel opens in
`connect()` and is skipped on a [direct-access host](#direct-access-vs-ssh-tunnel).

```yaml
impala_prod:
  type: impala
  host: impala-host
  user: ${IMPALA_USER}
  password: ${IMPALA_PASS}
  ssh:
    host: ${BASTION_SERVER}        # bastion
    port: 22                     # optional
    user: ${BASTION_USER}
    private_key: ${BASTION_PRIVATE_KEY}
    tunnel_type: python          # optional: python (Impala default) | ssh (DB2 default)
```

```python
conn = store.connect("impala_prod")         # tunnel opens
store.save("t1", "SELECT ...", conn=conn)
store.save("t2", "SELECT ...", conn=conn)   # same tunnel reused
store.disconnect("impala_prod")             # tunnel closes
```

Tunnels are also closed when the `DataStore` is garbage-collected. A tunnel
that cannot open raises an error from `connect()`.

### Saving data

All save methods share these options:

| Parameter | Default | Description |
|---|---|---|
| `chunk_size` | `100_000` | Rows per Parquet part file |
| `compression` | `"snappy"` | `snappy`, `gzip`, `zstd`, `none` |
| `mode` | `"error"` | What to do if the dataset exists — see [Save modes](#save-modes) |

**Dataset names** become folder names and SQL view names. Any name is
allowed except ones containing `/`, `\` or `:`, starting with `.`, or with
leading/trailing spaces. Names that are not plain identifiers
(e.g. `my-data`, `select`) must be double-quoted in SQL:
`SELECT * FROM "my-data"`.

#### save() — from SQL

```python
store.save(name, query, conn, chunk_size=100_000, compression="snappy", mode="error")
```

`conn` can be a `bigquery.Client`, any DB-API connection (DB2, Impala, …) or
the store's own DuckDB connection `store.ddb`.

```python
from google.cloud import bigquery
store.save("sales", "SELECT * FROM `proj.ds.sales`", conn=bigquery.Client(project="my-project"))

# BigQuery job options
store.save("sales", "SELECT ...", conn=bq, job_config=bigquery.QueryJobConfig(use_query_cache=False))

# Derive a dataset from existing ones
store.save("sales_2024", "SELECT * FROM sales WHERE year = 2024", conn=store.ddb)
```

#### save_file() — from local files

```python
store.save_file(name, file_path, file_type=None, chunk_size=100_000,
                compression="snappy", mode="error", use_polars=False, **read_kwargs)
```

**Formats:** CSV, TXT, XLSX, XLS, XLSM, ODS, Parquet.
**Globs:** `file_path` may contain `*`, `?`, `[...]` (and `**` for
recursive). All matches are read in sorted order into one dataset.

```python
store.save_file("customers", "/data/customers.csv")
store.save_file("transactions", "/data/transactions.csv",
                sep="|", skiprows=1, usecols=["date", "amount", "account_id"], encoding="utf-8")
store.save_file("logs", "/data/logs_*.csv")                        # many files → one dataset
store.save_file("reports", "/data/**/report.csv", file_type="csv") # recursive
store.save_file("products", "/data/catalog.xlsx", sheet_name="Sheet2", usecols="A,B,C")
store.save_file("events", "/data/events.parquet", columns=["event_type", "timestamp"])
store.save_file("logs", "/data/access_log.txt", sep="\t", mode="append")
store.save_file("data", "/data/export.dat", file_type="csv", sep=";")
```

**`use_polars=True`** uses Polars' multi-threaded CSV reader (~2–5× faster
for large CSV/TXT). These pandas-style options are translated for you:

| You write | Polars option |
|---|---|
| `sep` / `delimiter` | `separator` |
| `skiprows` | `skip_rows` (`0` works) |
| `usecols` (list or `"a,b"`) | column selection |
| `dtype` / `dtypes` | `schema_overrides` |
| `encoding` | UTF-8 is read by Polars; other encodings (e.g. `cp874`) automatically use the pandas reader |

Other options are ignored with a note. Give each option only once
(`sep` *or* `delimiter`). Parquet and Excel files use the normal readers.

#### save_df() — from DataFrames

```python
store.save_df(name, df, chunk_size=100_000, compression="snappy", mode="error")
```

Accepts `pd.DataFrame`, `pl.DataFrame` or `pl.LazyFrame`.

```python
lf = DB2Connect().query("SELECT * FROM SCHEMA.REGIONS", engine="polars")
store.save_df("regions", lf)
store.save_df("data", df_new, mode="append")
```

#### save_remote() — from SFTP

```python
store.save_remote(name, loader, remote_path, file_type=None, chunk_size=100_000,
                  compression="snappy", mode="error", **read_kwargs)
```

Downloads the file with an [`SFTPDataLoader`](#8-sftp--sftpdataloader) to a
temporary folder, streams it like `save_file()`, then deletes the download.

```python
loader = SFTPDataLoader(hostname="sftp.example.com", username="user", pkey="/key")
store.save_remote("customers", loader, "/data/customers.csv")
store.save_remote("sales", loader, "/data/sales.csv", sep="|", skiprows=1)
store.save_remote("logs", loader, "/data/access.log", file_type="csv", sep="\t", mode="append")
loader.close()
```

#### Save modes

| Mode | Behaviour |
|---|---|
| `"error"` | Raise `FileExistsError` if the dataset exists. **(default)** |
| `"replace"` | Write the new data first, then swap it in. If the write fails, the old data is kept. |
| `"append"` | Add new part files after the existing ones; the view updates automatically. |

```python
store.save_file("logs", "/data/jan.csv")
store.save_file("logs", "/data/feb.csv", mode="append")
store.save_file("logs", "/data/all.csv", mode="replace")
```

Part files with different schemas (e.g. a column added later, or
`DECIMAL(11,2)` vs `DECIMAL(12,2)`) are merged by column name.

#### Chunking and safety

| Source | How it is chunked | Fully streamed? |
|---|---|---|
| BigQuery | API pages | Yes |
| DB2 / Impala / DB-API | `cursor.fetchmany(chunk_size)` | Yes |
| CSV / TXT | `pd.read_csv(chunksize=)` | Yes |
| CSV / TXT, `use_polars=True` | Polars streaming batches | Yes |
| Parquet | `ParquetFile.iter_batches()` | Yes |
| Excel / ODS | read whole sheet, write in slices | Read: no |
| pandas / polars DataFrame | sliced to Arrow | Already in memory |
| polars LazyFrame | collected once, then sliced | Collect: no |
| SFTP file | downloaded, then as a local file | Download: whole file |

Every write goes to a hidden temporary folder next to the dataset and is only
moved into place when all chunks succeed. A dropped connection or error
mid-stream leaves no partial files and does not touch the existing dataset or
the catalog.

```
stream → .sales_tmp_xxxx/part-0000.parquet, part-0001.parquet, ...
       → success → move into sales/ → update catalog → register view
       → failure → delete temp folder → nothing else changes
```

### Querying data

```python
df  = store.query("SELECT * FROM sales WHERE year = 2024")                 # pd.DataFrame
lf  = store.query("SELECT * FROM sales", engine="polars")                  # pl.LazyFrame
rel = store.sql("SELECT * FROM sales LIMIT 10"); rel.show(); df = rel.df() # DuckDB relation

df = store.query("""
    SELECT s.date, s.amount, c.name, p.category
    FROM sales s
    JOIN customers c USING (cust_id)
    JOIN products p USING (product_id)
""")
```

`store.ddb` is the underlying DuckDB connection if you need it directly.

### Managing datasets

```python
store.list()                  # name, rows, chunks, source, chunk_size, compression, created_at, path
store.info("sales")           # catalog entry as a dict
store.drop("sales")           # remove from catalog + view; files stay on disk
store.drop("sales", files=True)   # ... and delete the Parquet files
store.clear_all()             # drop every dataset (files stay)
store.clear_all(files=True)   # drop every dataset and delete all files
store.reload("sales")         # re-register the view after adding part files by hand
```

The catalog records each dataset's folder **relative to `base_dir`**, so a
store can be moved, copied to another machine or opened from any working
directory. Catalogs written by older versions (paths relative to the old
working directory, Windows `\` separators) are still read.

### Jupyter magics

```python
store.enable_magic()          # once per notebook session
```

This registers `%%parqstore` and the shortcuts `%%save`, `%%query` and
`%%dql` (from magic-duckdb, extended). Each works as a cell magic (`%%`) and a
line magic (`%`) — use `%` when there is no cell body.

> `%save` replaces IPython's built-in `%save` command for the session.

```
%%parqstore --save <name> [options]   → same as %%save
%%parqstore --query [options]         → same as %%query
%parqstore  --list <loader> [...]     → browse SFTP (below)
```

#### Query flags (`%%query`, `%%dql`, `%%parqstore --query`)

| Flag | Effect |
|---|---|
| `--var <name>` | Store the result in a notebook variable |
| `--polars` | Result is a `pl.LazyFrame` instead of `pd.DataFrame` |
| `--dql` | Render with magic-duckdb's table view |
| `--create <name>` | Create a DuckDB **in-memory** table (gone after restart) |
| `--persist <name>` | Save the result as a **Parquet dataset** + view |
| `--mode`, `--chunk-size`, `--compression` | Options for `--persist` |

`%%dql` with no flags is plain magic-duckdb; with any flag it behaves like
`%%query --dql`.

```
%%query --var df_summary
SELECT region, SUM(amount) AS total FROM sales GROUP BY region
```

```
%%query --var lf_sales --polars
SELECT * FROM sales WHERE amount > 1000
```

```
%%dql --create sales_2024
SELECT * FROM sales WHERE year = 2024
```

```
%%query --persist monthly_agg --mode replace
SELECT date_trunc('month', sale_date) AS month, SUM(amount) AS total FROM sales GROUP BY 1
```

| | `--create` | `--persist` |
|---|---|---|
| Storage | DuckDB memory | Parquet on disk |
| Survives restart | No | Yes |
| In `store.list()` | No | Yes |

#### %%save — SQL mode (default)

```
%%save <name> [--conn <name_or_var>] [--mode M] [--chunk-size N] [--compression C]
SELECT ...
```

`--conn` is looked up as: built-in `db2` / `impala` → `connections.yaml` name →
notebook variable. Without `--conn` the SQL runs on the store's DuckDB
(i.e. over existing datasets).

```
%%save orders --conn db2
SELECT * FROM SCHEMA.ORDERS
```

#### %save --df — DataFrame mode

```
%save regions --df lf_regions --mode replace
```

#### %%save --file — local file mode

```
%%save <name> --file [--file-type TYPE] [--polars] [shared options]
/path/or/glob/*.csv
key=value          ← reader options, one per line ('#' lines ignored)
```

```
%%save transactions --file --mode replace
/data/transactions.csv
sep=|
skiprows=1
usecols=date,amount,account_id
encoding=utf-8
```

```
%%save products --file --file-type xlsx
/data/catalog.xlsx
sheet_name=Sheet2
```

#### %%save --remote — SFTP mode

```
%%save <name> --remote <loader_var> [--file-type TYPE] [shared options]
/remote/path/file.csv
sep=|
```

#### %parqstore --list — browse SFTP

```
%parqstore --list <loader_var> [remote_dir] [pattern] [-r] [--max-depth N]
                  [--types csv,parquet | --all-files] [--sort modified|name|size|path]
                  [--limit N] [--var NAME]
```

```
%parqstore --list my_loader /data/exports
%parqstore --list my_loader /data "*sales*" -r --sort name --var files
%parqstore --list my_loader /data --types parquet --sort size --limit 20
```

Shows up to `--limit` rows (default 200). Quote patterns and paths that
contain spaces or wildcards. `--var` also stores the DataFrame.

#### Reader option values in magics

One `key=value` per line after the path; blank lines and `#` lines are ignored.

| Written | Becomes |
|---|---|
| `sep`, `delimiter`, `encoding`, `quotechar`, `decimal`, `thousands`, `comment`, … | always text: `sep=,` → `","`, `sep=;` → `";"` |
| `\t`, `\n`, `\r` (for the text options above) | tab / newline / carriage return: `sep=\t` |
| a value in quotes | taken literally: `sep=" "` → a space, `na_values="N/A, -"` → that string |
| digits | `int` (`skiprows=2`) |
| `true` / `false` | `bool` |
| `none` | `None` |
| contains a comma | list of strings (`usecols=A,B,C`) |
| anything else | string |

Options that need Python objects (e.g. `dtype={...}`) must use the Python API.

### File layout

```
<base_dir>/
├── .catalog.json          ← registry of all datasets
├── sales/
│   ├── part-0000.parquet
│   └── part-0001.parquet
└── customers/
    └── part-0000.parquet
```

```json
{
  "sales": {
    "path": "sales",
    "query": "SELECT * FROM db.sales",
    "rows": 350000,
    "chunks": 4,
    "source": "impala",
    "compression": "snappy",
    "chunk_size": 100000,
    "created_at": "2026-11-19T10:30:00"
  }
}
```

Keep `connections.yaml` out of version control (the
project's `.gitignore` already excludes them).

---

## 10. Error handling and console output

- **Errors are raised.** Database queries, tunnels and SFTP connections raise
  on failure instead of printing and returning `None`. Wrap calls in
  `try/except` where you want to continue.
- **Progress messages** are printed (`Saving 'sales'…`, `✓ 'sales' ready …`).
  On consoles that cannot show `✓` or `→` — such as the Thai Windows code
  page cp874 — they are printed as `OK` and `->` instead of crashing.
  Jupyter shows the symbols normally.

---

## 11. API reference

### datasuite

| Function / class | Description |
|---|---|
| `FrameworkConfig.dataframe_engine` | Global default `"pandas"` / `"polars"` |
| `DB2Connect(**overrides).query(sql, engine=None)` | Query DB2 |
| `ImpalaConnect(**overrides).query(sql, engine=None)` | Query Impala/Hive |
| `open_db2_connection(dbc=None)` / `open_impala_connection(dbc=None)` | Raw `(connection, tunnel)` |
| `SSHTunnelConnect(...).connect() / .close() / .port_selection()` | SSH port forwarding |
| `read_local(path_or_buffer, file_type=None, engine=None, **kw)` | Read a file |
| `local_query(sql, engine=None, **tables)` | DuckDB SQL over in-memory frames |
| `export_excel(filepath, sheet_df, pretty_format=True)` | Write an Excel workbook |
| `SFTPDataLoader(...)` | SFTP client: `connect`, `list_remote`, `read_remote`, `close` |

### DataStore

| Method | Description |
|---|---|
| `DataStore(base_dir, connections_file=None)` | Open or create a store |
| `connect(name)` / `disconnect(name=None)` / `connections()` | Database connections and tunnels |
| `save(name, query, conn, ...)` | SQL → Parquet |
| `save_file(name, file_path, ...)` | File(s) → Parquet |
| `save_df(name, df, ...)` | DataFrame → Parquet |
| `save_remote(name, loader, remote_path, ...)` | SFTP file → Parquet |
| `query(sql, engine=None)` | SQL → DataFrame / LazyFrame |
| `sql(sql)` | SQL → DuckDB relation |
| `list()` / `info(name)` | Catalog |
| `drop(name, files=False)` / `clear_all(files=False)` / `reload(name)` | Manage datasets |
| `enable_magic()` | Register Jupyter magics |

---

## 12. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `TypeError: DB2Connect() got an unexpected keyword argument` | Misspelt override — see the keyword table in [section 3](#3-database-queries--db2connect--impalaconnect). |
| `ConnectionError: ssh tunnel exited with code 255: ...` | ssh could not log in to the bastion — check `BASTION_*`, key path and permissions. The text after the colon is ssh's own message. |
| `TimeoutError: ssh tunnel ... did not open local port` | Bastion slow or blocked, or ssh waiting for a key passphrase. Use a key without a passphrase (or an ssh agent), or raise `connect_timeout`. |
| `RuntimeError: No free local port in range 8801-8899` | Too many open tunnels — close old ones (`store.disconnect()`, restart kernels). |
| Env-var change has no effect | Variables are read at import — restart the kernel, or pass overrides to the constructor. |
| `FileExistsError: Dataset 'x' already exists` | Use `mode="replace"` or `mode="append"`. |
| `ValueError: Invalid dataset name` | Remove `/ \ :`, a leading `.`, or surrounding spaces from the name. |
| SQL error on a dataset name with `-` or a space | Double-quote it: `SELECT * FROM "my-data"`. |
| `ValueError: Cannot infer the file type` (SFTP) | The remote path has no extension — pass `file_type="csv"` etc. |
| Connections tunnel on a machine that should connect directly | Add its hostname to `DATASUITE_DIRECT_HOSTS`. |
| `%save` no longer saves notebook code | `enable_magic()` replaces IPython's `%save` for the session. |

# DataSuite

A Python toolkit for data work: DB2 / Impala queries over SSH tunnels, SFTP
file browsing and loading, local file reading, in-memory SQL with DuckDB,
Excel export, and a persistent Parquet data store with Jupyter magics.

## Installation

Requires Python 3.12+.

```bash
pip install -e .
# exact tested dependency versions:
pip install -e ".[dev]" -c constraints-py312.txt
```

```python
from datasuite import (DB2Connect, ImpalaConnect, SSHTunnelConnect, SFTPDataLoader,
                       read_local, local_query, export_excel, FrameworkConfig)
from datasuite.parquet_store import DataStore
```

## Documentation

See **[docs/manual.md](docs/manual.md)** for the full manual: configuration,
database queries, SSH tunnels, file reading, SQL on DataFrames, Excel export,
SFTP, the DataStore, Jupyter magics and troubleshooting.

**[examples/datasuite_tour.ipynb](examples/datasuite_tour.ipynb)** walks through every feature.
It runs end to end without credentials: SFTP is demonstrated against a local folder, and the
parts that need a database or bastion are switched on with `LIVE_*` flags.

## Tests

```bash
python -m pytest tests -q
```

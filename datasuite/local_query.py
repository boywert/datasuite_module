# local_query — run SQL against in-memory DataFrames via DuckDB
#
# The key trick is caller-scope discovery: local_query() uses
# ``inspect.currentframe().f_back`` to walk into the caller's local
# variables and register every pd.DataFrame / pl.DataFrame / pl.LazyFrame
# it finds as a DuckDB table — so users can write SQL using their variable
# names directly without any manual registration step.
#
# Extra DataFrames can also be passed as **kwargs for cases where the
# data isn't in the caller's locals (e.g. returned from a function).
#
# Each call creates a fresh in-memory DuckDB connection to avoid
# state leaking between queries.

import duckdb
import pandas as pd
import polars as pl
import inspect
from typing import Optional, Union

from .framework_config import _resolve_engine
from .helpers import duckdb_to_arrow


def local_query(sql: str, engine: Optional[str] = None, **kwargs) -> Union[pd.DataFrame, pl.LazyFrame]:
    """
    Run a SQL query against in-memory DataFrames using DuckDB.

    Automatically discovers all pd.DataFrame, pl.DataFrame, and pl.LazyFrame
    variables in the caller's local scope and makes them available as SQL tables
    (using the variable name as the table name). Extra DataFrames can also be
    passed explicitly as keyword arguments.

    Parameters
    ----------
    sql : str
        SQL query to execute. Table names correspond to variable names in the
        caller's scope (or kwargs keys).
    engine : str, optional
        "pandas" → returns pd.DataFrame
        "polars" → returns pl.LazyFrame
        Defaults to FrameworkConfig.dataframe_engine when not provided.
    **kwargs
        Additional DataFrames to register. Key = table name, value = DataFrame.

    Returns
    -------
    pd.DataFrame or pl.LazyFrame

    Example
    -------
    df_sales = pd.read_csv("sales.csv")
    result = local_query("SELECT region, SUM(amount) FROM df_sales GROUP BY region")
    """

    # --- 1. Create a fresh in-memory DuckDB connection for this query ---
    con = duckdb.connect()

    # --- 2. Walk the caller's local scope and register every DataFrame found ---
    # inspect.currentframe().f_back gives us the frame of whoever called local_query(),
    # so we can see all their local variables without them having to pass anything explicitly.
    caller_frame = inspect.currentframe().f_back

    for name, value in caller_frame.f_locals.items():
        if isinstance(value, pd.DataFrame):
            con.register(name, value)
        elif isinstance(value, pl.LazyFrame):
            # DuckDB cannot scan a LazyFrame directly — materialise it first.
            # This is the only point where a Polars LazyFrame must be collected
            # before query execution.
            con.register(name, value.collect())
        elif isinstance(value, pl.DataFrame):
            # pl.DataFrame is accepted by DuckDB natively (zero-copy via Arrow)
            con.register(name, value)

    # --- 3. Register any extra DataFrames passed as keyword arguments ---
    for name, value in kwargs.items():
        if isinstance(value, pl.LazyFrame):
            con.register(name, value.collect())
        else:
            con.register(name, value)

    # --- 4. Execute the SQL and return in the requested format ---
    resolved = _resolve_engine(engine)
    if resolved == 'polars':
        # Fetch the DuckDB result as Apache Arrow (zero copy), then pl.from_arrow()
        # wraps it as a Polars DataFrame before making it lazy.
        # This avoids an unnecessary pandas round-trip.
        return pl.from_arrow(duckdb_to_arrow(con, sql)).lazy()

    # Default: return pandas DataFrame
    return con.execute(sql).df()

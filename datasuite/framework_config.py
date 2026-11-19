# framework_config — global configuration for datasuite
#
# This module defines the library-wide defaults that every reader / query
# function consults.  Users set them once at the top of their script:
#
#     from datasuite import FrameworkConfig
#     FrameworkConfig.dataframe_engine = "polars"
#
# Individual function calls can still override via their ``engine=`` kwarg.
# The resolution logic lives in ``_resolve_engine()`` which is imported by
# every module that returns a DataFrame.

from typing import Literal, Optional

# Allowed values for the dataframe engine option
EngineType = Literal["pandas", "polars"]


class FrameworkConfig:
    """
    Global configuration for datasuite.

    Change these class attributes once at the top of your script/notebook to
    apply a default across all functions in the library.

    Example
    -------
    from datasuite import FrameworkConfig
    FrameworkConfig.dataframe_engine = "polars"   # all functions return pl.LazyFrame by default

    Attributes
    ----------
    sqlengine : str
        Backend used by local_query(). Currently only "duckdb" is supported.
    dataframe_engine : str
        Default return type for all reader/query functions.
        "pandas" → pd.DataFrame  (default)
        "polars" → pl.LazyFrame
    """
    sqlengine: str = 'duckdb'
    dataframe_engine: EngineType = 'pandas'


def _resolve_engine(per_call_engine: Optional[str]) -> EngineType:
    """
    Determine the effective engine for a single function call.

    Priority: per-call argument > FrameworkConfig.dataframe_engine global default.
    Passing None means "use whatever the global says".

    Parameters
    ----------
    per_call_engine : str or None
        Value passed by the caller via the `engine=` argument. None = use global.

    Returns
    -------
    "pandas" or "polars"
    """
    engine = per_call_engine if per_call_engine is not None else FrameworkConfig.dataframe_engine
    if engine not in ('pandas', 'polars'):
        raise ValueError(f"engine must be 'pandas' or 'polars', got {engine!r}")
    return engine

# read_local — read local files into DataFrames
#
# Unified reader for CSV, TXT, Excel (.xlsx/.xls/.xlsm/.ods), and Parquet.
# Supports both pandas and polars engines.  Also accepts file-like objects
# (e.g. a paramiko SFTP handle from SFTPDataLoader) — pass ``file_type``
# explicitly in that case since the extension can't be inferred.
#
# This module is also used internally by parquet_store.save_file() to
# read files before converting them to chunked Parquet.

import io
import pandas as pd
import polars as pl
import pathlib
from typing import Union, IO, Optional

from .framework_config import _resolve_engine


def read_local(
    filepath_or_buffer: Union[str, pathlib.Path, IO],
    file_type: Optional[str] = None,
    engine: Optional[str] = None,
    **kwargs
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """
    Read a local file into a DataFrame.

    Supports CSV, TXT, Excel (xlsx/xls/xlsm/ods), and Parquet.
    Also accepts file-like objects (e.g. a paramiko SFTP file handle) — in that
    case you must provide `file_type` explicitly since the extension cannot be
    inferred from the object.

    Parameters
    ----------
    filepath_or_buffer : str | Path | file-like
        Path to the file, or an open file-like object.
    file_type : str, optional
        Force a specific format, e.g. "csv", "parquet". Inferred from the file
        extension when omitted.
    engine : str, optional
        "pandas" → returns pd.DataFrame
        "polars" → returns pl.LazyFrame  (reads lazily where the format allows)
        Defaults to FrameworkConfig.dataframe_engine when not provided.
    **kwargs
        Forwarded directly to the underlying reader (pd.read_csv, pl.scan_csv, etc.).

    Returns
    -------
    pd.DataFrame or pl.LazyFrame
    """

    # --- 1. Resolve the file extension ---
    if file_type is None:
        if isinstance(filepath_or_buffer, (str, pathlib.Path)):
            ext = pathlib.Path(filepath_or_buffer).suffix.lower()
        else:
            # File-like objects have no name we can inspect — caller must tell us
            raise ValueError("Cannot detect file type from a buffer. Please specify 'file_type'.")
    else:
        # Normalise "csv" → ".csv", ".CSV" → ".csv", etc.
        ext = f".{file_type.lower().strip('.')}"

    resolved = _resolve_engine(engine)

    # --- 2. Dispatch to the correct reader ---
    try:
        if resolved == 'pandas':
            # Pandas readers accept both file paths and file-like objects natively
            if ext in ('.csv', '.txt'):
                return pd.read_csv(filepath_or_buffer, **kwargs)
            elif ext in ('.xlsx', '.xls', '.xlsm', '.odf', '.ods'):
                return pd.read_excel(filepath_or_buffer, **kwargs)
            elif ext in ('.parquet', '.pqt'):
                return pd.read_parquet(filepath_or_buffer, **kwargs)
            else:
                raise ValueError(f"Unsupported file extension: {ext}")

        else:  # polars
            # pl.scan_* only accepts file paths or bytes — not arbitrary file handles.
            # When the source is a file-like object (e.g. SFTP handle), read all
            # bytes into a BytesIO so Polars can work with it.
            # For plain file paths, we pass them through untouched so Polars can
            # stream from disk without loading everything into memory upfront.
            source = filepath_or_buffer
            if not isinstance(source, (str, pathlib.Path)):
                source = io.BytesIO(source.read())

            if ext in ('.csv', '.txt'):
                # scan_csv is lazy — rows are only parsed on .collect()
                return pl.scan_csv(source, **kwargs)
            elif ext in ('.parquet', '.pqt'):
                # scan_parquet is lazy and supports column/row pruning pushed down
                # to the file reader (best format for large datasets with Polars)
                return pl.scan_parquet(source, **kwargs)
            elif ext in ('.xlsx', '.xls', '.xlsm', '.odf', '.ods'):
                # Excel has no lazy reader; read_excel is eager.
                # .lazy() wraps it so callers can chain Polars expressions uniformly.
                # Note: requires 'fastexcel' package (pip install fastexcel)
                return pl.read_excel(source, **kwargs).lazy()
            else:
                raise ValueError(f"Unsupported file extension: {ext}")

    except Exception as e:
        print(f"Error reading file: {e}")
        raise

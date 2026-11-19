# export_excel — multi-sheet Excel export with formatting
#
# Writes one or more DataFrames (pandas, polars, or polars LazyFrame) to a
# single .xlsx workbook using xlsxwriter.  Each dict in the ``sheet_df`` list
# becomes one sheet.
#
# When ``pretty_format=True`` (default), each sheet gets:
#   - Autofilter on the header row
#   - Frozen top row (stays visible when scrolling)
#   - Auto-width columns sized to fit the widest value
#   - Comma-formatted float columns (#,##0.00)
#
# Polars objects are converted to pandas internally because xlsxwriter
# has no native Polars support.

import pandas as pd
import polars as pl


def export_excel(filepath, sheet_df, pretty_format=True):
    """
    Write one or more DataFrames to an Excel workbook, one sheet per DataFrame.

    Accepts pd.DataFrame, pl.DataFrame, or pl.LazyFrame in the `dataframe` field
    of each sheet dict — Polars objects are converted to pandas internally since
    xlsxwriter has no native Polars support.

    Parameters
    ----------
    filepath : str
        Output .xlsx file path.
    sheet_df : list[dict]
        Each dict must have:
            "sheetname"  : str          — Excel sheet name
            "dataframe"  : pd.DataFrame | pl.DataFrame | pl.LazyFrame
    pretty_format : bool
        When True (default), applies autofilter, frozen header row, and
        auto-width columns with comma formatting for float columns.

    Example
    -------
    export_excel("report.xlsx", [
        {"sheetname": "Sales",    "dataframe": df_sales},
        {"sheetname": "Summary",  "dataframe": lf_summary},   # pl.LazyFrame is fine
    ])
    """

    def _to_pandas(obj):
        """Normalise any supported DataFrame type to pd.DataFrame."""
        if isinstance(obj, pl.LazyFrame):
            return obj.collect().to_pandas()
        if isinstance(obj, pl.DataFrame):
            return obj.to_pandas()
        return obj  # already pandas

    # Convert each sheet once (a LazyFrame would otherwise be collected twice).
    sheets = [(sheet['sheetname'], _to_pandas(sheet['dataframe'])) for sheet in sheet_df]

    with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:

        # --- Pass 1: write data to each sheet ---
        for sheetname, df in sheets:
            df.to_excel(writer, sheet_name=sheetname, index=False)

        if not pretty_format:
            return

        # --- Pass 2: apply formatting (requires the workbook to exist first) ---
        workbook = writer.book
        comma_format = workbook.add_format({'num_format': '#,##0.00'})

        for sheetname, df in sheets:
            ws = writer.sheets[sheetname]
            if len(df.columns) == 0:
                continue  # nothing to filter or size

            # Autofilter spans the full data range (header row + data rows)
            ws.autofilter(0, 0, len(df), len(df.columns) - 1)

            # Freeze the header row so it stays visible when scrolling
            ws.freeze_panes(1, 0)

            # Set each column width to fit its widest value, plus padding
            float_cols = set(df.select_dtypes(include=['float']).columns)
            for i, col in enumerate(df.columns):
                values = df.iloc[:, i]
                widest = int(values.astype(str).str.len().max()) if len(values) else 0
                max_len = max(widest, len(str(col))) + 2
                if col in float_cols:
                    ws.set_column(i, i, max_len, cell_format=comma_format)
                else:
                    ws.set_column(i, i, max_len)

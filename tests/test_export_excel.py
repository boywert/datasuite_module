"""Tests for export_excel.py — multi-sheet Excel export."""

import pandas as pd
import polars as pl
import pytest

from datasuite.export_excel import export_excel


@pytest.fixture
def sample_df():
    return pd.DataFrame({"name": ["a", "b"], "value": [1.5, 2.5], "count": [10, 20]})


class TestExportExcel:
    def test_single_sheet(self, tmp_path, sample_df):
        path = str(tmp_path / "out.xlsx")
        export_excel(path, [{"sheetname": "Sheet1", "dataframe": sample_df}])
        result = pd.read_excel(path, sheet_name="Sheet1")
        assert list(result.columns) == ["name", "value", "count"]
        assert len(result) == 2

    def test_multiple_sheets(self, tmp_path, sample_df):
        path = str(tmp_path / "out.xlsx")
        export_excel(path, [
            {"sheetname": "First", "dataframe": sample_df},
            {"sheetname": "Second", "dataframe": sample_df.head(1)},
        ])
        sheets = pd.read_excel(path, sheet_name=None)
        assert set(sheets.keys()) == {"First", "Second"}
        assert len(sheets["First"]) == 2
        assert len(sheets["Second"]) == 1

    def test_no_pretty_format(self, tmp_path, sample_df):
        path = str(tmp_path / "out.xlsx")
        export_excel(path, [{"sheetname": "S", "dataframe": sample_df}], pretty_format=False)
        result = pd.read_excel(path, sheet_name="S")
        assert len(result) == 2

    def test_polars_dataframe(self, tmp_path):
        pl_df = pl.DataFrame({"x": [1, 2], "y": [3.0, 4.0]})
        path = str(tmp_path / "out.xlsx")
        export_excel(path, [{"sheetname": "PL", "dataframe": pl_df}])
        result = pd.read_excel(path, sheet_name="PL")
        assert list(result.columns) == ["x", "y"]
        assert len(result) == 2

    def test_polars_lazyframe(self, tmp_path):
        lf = pl.LazyFrame({"x": [10, 20], "y": [30.0, 40.0]})
        path = str(tmp_path / "out.xlsx")
        export_excel(path, [{"sheetname": "LF", "dataframe": lf}])
        result = pd.read_excel(path, sheet_name="LF")
        assert len(result) == 2

    def test_data_integrity(self, tmp_path, sample_df):
        path = str(tmp_path / "out.xlsx")
        export_excel(path, [{"sheetname": "S", "dataframe": sample_df}])
        result = pd.read_excel(path, sheet_name="S")
        assert result["name"].tolist() == ["a", "b"]
        assert result["count"].tolist() == [10, 20]

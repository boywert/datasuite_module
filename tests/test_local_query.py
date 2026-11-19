"""Tests for local_query.py — DuckDB in-memory SQL with caller scope discovery."""

import pandas as pd
import polars as pl
import pytest

from datasuite.local_query import local_query


class TestLocalQuery:
    def test_basic_select(self):
        df_test = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = local_query("SELECT * FROM df_test", engine="pandas")
        assert len(result) == 3
        assert list(result.columns) == ["a", "b"]

    def test_aggregation(self):
        df_sales = pd.DataFrame({"region": ["A", "B", "A"], "amount": [10, 20, 30]})
        result = local_query(
            "SELECT region, SUM(amount) as total FROM df_sales GROUP BY region ORDER BY region",
            engine="pandas",
        )
        assert result["total"].tolist() == [40, 20]

    def test_where_clause(self):
        df_data = pd.DataFrame({"x": [1, 2, 3, 4, 5]})
        result = local_query("SELECT x FROM df_data WHERE x > 3", engine="pandas")
        assert result["x"].tolist() == [4, 5]

    def test_join(self):
        df_a = pd.DataFrame({"id": [1, 2], "name": ["foo", "bar"]})
        df_b = pd.DataFrame({"id": [1, 2], "score": [100, 200]})
        result = local_query(
            "SELECT a.name, b.score FROM df_a a JOIN df_b b ON a.id = b.id ORDER BY a.id",
            engine="pandas",
        )
        assert result["name"].tolist() == ["foo", "bar"]
        assert result["score"].tolist() == [100, 200]

    def test_kwargs_dataframes(self):
        extra = pd.DataFrame({"val": [10, 20, 30]})
        result = local_query("SELECT SUM(val) as s FROM extra", engine="pandas", extra=extra)
        assert result["s"].iloc[0] == 60

    def test_returns_polars_lazyframe(self):
        df_items = pd.DataFrame({"item": ["a", "b"]})
        result = local_query("SELECT * FROM df_items", engine="polars")
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().shape == (2, 1)

    def test_polars_input_dataframe(self):
        pl_table = pl.DataFrame({"x": [1, 2, 3]})
        result = local_query("SELECT SUM(x) as total FROM pl_table", engine="pandas")
        assert result["total"].iloc[0] == 6

    def test_polars_input_lazyframe(self):
        pl_lazy = pl.LazyFrame({"x": [10, 20]})
        result = local_query("SELECT SUM(x) as total FROM pl_lazy", engine="pandas")
        assert result["total"].iloc[0] == 30

    def test_empty_dataframe(self):
        df_empty = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        result = local_query("SELECT COUNT(*) as n FROM df_empty", engine="pandas")
        assert result["n"].iloc[0] == 0

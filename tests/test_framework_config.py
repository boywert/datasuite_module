"""Tests for framework_config.py — FrameworkConfig and _resolve_engine."""

import pytest
from datasuite.framework_config import FrameworkConfig, _resolve_engine


class TestResolveEngine:
    def test_none_returns_global_default(self):
        original = FrameworkConfig.dataframe_engine
        try:
            FrameworkConfig.dataframe_engine = "pandas"
            assert _resolve_engine(None) == "pandas"
            FrameworkConfig.dataframe_engine = "polars"
            assert _resolve_engine(None) == "polars"
        finally:
            FrameworkConfig.dataframe_engine = original

    def test_explicit_overrides_global(self):
        original = FrameworkConfig.dataframe_engine
        try:
            FrameworkConfig.dataframe_engine = "pandas"
            assert _resolve_engine("polars") == "polars"
            FrameworkConfig.dataframe_engine = "polars"
            assert _resolve_engine("pandas") == "pandas"
        finally:
            FrameworkConfig.dataframe_engine = original

    def test_invalid_engine_raises(self):
        with pytest.raises(ValueError, match="engine must be"):
            _resolve_engine("spark")

    def test_pandas_and_polars_accepted(self):
        assert _resolve_engine("pandas") == "pandas"
        assert _resolve_engine("polars") == "polars"

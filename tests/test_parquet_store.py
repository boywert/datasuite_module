"""Tests for parquet_store.py — DataStore, streaming, chunking, file import."""

import json
import os
import textwrap

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from datasuite.parquet_store import (
    DataStore,
    _expand_env_recursive,
    _next_part_number,
    _parse_magic_value,
    _resolve_ext,
    _stream_file,
    _write_from_iter,
)


# ─── helpers / internals ─────────────────────────────────────────────────────

class TestExpandEnvRecursive:
    def test_string_expansion(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "hello")
        assert _expand_env_recursive("${MY_VAR}") == "hello"

    def test_missing_var_becomes_empty(self, monkeypatch):
        monkeypatch.delenv("MISSING_XYZ", raising=False)
        assert _expand_env_recursive("${MISSING_XYZ}") == ""

    def test_dict_expansion(self, monkeypatch):
        monkeypatch.setenv("HOST", "myhost")
        result = _expand_env_recursive({"host": "${HOST}", "port": 22})
        assert result == {"host": "myhost", "port": 22}

    def test_nested_dict(self, monkeypatch):
        monkeypatch.setenv("USER", "admin")
        result = _expand_env_recursive({"ssh": {"user": "${USER}"}})
        assert result == {"ssh": {"user": "admin"}}

    def test_list_expansion(self, monkeypatch):
        monkeypatch.setenv("A", "x")
        result = _expand_env_recursive(["${A}", "literal"])
        assert result == ["x", "literal"]

    def test_non_string_passthrough(self):
        assert _expand_env_recursive(42) == 42
        assert _expand_env_recursive(None) is None
        assert _expand_env_recursive(True) is True


class TestParseMagicValue:
    def test_int(self):
        assert _parse_magic_value("42") == 42

    def test_bool_true(self):
        assert _parse_magic_value("true") is True
        assert _parse_magic_value("True") is True

    def test_bool_false(self):
        assert _parse_magic_value("false") is False

    def test_none(self):
        assert _parse_magic_value("none") is None
        assert _parse_magic_value("None") is None

    def test_list(self):
        assert _parse_magic_value("A,B,C") == ["A", "B", "C"]

    def test_list_with_spaces(self):
        assert _parse_magic_value("A, B, C") == ["A", "B", "C"]

    def test_string(self):
        assert _parse_magic_value("|") == "|"
        assert _parse_magic_value("utf-8") == "utf-8"


class TestResolveExt:
    def test_from_path(self):
        assert _resolve_ext("/data/file.csv", None) == ".csv"
        assert _resolve_ext("/data/file.XLSX", None) == ".xlsx"

    def test_explicit_type(self):
        assert _resolve_ext("/data/file.dat", "csv") == ".csv"
        assert _resolve_ext("/data/file.dat", ".CSV") == ".csv"


class TestNextPartNumber:
    def test_empty_dir(self, tmp_path):
        assert _next_part_number(tmp_path) == 0

    def test_existing_parts(self, tmp_path):
        (tmp_path / "part-0000.parquet").touch()
        (tmp_path / "part-0001.parquet").touch()
        (tmp_path / "part-0002.parquet").touch()
        assert _next_part_number(tmp_path) == 3

    def test_gap_uses_last(self, tmp_path):
        (tmp_path / "part-0000.parquet").touch()
        (tmp_path / "part-0005.parquet").touch()
        assert _next_part_number(tmp_path) == 6


# ─── _write_from_iter ────────────────────────────────────────────────────────

class TestWriteFromIter:
    def test_writes_parquet_files(self, tmp_path):
        tables = [
            pa.table({"x": [1, 2, 3]}),
            pa.table({"x": [4, 5]}),
        ]
        rows, chunks = _write_from_iter(tables, tmp_path, "snappy")
        assert rows == 5
        assert chunks == 2
        assert (tmp_path / "part-0000.parquet").exists()
        assert (tmp_path / "part-0001.parquet").exists()

    def test_start_part_offset(self, tmp_path):
        tables = [pa.table({"x": [1]})]
        rows, chunks = _write_from_iter(tables, tmp_path, "snappy", start_part=5)
        assert chunks == 1
        assert (tmp_path / "part-0005.parquet").exists()

    def test_empty_iterator(self, tmp_path):
        rows, chunks = _write_from_iter(iter([]), tmp_path, "snappy")
        assert rows == 0
        assert chunks == 0

    def test_data_integrity(self, tmp_path):
        df = pd.DataFrame({"a": [10, 20], "b": ["x", "y"]})
        tables = [pa.Table.from_pandas(df, preserve_index=False)]
        _write_from_iter(tables, tmp_path, "snappy")
        result = pq.read_table(tmp_path / "part-0000.parquet").to_pandas()
        pd.testing.assert_frame_equal(result, df)


# ─── _stream_file ────────────────────────────────────────────────────────────

class TestStreamFile:
    def test_csv_chunked(self, tmp_path):
        p = tmp_path / "data.csv"
        df = pd.DataFrame({"x": range(100)})
        df.to_csv(p, index=False)
        chunks = list(_stream_file(str(p), None, chunk_size=30))
        assert len(chunks) == 4  # 30+30+30+10
        total = sum(len(c) for c in chunks)
        assert total == 100

    def test_csv_with_sep(self, tmp_path):
        p = tmp_path / "data.csv"
        p.write_text("a|b\n1|2\n3|4\n")
        chunks = list(_stream_file(str(p), None, chunk_size=100, sep="|"))
        assert len(chunks) == 1
        assert chunks[0].num_rows == 2
        assert chunks[0].column_names == ["a", "b"]

    def test_parquet_chunked(self, tmp_path):
        p = tmp_path / "data.parquet"
        df = pd.DataFrame({"x": range(100)})
        df.to_parquet(p, index=False)
        chunks = list(_stream_file(str(p), None, chunk_size=30))
        total = sum(len(c) for c in chunks)
        assert total == 100

    def test_excel_chunked(self, tmp_path):
        p = tmp_path / "data.xlsx"
        df = pd.DataFrame({"x": range(50)})
        df.to_excel(p, index=False)
        chunks = list(_stream_file(str(p), None, chunk_size=20))
        total = sum(len(c) for c in chunks)
        assert total == 50
        assert len(chunks) == 3  # 20+20+10

    def test_forced_file_type(self, tmp_path):
        p = tmp_path / "data.dat"
        pd.DataFrame({"v": [1, 2]}).to_csv(p, index=False)
        chunks = list(_stream_file(str(p), "csv", chunk_size=100))
        assert sum(len(c) for c in chunks) == 2

    def test_unsupported_ext_raises(self, tmp_path):
        p = tmp_path / "data.json"
        p.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            list(_stream_file(str(p), None, chunk_size=100))

    def test_yields_pyarrow_tables(self, tmp_path):
        p = tmp_path / "data.csv"
        pd.DataFrame({"a": [1]}).to_csv(p, index=False)
        for chunk in _stream_file(str(p), None, chunk_size=100):
            assert isinstance(chunk, pa.Table)


# ─── DataStore ───────────────────────────────────────────────────────────────

class TestDataStoreInit:
    def test_creates_base_dir(self, tmp_path):
        store_dir = tmp_path / "new_store"
        store = DataStore(str(store_dir))
        assert store_dir.exists()
        assert (store_dir / ".catalog.json").exists() is False  # no datasets yet

    def test_loads_existing_catalog(self, tmp_path):
        catalog = {"test": {"path": str(tmp_path / "test"), "rows": 10,
                            "chunks": 1, "source": "file", "compression": "snappy",
                            "chunk_size": 100000,
                            "created_at": "2024-01-01T00:00:00"}}
        (tmp_path / ".catalog.json").write_text(json.dumps(catalog))
        store = DataStore(str(tmp_path))
        assert "test" in store._catalog


class TestDataStoreSaveFile:
    def test_save_csv(self, tmp_path):
        csv_path = tmp_path / "input.csv"
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_csv(csv_path, index=False)

        store_dir = tmp_path / "store"
        store = DataStore(str(store_dir))
        store.save_file("test_csv", str(csv_path))

        assert "test_csv" in store._catalog
        assert store._catalog["test_csv"]["rows"] == 3
        assert store._catalog["test_csv"]["source"] == "file"
        # Can query via DuckDB
        result = store.query("SELECT * FROM test_csv ORDER BY a")
        assert len(result) == 3
        assert result["a"].tolist() == [1, 2, 3]

    def test_save_excel(self, tmp_path):
        xlsx_path = tmp_path / "input.xlsx"
        pd.DataFrame({"x": [10, 20]}).to_excel(xlsx_path, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("from_excel", str(xlsx_path))
        assert store._catalog["from_excel"]["rows"] == 2

    def test_save_parquet(self, tmp_path):
        pq_path = tmp_path / "input.parquet"
        pd.DataFrame({"v": range(50)}).to_parquet(pq_path, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("from_pq", str(pq_path))
        assert store._catalog["from_pq"]["rows"] == 50

    def test_chunking(self, tmp_path):
        csv_path = tmp_path / "big.csv"
        pd.DataFrame({"x": range(250)}).to_csv(csv_path, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("chunked", str(csv_path), chunk_size=100)
        assert store._catalog["chunked"]["rows"] == 250
        assert store._catalog["chunked"]["chunks"] == 3  # 100+100+50

    def test_csv_with_read_kwargs(self, tmp_path):
        csv_path = tmp_path / "pipe.csv"
        csv_path.write_text("a|b|c\n1|2|3\n4|5|6\n")

        store = DataStore(str(tmp_path / "store"))
        store.save_file("piped", str(csv_path), sep="|", usecols=["a", "b"])
        result = store.query("SELECT * FROM piped ORDER BY a")
        assert list(result.columns) == ["a", "b"]
        assert len(result) == 2

    def test_mode_error_on_duplicate(self, tmp_path):
        csv_path = tmp_path / "data.csv"
        pd.DataFrame({"a": [1]}).to_csv(csv_path, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("ds", str(csv_path))
        with pytest.raises(FileExistsError):
            store.save_file("ds", str(csv_path))

    def test_mode_replace(self, tmp_path):
        csv1 = tmp_path / "v1.csv"
        csv2 = tmp_path / "v2.csv"
        pd.DataFrame({"a": [1, 2]}).to_csv(csv1, index=False)
        pd.DataFrame({"a": [10, 20, 30]}).to_csv(csv2, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("ds", str(csv1))
        assert store._catalog["ds"]["rows"] == 2
        store.save_file("ds", str(csv2), mode="replace")
        assert store._catalog["ds"]["rows"] == 3

    def test_mode_append(self, tmp_path):
        csv1 = tmp_path / "part1.csv"
        csv2 = tmp_path / "part2.csv"
        pd.DataFrame({"a": [1, 2]}).to_csv(csv1, index=False)
        pd.DataFrame({"a": [3, 4, 5]}).to_csv(csv2, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("ds", str(csv1))
        store.save_file("ds", str(csv2), mode="append")
        assert store._catalog["ds"]["rows"] == 5
        result = store.query("SELECT COUNT(*) as n FROM ds")
        assert result["n"].iloc[0] == 5


class TestDataStoreSaveSQL:
    def test_save_from_duckdb(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        # DuckDB can generate data with SQL
        store.save("generated", "SELECT i as x, i*2 as y FROM range(100) t(i)",
                   conn=store.ddb, chunk_size=30)
        assert store._catalog["generated"]["rows"] == 100
        result = store.query("SELECT SUM(x) as s FROM generated")
        assert result["s"].iloc[0] == 4950  # sum(0..99)

    def test_sql_chunking(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        store.save("chunks", "SELECT i FROM range(250) t(i)",
                   conn=store.ddb, chunk_size=100)
        assert store._catalog["chunks"]["chunks"] == 3  # 100+100+50

    def test_sql_append(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        store.save("ds", "SELECT 1 as x", conn=store.ddb)
        store.save("ds", "SELECT 2 as x", conn=store.ddb, mode="append")
        result = store.query("SELECT COUNT(*) as n FROM ds")
        assert result["n"].iloc[0] == 2


class TestDataStoreManagement:
    def _make_store(self, tmp_path):
        csv = tmp_path / "data.csv"
        pd.DataFrame({"a": [1, 2, 3]}).to_csv(csv, index=False)
        store = DataStore(str(tmp_path / "store"))
        store.save_file("ds1", str(csv))
        store.save_file("ds2", str(csv))
        return store

    def test_list(self, tmp_path):
        store = self._make_store(tmp_path)
        listing = store.list()
        assert isinstance(listing, pd.DataFrame)
        assert set(listing["name"]) == {"ds1", "ds2"}
        assert all(listing["rows"] == 3)

    def test_info(self, tmp_path):
        store = self._make_store(tmp_path)
        info = store.info("ds1")
        assert info["rows"] == 3
        assert info["source"] == "file"
        assert "path" in info

    def test_info_missing_raises(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        with pytest.raises(KeyError):
            store.info("nonexistent")

    def test_drop_view_only(self, tmp_path):
        store = self._make_store(tmp_path)
        ds1_path = store._dataset_path("ds1")
        assert os.path.exists(ds1_path)
        store.drop("ds1")
        assert "ds1" not in store._catalog
        # Files still exist
        assert os.path.exists(ds1_path)

    def test_drop_with_files(self, tmp_path):
        store = self._make_store(tmp_path)
        ds1_path = store._dataset_path("ds1")
        assert os.path.exists(ds1_path)
        store.drop("ds1", files=True)
        assert "ds1" not in store._catalog
        assert not os.path.exists(ds1_path)

    def test_query_after_drop(self, tmp_path):
        store = self._make_store(tmp_path)
        store.drop("ds1")
        # ds2 should still work
        result = store.query("SELECT COUNT(*) as n FROM ds2")
        assert result["n"].iloc[0] == 3

    def test_reload(self, tmp_path):
        store = self._make_store(tmp_path)
        # Manually drop the view without removing catalog
        store.ddb.execute("DROP VIEW IF EXISTS ds1")
        # reload re-registers
        store.reload("ds1")
        result = store.query("SELECT COUNT(*) as n FROM ds1")
        assert result["n"].iloc[0] == 3

    def test_repr(self, tmp_path):
        store = self._make_store(tmp_path)
        r = repr(store)
        assert "DataStore" in r
        assert "ds1" in r

    def test_list_empty(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        listing = store.list()
        assert isinstance(listing, pd.DataFrame)
        assert len(listing) == 0


class TestDataStorePersistence:
    def test_catalog_survives_reload(self, tmp_path):
        csv = tmp_path / "data.csv"
        pd.DataFrame({"a": [1, 2]}).to_csv(csv, index=False)

        store_dir = tmp_path / "store"
        store1 = DataStore(str(store_dir))
        store1.save_file("persistent", str(csv))
        del store1

        # New DataStore instance picks up the catalog
        store2 = DataStore(str(store_dir))
        assert "persistent" in store2._catalog
        result = store2.query("SELECT * FROM persistent ORDER BY a")
        assert result["a"].tolist() == [1, 2]

    def test_catalog_json_valid(self, tmp_path):
        csv = tmp_path / "data.csv"
        pd.DataFrame({"a": [1]}).to_csv(csv, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("ds", str(csv))
        catalog_path = tmp_path / "store" / ".catalog.json"
        catalog = json.loads(catalog_path.read_text())
        assert "ds" in catalog
        assert catalog["ds"]["rows"] == 1


class TestDataStoreCompression:
    def test_gzip_compression(self, tmp_path):
        csv = tmp_path / "data.csv"
        pd.DataFrame({"a": range(10)}).to_csv(csv, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("gzipped", str(csv), compression="gzip")
        assert store._catalog["gzipped"]["compression"] == "gzip"
        result = store.query("SELECT COUNT(*) as n FROM gzipped")
        assert result["n"].iloc[0] == 10

    def test_zstd_compression(self, tmp_path):
        csv = tmp_path / "data.csv"
        pd.DataFrame({"a": range(10)}).to_csv(csv, index=False)

        store = DataStore(str(tmp_path / "store"))
        store.save_file("zstd", str(csv), compression="zstd")
        result = store.query("SELECT COUNT(*) as n FROM zstd")
        assert result["n"].iloc[0] == 10


class TestDataStoreConnectionsYaml:
    def test_loads_connections(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_HOST", "myhost")
        conn_file = tmp_path / "connections.yaml"
        conn_file.write_text(textwrap.dedent("""\
            impala_test:
              type: impala
              host: ${TEST_HOST}
              port: 21050
              user: testuser
              password: testpass
        """))
        store = DataStore(str(tmp_path / "store"), connections_file=str(conn_file))
        assert "impala_test" in store._connections
        assert store._connections["impala_test"]["host"] == "myhost"

    def test_connections_with_ssh(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BASTION", "jump.example.com")
        conn_file = tmp_path / "connections.yaml"
        conn_file.write_text(textwrap.dedent("""\
            db:
              type: impala
              host: db-host
              ssh:
                host: ${BASTION}
                user: admin
                private_key: /path/to/key
        """))
        store = DataStore(str(tmp_path / "store"), connections_file=str(conn_file))
        cfg = store._connections["db"]
        assert cfg["ssh"]["host"] == "jump.example.com"

    def test_connections_df(self, tmp_path):
        conn_file = tmp_path / "connections.yaml"
        conn_file.write_text(textwrap.dedent("""\
            mydb:
              type: impala
              host: somehost
              port: 21050
              database: default
        """))
        store = DataStore(str(tmp_path / "store"), connections_file=str(conn_file))
        df = store.connections()
        assert len(df) == 1
        assert df["name"].iloc[0] == "mydb"

    def test_missing_connection_raises(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        with pytest.raises(KeyError, match="not found"):
            store.connect("nonexistent")

    def test_no_connections_file(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        df = store.connections()
        assert len(df) == 0


class TestDataStoreDisconnect:
    def test_disconnect_no_tunnels(self, tmp_path):
        store = DataStore(str(tmp_path / "store"))
        # Should not raise
        store.disconnect()
        store.disconnect("anything")

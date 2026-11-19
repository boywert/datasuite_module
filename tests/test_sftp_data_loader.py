"""Tests for SFTPDataLoader.list_remote — uses an in-memory fake SFTP client."""

import stat
from types import SimpleNamespace

import pytest

from datasuite.sftp_data_loader import (
    SFTPDataLoader, _human_size, _list_remote_from_line,
)


def _f(name, size=1_048_576, mtime=1_700_000_000):
    return SimpleNamespace(filename=name, st_mode=stat.S_IFREG | 0o644,
                           st_size=size, st_mtime=mtime)


def _d(name):
    return SimpleNamespace(filename=name, st_mode=stat.S_IFDIR | 0o755,
                           st_size=0, st_mtime=0)


def _link(name):
    return SimpleNamespace(filename=name, st_mode=stat.S_IFLNK | 0o777,
                           st_size=0, st_mtime=0)


TREE = {
    "/data": [_f("sales_2024.csv"), _f("SALES_2023.CSV"), _f("notes.pdf"),
              _f("events.parquet", size=2_097_152), _link("latest.csv"),
              _d("archive"), _d("locked")],
    "/data/archive": [_f("sales_2022.xlsx"), _d("old")],
    "/data/archive/old": [_f("sales_2019.txt")],
}


class FakeSFTP:
    def listdir_attr(self, path):
        if path == "/data/locked":
            raise PermissionError(13, "Permission denied")
        if path not in TREE:
            raise FileNotFoundError(2, "No such file")
        return TREE[path]


@pytest.fixture
def loader():
    ld = SFTPDataLoader(hostname="h", username="u")
    ld.sftp = FakeSFTP()  # skip connect()
    return ld


COLUMNS = ["name", "folder", "file_type", "size", "modified", "path", "size_bytes"]


def test_lists_only_table_files_in_folder(loader):
    df = loader.list_remote("/data", sort="name")
    assert list(df["name"]) == ["events.parquet", "SALES_2023.CSV", "sales_2024.csv"]
    assert list(df.columns) == COLUMNS
    row = df[df["name"] == "events.parquet"].iloc[0]
    assert row["size"] == "2.0 MB" and row["size_bytes"] == 2_097_152
    assert row["path"] == "/data/events.parquet" and row["folder"] == "."


def test_pattern_is_case_insensitive_by_default(loader):
    df = loader.list_remote("/data", pattern="sales*")
    assert set(df["name"]) == {"sales_2024.csv", "SALES_2023.CSV"}


def test_case_sensitive(loader):
    df = loader.list_remote("/data", pattern="sales*", case_sensitive=True)
    assert list(df["name"]) == ["sales_2024.csv"]


def test_recursive_skips_unreadable_and_symlinks(loader, capsys):
    df = loader.list_remote("/data", pattern="sales*", recursive=True)
    assert set(df["folder"]) == {".", "archive", "archive/old"}
    assert set(df["path"]) == {
        "/data/sales_2024.csv", "/data/SALES_2023.CSV",
        "/data/archive/sales_2022.xlsx", "/data/archive/old/sales_2019.txt",
    }
    out = capsys.readouterr().out
    assert "Skipped /data/locked" in out
    assert "4 file(s)" in out and "3 folder(s) searched, 1 skipped" in out


def test_max_depth(loader):
    df = loader.list_remote("/data", pattern="sales*", recursive=True, max_depth=1)
    assert "/data/archive/old/sales_2019.txt" not in set(df["path"])
    assert "/data/archive/sales_2022.xlsx" in set(df["path"])


def test_file_types_filter_and_none(loader):
    assert list(loader.list_remote("/data", file_types=["parquet"])["name"]) == ["events.parquet"]
    assert "notes.pdf" in set(loader.list_remote("/data", file_types=None)["name"])


def test_no_match_returns_empty_frame_with_columns(loader):
    df = loader.list_remote("/data", pattern="nothing*")
    assert df.empty and list(df.columns) == COLUMNS


def test_missing_root_folder_raises(loader):
    with pytest.raises(FileNotFoundError):
        loader.list_remote("/nope")


# ── sorting / display ────────────────────────────────────────────────────

SORT_TREE = {"/s": [_f("b.csv", size=10, mtime=1_700_000_300),
                    _f("A.csv", size=30, mtime=1_700_000_100),
                    _f("c.csv", size=20, mtime=1_700_000_200)]}


@pytest.fixture
def sort_loader(monkeypatch):
    monkeypatch.setitem(TREE, "/s", SORT_TREE["/s"])
    ld = SFTPDataLoader(hostname="h", username="u")
    ld.sftp = FakeSFTP()
    return ld


@pytest.mark.parametrize("sort,expected", [
    ("modified", ["b.csv", "c.csv", "A.csv"]),   # newest first
    ("name", ["A.csv", "b.csv", "c.csv"]),       # case-insensitive A→Z
    ("size", ["A.csv", "c.csv", "b.csv"]),       # largest first
])
def test_sort_orders(sort_loader, sort, expected):
    assert list(sort_loader.list_remote("/s", sort=sort, verbose=False)["name"]) == expected


def test_ascending_override(sort_loader):
    df = sort_loader.list_remote("/s", sort="modified", ascending=True, verbose=False)
    assert list(df["name"]) == ["A.csv", "c.csv", "b.csv"]


def test_bad_sort_raises(loader):
    with pytest.raises(ValueError):
        loader.list_remote("/data", sort="color")


def test_tz_aware_and_local(sort_loader):
    df = sort_loader.list_remote("/s", tz="Asia/Bangkok", sort="name", verbose=False)
    assert str(df["modified"].dt.tz) == "Asia/Bangkok"
    assert df.loc[0, "modified"].hour == (1_700_000_100 // 3600 % 24 + 7) % 24
    assert df.loc[0, "modified"] == df.loc[0, "modified"].floor("s")
    local = sort_loader.list_remote("/s", verbose=False)
    assert local["modified"].dt.tz is None


def test_verbose_false_prints_nothing(loader, capsys):
    loader.list_remote("/data", recursive=True, verbose=False)
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("n,expected", [
    (0, "0 B"), (900, "900 B"), (1536, "1.5 KB"),
    (1_048_576, "1.0 MB"), (3 * 1024**3, "3.0 GB"), (None, "0 B"),
])
def test_human_size(n, expected):
    assert _human_size(n) == expected


# ── %parqstore --list line parsing ──────────────────────────────────────

def test_magic_line_parsing(loader):
    ns = {"ld": loader}
    df, limit, var = _list_remote_from_line(
        'ld /data "sales*" -r --max-depth 1 --sort name --limit 50 --var files', ns)
    assert limit == 50 and var == "files"
    assert list(df["name"]) == ["sales_2022.xlsx", "SALES_2023.CSV", "sales_2024.csv"]


def test_magic_types_and_all_files(loader):
    df, *_ = _list_remote_from_line("ld /data --types parquet", {"ld": loader})
    assert list(df["name"]) == ["events.parquet"]
    df, *_ = _list_remote_from_line("ld /data --all-files", {"ld": loader})
    assert "notes.pdf" in set(df["name"])


def test_magic_unknown_or_wrong_variable(loader):
    with pytest.raises(NameError):
        _list_remote_from_line("nope /data", {})
    with pytest.raises(TypeError):
        _list_remote_from_line("x /data", {"x": 42})

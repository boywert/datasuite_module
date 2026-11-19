# sftp_data_loader — download remote files over SFTP into DataFrames
#
# SFTPDataLoader establishes an SSH/SFTP connection (optionally through
# a bastion/jump host) and streams remote files through read_local() to
# produce a pandas DataFrame or polars LazyFrame.
#
# Downloaded files are cached locally as Parquet keyed by an MD5 hash of
# (hostname + remote path + engine + parse args), with a configurable TTL
# (default 1 hour).  On a cache hit the Parquet is read directly, skipping
# the network transfer entirely.
#
# Supports password or key-based auth, context-manager usage, and
# prefetching (paramiko's ``prefetch()`` for faster large-file reads).

import pandas as pd
import polars as pl
import os

try:
    import paramiko
except ImportError:
    paramiko = None
import time
import hashlib
import pathlib
import argparse
import fnmatch
import posixpath
import shlex
import stat
from datetime import datetime
from typing import Iterable, Optional, Union
from .read_local import read_local
from .framework_config import _resolve_engine


# File extensions that read_local() / DataStore.save_file() can parse as tables.
TABLE_EXTENSIONS = (
    ".csv", ".txt", ".parquet", ".pqt",
    ".xlsx", ".xls", ".xlsm", ".ods", ".odf",
)

_LIST_COLUMNS = ["name", "folder", "file_type", "size", "modified", "path", "size_bytes"]

# sort key → (column, default ascending)
_LIST_SORTS = {
    "modified": ("modified", False),    # newest first
    "name": ("name", True),
    "size": ("size_bytes", False),      # largest first
    "path": ("path", True),
}


def _human_size(n: Optional[int]) -> str:
    """Format a byte count as e.g. ``"900 B"``, ``"1.2 MB"``, ``"3.4 GB"``."""
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{int(n)} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def _list_remote_from_line(line: str, user_ns: dict) -> tuple:
    """Parse a ``%parqstore --list`` line and run ``list_remote``.

    Syntax::

        <loader_var> [remote_dir] [pattern] [-r] [--max-depth N]
                     [--types csv,parquet | --all-files] [--sort KEY]
                     [--limit N] [--var NAME]

    Returns ``(df, limit, var_name)``.  Kept outside ``enable_magic`` so it
    can be unit-tested without IPython.
    """
    parser = argparse.ArgumentParser(prog="%parqstore --list", add_help=False)
    parser.add_argument("loader")
    parser.add_argument("remote_dir", nargs="?", default=".")
    parser.add_argument("pattern", nargs="?", default="*")
    parser.add_argument("-r", "--recursive", action="store_true")
    parser.add_argument("--max-depth", type=int, default=None)
    parser.add_argument("--types", default=None)
    parser.add_argument("--all-files", action="store_true")
    parser.add_argument("--sort", default="modified", choices=sorted(_LIST_SORTS))
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--var", default=None)
    # shlex: allow quoted paths with spaces; posix=True keeps '*' literal
    args = parser.parse_args(shlex.split(line))

    if args.loader not in user_ns:
        raise NameError(f"Variable '{args.loader}' not found in the notebook namespace.")
    loader = user_ns[args.loader]
    if not hasattr(loader, "list_remote"):
        raise TypeError(f"'{args.loader}' is not an SFTPDataLoader.")

    if args.all_files:
        file_types = None
    elif args.types:
        file_types = [t.strip() for t in args.types.split(",") if t.strip()]
    else:
        file_types = TABLE_EXTENSIONS

    df = loader.list_remote(
        args.remote_dir, pattern=args.pattern, recursive=args.recursive,
        max_depth=args.max_depth, file_types=file_types, sort=args.sort,
    )
    return df, args.limit, args.var


class SFTPDataLoader:
    """
    Load remote files over SFTP into a DataFrame, with optional local caching.

    Supports direct connections and connections through a bastion/jump host.
    The SSH connection is established lazily — nothing happens until the first
    call to read_remote() or an explicit connect().

    Caching
    -------
    Downloaded files are cached locally as Parquet (under `cache_dir`).
    The cache key is an MD5 hash of: hostname + remote path + engine + parse args.
    Cached files are reused if they are younger than `cache_ttl` seconds (default 1 h).

    Usage
    -----
    # Direct connection
    loader = SFTPDataLoader(hostname="sftp.example.com", username="user", pkey="/path/to/key")
    df = loader.read_remote("/data/file.csv", file_type="csv")
    loader.close()

    # Via bastion host, using context manager
    with SFTPDataLoader(
        hostname="internal-server",
        username="user",
        pkey="/path/to/key",
        bastion_hostname="bastion.example.com",
        bastion_username="bastion_user",
        bastion_pkey="/path/to/bastion_key",
    ) as loader:
        df = loader.read_remote("/data/file.parquet", engine="polars")
    """

    def __init__(
        self,
        hostname: str,
        username: str,
        # Auth for Target
        password: Optional[str] = None,
        pkey: Optional[str] = None,
        # Bastion Configuration
        bastion_hostname: Optional[str] = None,
        bastion_username: Optional[str] = None,
        bastion_password: Optional[str] = None,
        bastion_pkey: Optional[str] = None,
        # Caching Default Configuration
        cache_dir: str = "./sftp_cache",
        cache_ttl: int = 3600,
    ):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.pkey = pkey

        self.bastion_hostname = bastion_hostname
        self.bastion_username = bastion_username
        self.bastion_password = bastion_password
        self.bastion_pkey = bastion_pkey

        self.cache_dir = cache_dir
        self.cache_ttl = cache_ttl  # seconds before a cached file is considered stale

        # SSH/SFTP client handles — populated on connect()
        self.client: Optional[paramiko.SSHClient] = None
        self.bastion_client: Optional[paramiko.SSHClient] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

    def connect(self):
        """
        Establish the SSH/SFTP connection.

        If a bastion host is configured, a tunnel is opened through it first and
        the target connection is piped through that socket. Calling connect() when
        already connected is a no-op.
        """
        transport = self.client.get_transport() if self.client is not None else None
        if transport is not None and transport.is_active():
            return  # Already connected

        # Drop any stale handles (e.g. a dropped connection) before reconnecting,
        # so an old bastion session is not leaked.
        self._close_handles()

        try:
            self._open_connections()
        except BaseException:
            self._close_handles()
            raise
        print("Connection established.")

    def _open_connections(self):
        """Open the bastion (if any), target SSH client and SFTP session."""
        proxy_socket = None

        # --- Step 1: Connect to Bastion (jump host), if configured ---
        if self.bastion_hostname:
            print(f"Connecting to Bastion: {self.bastion_hostname}...")
            self.bastion_client = paramiko.SSHClient()
            self.bastion_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.bastion_client.connect(
                self.bastion_hostname,
                username=self.bastion_username,
                password=self.bastion_password,
                key_filename=self.bastion_pkey,
            )
            # Open a direct-tcpip channel through the bastion to the target host.
            # This acts as a TCP tunnel (similar to ssh -W or ProxyJump).
            transport = self.bastion_client.get_transport()
            proxy_socket = transport.open_channel(
                "direct-tcpip",
                (self.hostname, 22),
                ('0.0.0.0', 0),
            )

        # --- Step 2: Connect to the target server ---
        # If proxy_socket is set, paramiko routes the connection through the bastion tunnel.
        print(f"Connecting to Target: {self.hostname}...")
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            self.hostname,
            username=self.username,
            password=self.password,
            key_filename=self.pkey,
            sock=proxy_socket,
        )

        self.sftp = self.client.open_sftp()

    def list_remote(
        self,
        remote_dir: str = ".",
        pattern: str = "*",
        recursive: bool = False,
        max_depth: Optional[int] = None,
        file_types: Optional[Iterable[str]] = TABLE_EXTENSIONS,
        case_sensitive: bool = False,
        sort: str = "modified",
        ascending: Optional[bool] = None,
        tz: Optional[str] = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        List / search table files in a remote SFTP folder.

        Parameters
        ----------
        remote_dir : str
            Remote folder to look in (default: the login directory).
        pattern : str
            Shell-style wildcard matched against the **file name**, e.g.
            ``"sales_2024*"``, ``"*cust*"``, ``"report_??.xlsx"``.
            Default ``"*"`` matches everything.
        recursive : bool
            Also search sub-folders.  Symlinked folders are not followed.
            Prints a live folder/file counter while scanning.
        max_depth : int, optional
            With ``recursive=True``, how many folder levels below
            ``remote_dir`` to descend (``1`` = direct sub-folders only).
            ``None`` = unlimited.
        file_types : iterable of str, optional
            Extensions to keep, with or without the dot (``["csv", ".parquet"]``).
            Defaults to every format ``read_remote()`` can parse
            (``TABLE_EXTENSIONS``).  Pass ``None`` to list all files.
        case_sensitive : bool
            Match ``pattern`` and extensions case-sensitively (default False,
            so ``"*.CSV"`` and ``"*.csv"`` both match).
        sort : str
            ``"modified"`` (newest first, default) | ``"name"`` (A→Z) |
            ``"size"`` (largest first) | ``"path"`` (A→Z).
            Name and path sorting ignore case.
        ascending : bool, optional
            Override the sort direction.
        tz : str, optional
            Show ``modified`` in this time zone, e.g. ``"Asia/Bangkok"`` or
            ``"UTC"`` (timezone-aware).  Default ``None`` = the notebook
            machine's local time — note that in a container this is often UTC.
        verbose : bool
            Print progress (recursive only) and a one-line summary.

        Returns
        -------
        pd.DataFrame
            ``name``        file name
            ``folder``      folder relative to ``remote_dir`` (``"."`` = top level)
            ``file_type``   extension without the dot
            ``size``        human-readable size, e.g. ``"1.2 MB"``
            ``modified``    last-modified time
            ``path``        full remote path — pass to ``read_remote()`` /
                            ``DataStore.save_remote()``
            ``size_bytes``  raw size, for filtering (``df[df.size_bytes > 1e9]``)

        Example
        -------
        loader.list_remote("/data/exports")                              # newest first
        loader.list_remote("/data", pattern="*sales*", recursive=True)   # search sub-folders
        loader.list_remote("/data", file_types=["parquet"], sort="size")
        files = loader.list_remote("/data", pattern="txn_2024*.csv", sort="name")
        for p in files["path"]:
            store.save_remote("txn_2024", loader, p, mode="append")
        """
        if sort not in _LIST_SORTS:
            raise ValueError(f"sort must be one of {sorted(_LIST_SORTS)}; got {sort!r}")

        if self.sftp is None:
            self.connect()

        norm = (lambda s: s) if case_sensitive else str.lower
        exts = None
        if file_types is not None:
            exts = {norm("." + t.lstrip(".")) for t in file_types}
        pat = norm(pattern)
        show_progress = verbose and recursive

        rows, skipped, searched = [], [], 0
        # Iterative walk: stack of (folder, depth) avoids recursion limits.
        stack = [(remote_dir, 0)]
        while stack:
            folder, depth = stack.pop()
            try:
                entries = self.sftp.listdir_attr(folder)
            except (IOError, OSError) as e:
                if folder == remote_dir:
                    raise  # the folder the caller asked for must be readable
                skipped.append((folder, e))
                continue
            searched += 1

            rel_folder = "." if folder == remote_dir else posixpath.relpath(folder, remote_dir)
            for attr in entries:
                path = posixpath.join(folder, attr.filename)
                mode = attr.st_mode or 0
                if stat.S_ISDIR(mode):
                    if recursive and (max_depth is None or depth < max_depth):
                        stack.append((path, depth + 1))
                    continue
                if not stat.S_ISREG(mode):
                    continue  # skip symlinks, sockets, etc.

                name = norm(attr.filename)
                ext = posixpath.splitext(name)[1]
                if exts is not None and ext not in exts:
                    continue
                if not fnmatch.fnmatchcase(name, pat):
                    continue

                rows.append({
                    "name": attr.filename,
                    "folder": rel_folder,
                    "file_type": ext.lstrip("."),
                    "size": _human_size(attr.st_size),
                    "modified": attr.st_mtime or None,   # epoch seconds; converted below
                    "path": path,
                    "size_bytes": int(attr.st_size or 0),
                })

            if show_progress:
                print(f"\r  Scanning... {searched} folder(s), {len(rows)} file(s) found",
                      end="", flush=True)

        if show_progress:
            print("\r" + " " * 70 + "\r", end="", flush=True)  # clear the progress line

        df = pd.DataFrame(rows, columns=_LIST_COLUMNS)

        # epoch seconds → requested zone (tz-aware) or local wall-clock (naive)
        if tz is not None:
            df["modified"] = pd.to_datetime(
                df["modified"].astype("float64"), unit="s", utc=True,
            ).dt.tz_convert(tz)
        else:
            df["modified"] = pd.to_datetime(
                [datetime.fromtimestamp(t) if pd.notna(t) else pd.NaT for t in df["modified"]]
            )

        col, default_asc = _LIST_SORTS[sort]
        df = df.sort_values(
            col,
            ascending=default_asc if ascending is None else ascending,
            key=(lambda s: s.str.lower()) if col in ("name", "path") else None,
            na_position="last",
            kind="stable",
            ignore_index=True,
        )

        if verbose:
            msg = f"{len(df):,} file(s), {_human_size(df['size_bytes'].sum())} in {remote_dir}"
            if recursive:
                msg += f"  ({searched} folder(s) searched"
                if skipped:
                    msg += f", {len(skipped)} skipped"
                msg += ")"
            print(msg)
            for f, e in skipped:
                print(f"  Skipped {f}: {e}")

        return df

    def read_remote(
        self,
        remotepath: str,
        use_cache: bool = True,
        use_prefetch: bool = True,
        *args,
        engine: Optional[str] = None,
        **kwargs,
    ) -> Union[pd.DataFrame, pl.LazyFrame]:
        """
        Download and parse a remote file, with optional local caching.

        Parameters
        ----------
        remotepath : str
            Absolute path of the file on the remote server.
        use_cache : bool
            When True, check for a local Parquet cache before downloading.
            On a cache miss the file is downloaded and the result is saved to cache.
        use_prefetch : bool
            When True, ask paramiko to prefetch the full file into its buffer before
            reading. This avoids repeated round-trips and is faster for large files.
        engine : str, optional
            "pandas" → returns pd.DataFrame
            "polars" → returns pl.LazyFrame
            Defaults to FrameworkConfig.dataframe_engine.
        *args, **kwargs
            Forwarded to read_local() (and from there to the underlying reader).
            Common examples: file_type="csv", sep="|", encoding="utf-8"
        """
        resolved = _resolve_engine(engine)

        # read_local() gets an open file handle, so it cannot see the file
        # name — infer the type from the remote path unless the caller set one.
        if not args and kwargs.get("file_type") is None:
            ext = posixpath.splitext(remotepath)[1].lstrip(".").lower()
            if not ext:
                raise ValueError(
                    f"Cannot infer the file type of {remotepath!r}; pass file_type=\"csv\" etc."
                )
            kwargs["file_type"] = ext

        # --- 1. Check local cache ---
        cache_path = None
        if use_cache:
            # Hash uniquely identifies this file + parse config combination.
            # engine is included so pandas and polars cache entries don't collide.
            unique_sig = f"{self.hostname}:{remotepath}:{resolved}:{str(args)}:{str(kwargs)}"
            file_hash = hashlib.md5(unique_sig.encode('utf-8')).hexdigest()

            pathlib.Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
            cache_path = os.path.join(self.cache_dir, f"{file_hash}.parquet")

            if os.path.exists(cache_path):
                file_age = time.time() - os.path.getmtime(cache_path)
                if file_age < self.cache_ttl:
                    print(f"Loading from cache ({file_age:.1f}s old): {remotepath}")
                    if resolved == 'polars':
                        # scan_parquet is fully lazy — no data is read until .collect()
                        return pl.scan_parquet(cache_path)
                    return pd.read_parquet(cache_path)

        # --- 2. Auto-connect if not already connected ---
        if self.sftp is None:
            self.connect()

        # --- 3. Download and parse the remote file ---
        print(f"Downloading: {remotepath}")
        with self.sftp.open(remotepath, mode='r') as remote_file:
            if use_prefetch:
                # Tells paramiko to read the whole file into its internal buffer
                # asynchronously. Much faster than letting read_local() pull bytes
                # in small chunks over the network.
                remote_file.prefetch()
            df = read_local(remote_file, *args, engine=resolved, **kwargs)

        # --- 4. Save to cache ---
        if use_cache and cache_path:
            if isinstance(df, pl.LazyFrame):
                # sink_parquet streams the LazyFrame to disk without materialising
                # the full DataFrame in memory. After this, the BytesIO that backed
                # the LazyFrame (holding the raw downloaded bytes) can be GC'd.
                # We then return a fresh lazy scan of the cache file instead.
                df.sink_parquet(cache_path)
                return pl.scan_parquet(cache_path)
            else:
                df.to_parquet(cache_path)

        return df

    def _close_handles(self):
        """Close and reset every SFTP/SSH handle, ignoring close errors."""
        for handle in (self.sftp, self.client, self.bastion_client):
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
        self.sftp = None
        self.client = None
        self.bastion_client = None

    def close(self):
        """Close all active SSH/SFTP connections."""
        self._close_handles()
        print("Connections closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

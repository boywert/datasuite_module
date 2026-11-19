from setuptools import setup, find_packages

setup(
    name='datasuite',
    version='0.1.9',
    packages=find_packages(),

    # ── Core dependencies ────────────────────────────────────────────────
    # All packages listed here are required for the base install.
    #
    # NOTE for developers:
    #   - ibm_db requires the IBM CLIDRIVER to be installed separately on
    #     the system.  On Windows the DLLs are auto-discovered from
    #     site-packages/clidriver/bin.  If ibm_db cannot be installed in
    #     your dev environment, the module still imports — DB2Connect will
    #     raise at runtime instead of at import time.
    #   - impyla (Impala/Hive) and sshtunnel follow the same pattern:
    #     graceful ImportError at module level, runtime error on use.
    #   - Excel readers are not imported directly but are required by the
    #     pandas / polars read_excel engines used in read_local and
    #     parquet_store: openpyxl (.xlsx/.xlsm), xlrd (.xls), odfpy
    #     (.ods/.odf), fastexcel (polars engine).  xlsxwriter is for writing.
    # ── Version policy ───────────────────────────────────────────────────
    # Floors = versions the test suite passed on (Python 3.12, 2026-11-19).
    # Caps   = below the next major (or next minor for 0.x packages), since
    #          major releases have removed APIs we use (duckdb 2: .arrow(),
    #          polars 2: read_csv_batched).  For an exact, reproducible
    #          environment use the lock file:
    #              pip install -e ".[dev]" -c constraints-py312.txt
    install_requires=[
        # DataFrame / storage core
        'pandas>=3.1.1,<4',          # core — primary DataFrame engine
        'polars>=2.1.0,<3',          # core — alternative DataFrame engine (LazyFrame)
        'pyarrow>=26.0.1,<27',       # core — Arrow IPC, Parquet read/write
        'duckdb>=2.0.0,<3',          # local_query, parquet_store — in-memory SQL engine
        'pyyaml>=6.0.3,<7',          # parquet_store      — parse connections.yaml

        # Excel / spreadsheet I/O
        'xlsxwriter>=3.3.1,<4',      # export_excel       — write .xlsx files
        'openpyxl>=3.1.5,<4',        # read_local         — pandas read .xlsx/.xlsm
        'xlrd>=2.0.2,<3',            # read_local         — pandas read legacy .xls
        'odfpy>=1.4.1,<2',           # read_local         — pandas read .ods/.odf
        'fastexcel>=0.21.0,<0.22',   # read_local         — polars read_excel engine

        # Connectivity
        'psutil>=7.2.2,<8',          # ssh_tunnel_connect — check port availability
        'paramiko>=5.1.0,<6',        # sftp_data_loader   — SSH/SFTP client
        'sshtunnel>=0.5.1,<0.6',     # ssh_tunnel_connect — SSHTunnelForwarder (<0.5 breaks on paramiko>=4)
        'impyla>=0.24.0,<0.25',      # db_connect, parquet_store — Impala/Hive DB-API driver
        'ibm_db>=3.3.0,<4',          # db_connect, parquet_store — IBM DB2 DB-API driver
        'google-cloud-bigquery>=3.47.0,<4',  # parquet_store — BigQuery source

        # Notebook magics
        'ipython>=9.18.1,<10',       # parquet_store      — %%parqstore / %%save / %%query magics
        'magic-duckdb>=0.2.4,<0.3',  # parquet_store      — %%dql magic (load_ext magic_duckdb)
    ],

    extras_require={
        'dev': [
            'pytest>=9.2.0,<10',     # tests/
        ],
    },

    author='Chaichalit Srisawat(Boyd)',
    author_email='boywert@gmail.com',
    description='A Python module to do headache things',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.12',
)

"""
End-to-end DB2 workflow:
  1. Query small dataset → Polars LazyFrame  (DB2Connect)
  2. Stream large dataset → chunked Parquet   (DataStore)
  3. Join + aggregate via %%dql               (DuckDB)
  4. Export result to Excel                   (export_excel)

Prerequisites:
  - Environment variables set: DB2_SERVER, DB2_USER, DB2_PASS,
    BASTION_SERVER, BASTION_USER, BASTION_PRIVATE_KEY
  - Or running on a host listed in DATASUITE_DIRECT_HOSTS (direct access, no tunnel needed)
"""

from datasuite import DB2Connect, export_excel
from datasuite.parquet_store import DataStore

# ─── 1. Small query → Polars LazyFrame ───────────────────────────────────────

db2 = DB2Connect()

dataA = db2.query("""
    SELECT
        customer_id,
        customer_name,
        region,
        segment
    FROM SCHEMA.CUSTOMERS
    WHERE status = 'ACTIVE'
""", engine="polars")                          # → pl.LazyFrame

print(f"dataA: {dataA.collect().shape}")       # peek at shape without full collect


# ─── 2. Large query → chunked Parquet via DataStore ──────────────────────────

store = DataStore("/home/jovyan/work/datasets")

# connect using the same dbc_info env vars / tunnel as DB2Connect
conn = store.connect("db2")

store.save(
    "dataB",
    """
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        amount,
        product_category
    FROM SCHEMA.TRANSACTIONS
    WHERE YEAR(transaction_date) = 2024
    """,
    conn=conn,
    chunk_size=100_000,          # 100k rows per parquet file
    compression="snappy",
    mode="replace",              # overwrite if exists
)

store.disconnect("db2")         # close the SSH tunnel

# At this point dataB lives on disk as:
#   /home/jovyan/work/datasets/dataB/part-0000.parquet
#   /home/jovyan/work/datasets/dataB/part-0001.parquet
#   ...
# and is registered as a DuckDB view named "dataB".


# ─── 3. Register dataA in DuckDB and join ────────────────────────────────────

# Register the Polars LazyFrame so DuckDB can query it alongside dataB
store.ddb.register("dataA", dataA.collect())

# Join + aggregate — runs entirely in DuckDB, dataB is read from parquet
summary = store.query("""
    SELECT
        a.region,
        a.segment,
        b.product_category,
        COUNT(*)                    AS num_transactions,
        SUM(b.amount)               AS total_amount,
        AVG(b.amount)               AS avg_amount,
        MIN(b.transaction_date)     AS first_txn,
        MAX(b.transaction_date)     AS last_txn
    FROM dataB b
    JOIN dataA a USING (customer_id)
    GROUP BY a.region, a.segment, b.product_category
    ORDER BY total_amount DESC
""")

print(f"summary: {summary.shape}")
print(summary.head(10))


# ─── 4. Export to Excel ──────────────────────────────────────────────────────

# Also grab the raw joined detail for a second sheet
detail = store.query("""
    SELECT
        a.customer_name,
        a.region,
        b.transaction_date,
        b.amount,
        b.product_category
    FROM dataB b
    JOIN dataA a USING (customer_id)
    ORDER BY b.transaction_date DESC
    LIMIT 50000
""")

export_excel("db2_report.xlsx", [
    {"sheetname": "Summary",  "dataframe": summary},
    {"sheetname": "Detail",   "dataframe": detail},
])

print("✓ Saved db2_report.xlsx")

"""
Same workflow as db2_workflow.py but using Jupyter cell magics.
Copy each section into a separate notebook cell.
"""

# ── Cell 1: Setup ────────────────────────────────────────────────────────────

from datasuite import DB2Connect, export_excel
from datasuite.parquet_store import DataStore

db2 = DB2Connect()
store = DataStore("/home/jovyan/work/datasets")
store.enable_magic()


# ── Cell 2: Small query → Polars LazyFrame ───────────────────────────────────

dataA = db2.query("""
    SELECT customer_id, customer_name, region, segment
    FROM SCHEMA.CUSTOMERS
    WHERE status = 'ACTIVE'
""", engine="polars")

# register in DuckDB so %%dql and %%save can see it
store.ddb.register("dataA", dataA.collect())
dataA.collect().head()


# ── Cell 3: Large query → chunked Parquet (use %%save magic) ────────────────
# In a real notebook this would be a %%save cell:
#
# %%save dataB --conn db2 --mode replace --chunk-size 100000
# SELECT transaction_id, customer_id, transaction_date, amount, product_category
# FROM SCHEMA.TRANSACTIONS
# WHERE YEAR(transaction_date) = 2024

conn = store.connect("db2")
store.save(
    "dataB",
    """
    SELECT transaction_id, customer_id, transaction_date, amount, product_category
    FROM SCHEMA.TRANSACTIONS
    WHERE YEAR(transaction_date) = 2024
    """,
    conn=conn,
    chunk_size=100_000,
    mode="replace",
)
store.disconnect("db2")


# ── Cell 4: Join + aggregate with %%dql ──────────────────────────────────────
# In a real notebook:
#
# %%dql
# SELECT
#     a.region, a.segment, b.product_category,
#     COUNT(*)        AS num_transactions,
#     SUM(b.amount)   AS total_amount,
#     AVG(b.amount)   AS avg_amount
# FROM dataB b
# JOIN dataA a USING (customer_id)
# GROUP BY a.region, a.segment, b.product_category
# ORDER BY total_amount DESC

summary = store.query("""
    SELECT
        a.region, a.segment, b.product_category,
        COUNT(*)        AS num_transactions,
        SUM(b.amount)   AS total_amount,
        AVG(b.amount)   AS avg_amount
    FROM dataB b
    JOIN dataA a USING (customer_id)
    GROUP BY a.region, a.segment, b.product_category
    ORDER BY total_amount DESC
""")
summary.head(10)


# ── Cell 5: Export to Excel ──────────────────────────────────────────────────

export_excel("db2_report.xlsx", [
    {"sheetname": "Summary", "dataframe": summary},
])
print("✓ Saved db2_report.xlsx")

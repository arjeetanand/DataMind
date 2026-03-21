"""
DataMind — Ingestion Layer
Simulates an S3 Data Lake: raw CSV → partitioned Parquet files.

In production this would be:
  s3://datamind-lake/raw/retail/year=YYYY/month=MM/part-*.parquet
Here we mirror that partition strategy locally for demo.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import KAGGLE_CSV, PARQUET_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── Schema Enforcement ────────────────────────────────────────────────────────
DTYPES = {
    "Invoice"     : "str",
    "StockCode"   : "str",
    "Description" : "str",
    "Quantity"    : "float64",
    "Price"       : "float64",
    "Customer ID" : "str",
    "Country"     : "str",
}


def load_raw(csv_path: Path = KAGGLE_CSV) -> pd.DataFrame:
    """Read Online Retail II CSV with type coercion and basic cleaning."""
    log.info(f"Reading raw CSV: {csv_path}")
    df = pd.read_csv(
        csv_path,
        dtype=DTYPES,
        parse_dates=["InvoiceDate"],
        encoding="utf-8",
        on_bad_lines="skip",
    )
    # ── Standardise column names ──────────────────────────────────────────────
    df.rename(columns={"Customer ID": "CustomerID"}, inplace=True)

    raw_rows = len(df)
    # ── Remove cancellations (Invoice starts with C) ──────────────────────────
    df = df[~df["Invoice"].str.startswith("C", na=False)]
    # ── Drop rows with missing critical fields ─────────────────────────────────
    df.dropna(subset=["CustomerID", "Description", "InvoiceDate"], inplace=True)
    # ── Remove nonsensical quantities / prices ─────────────────────────────────
    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]
    # ── Derived columns ────────────────────────────────────────────────────────
    df["Revenue"]   = (df["Quantity"] * df["Price"]).round(2)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["Year"]      = df["InvoiceDate"].dt.year
    df["Month"]     = df["InvoiceDate"].dt.month

    log.info(f"Cleaned: {raw_rows:,} → {len(df):,} rows ({raw_rows - len(df):,} dropped)")
    return df


def write_partitioned_parquet(df: pd.DataFrame, out_dir: Path = PARQUET_DIR) -> None:
    """
    Write Parquet files partitioned by Year/Month.
    Mirrors S3 Hive-style partitioning: year=YYYY/month=MM/data.parquet
    """
    log.info(f"Writing partitioned Parquet files (lake simulation)... Columns: {df.columns.tolist()}")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(out_dir),
        partition_cols=["Year", "Month"],
        existing_data_behavior="overwrite_or_ignore",
        compression="snappy",
    )
    parts = list(out_dir.rglob("*.parquet"))
    log.info(f"Wrote {len(parts)} Parquet partition file(s) to {out_dir}")


def read_parquet_lake(parquet_dir: Path = PARQUET_DIR) -> pd.DataFrame:
    """Read entire Parquet lake back into a single DataFrame."""
    log.info(f"Reading Parquet lake from {parquet_dir}")
    df = pd.read_parquet(parquet_dir)
    log.info(f"Lake loaded: {len(df):,} rows, {df['Year'].nunique()} year(s)")
    return df


def run_ingestion_pipeline() -> pd.DataFrame:
    """End-to-end ingestion: CSV → clean → Parquet lake."""
    df = load_raw()
    write_partitioned_parquet(df)
    return df


if __name__ == "__main__":
    run_ingestion_pipeline()

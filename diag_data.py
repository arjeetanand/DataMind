import pandas as pd
import numpy as np
from src.warehouse.queries import daily_sales_series
from config.settings import SEQ_LEN, PRED_LEN

def diag():
    print("Fetching data...")
    df = daily_sales_series()
    print(f"Rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    if df.empty:
        print("ERROR: DataFrame is empty!")
        return

    # Mock the feature engineering from forecaster.py
    df["ds"] = pd.to_datetime(df["ds"])
    df["dow"] = df["ds"].dt.dayofweek
    df["month"] = df["ds"].dt.month
    df["is_weekend"] = df["dow"].apply(lambda x: 1.0 if x >= 5 else 0.0)
    df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    
    features = ["y", "units", "orders", "dow_sin", "dow_cos", "month_sin", "month_cos", "is_weekend"]
    df_feats = df[features].fillna(0.0)
    print(f"Feature matrix shape: {df_feats.shape}")
    
    # Check dataset length
    dataset_len = len(df_feats) - SEQ_LEN - PRED_LEN + 1
    print(f"Dataset length (for DataLoader): {dataset_len}")
    if dataset_len <= 0:
        print(f"CRITICAL: Dataset length is {dataset_len}. This will cause a crash in DataLoader.")

if __name__ == "__main__":
    diag()

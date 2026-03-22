import sys
from pathlib import Path
import pandas as pd
from src.ml.forecaster import train
from src.warehouse.queries import daily_sales_series

def main():
    print("Fetching historical sales data...")
    df = daily_sales_series()
    print(f"Dataset size: {len(df)} days.")
    
    if len(df) < 60:
        print("Warning: Dataset might be too small for SEQ_LEN=60. Reducing SEQ_LEN locally...")
        # (This is handled by current dataset class with a len check)
    
    print("Starting LSTM retraining with new features...")
    model = train(df)
    print("Model retrained and saved successfully.")

if __name__ == "__main__":
    main()

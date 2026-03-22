"""
DataMind — PyTorch LSTM Demand Forecasting

Architecture:
  Input  → MinMaxScaler → LSTM (stacked) → Linear head → forecast
  
  Input shape:  (batch, seq_len, features)  — features: revenue, units, orders
  Output shape: (batch, pred_len)           — predicted daily revenue
"""

import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler

import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config.settings import (SEQ_LEN, PRED_LEN, HIDDEN_SIZE, NUM_LAYERS,
                              BATCH_SIZE, EPOCHS, LR, DEVICE, MODEL_DIR)

MODEL_PATH  = MODEL_DIR / "lstm_forecaster.pt"
SCALER_PATH = MODEL_DIR / "scaler.pkl"
FEATURES    = ["y", "units", "orders", "dow_sin", "dow_cos", "month_sin", "month_cos", "is_weekend"]


# ── Model Definition ──────────────────────────────────────────────────────────
class DemandLSTM(nn.Module):
    """
    Stacked LSTM for multi-step demand forecasting.
    Encoder-decoder architecture with dropout regularisation.
    """
    def __init__(self, input_size: int = 8, hidden_size: int = HIDDEN_SIZE,
                 num_layers: int = NUM_LAYERS, pred_len: int = PRED_LEN,
                 dropout: float = 0.2):
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers  = num_layers
        self.pred_len    = pred_len

        self.lstm = nn.LSTM(
            input_size  = input_size,
            hidden_size = hidden_size,
            num_layers  = num_layers,
            batch_first = True,
            dropout     = dropout if num_layers > 1 else 0.0,
        )
        self.attention = nn.MultiheadAttention(
            embed_dim   = hidden_size,
            num_heads   = 4,
            batch_first = True,
            dropout     = 0.1,
        )
        self.norm   = nn.LayerNorm(hidden_size)
        self.head   = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_size // 2, pred_len),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, input_size)
        lstm_out, _ = self.lstm(x)                      # (batch, seq_len, hidden)
        attn_out, _ = self.attention(lstm_out, lstm_out, lstm_out)
        out = self.norm(lstm_out + attn_out)            # residual
        context = out[:, -1, :]                         # last timestep
        return self.head(context)                       # (batch, pred_len)


# ── Dataset ───────────────────────────────────────────────────────────────────
class TimeSeriesDataset(torch.utils.data.Dataset):
    def __init__(self, data: np.ndarray, seq_len: int, pred_len: int):
        self.data     = data
        self.seq_len  = seq_len
        self.pred_len = pred_len

    def __len__(self):
        return len(self.data) - self.seq_len - self.pred_len + 1

    def __getitem__(self, idx):
        x = self.data[idx : idx + self.seq_len]                   # (seq_len, features)
        y = self.data[idx + self.seq_len : idx + self.seq_len + self.pred_len, 0]  # revenue only
        return torch.FloatTensor(x), torch.FloatTensor(y)


# ── Training ──────────────────────────────────────────────────────────────────
def train(df: pd.DataFrame, epochs: int = EPOCHS) -> DemandLSTM:
    """Train LSTM on daily sales DataFrame."""
    # Feature Engineering
    df["ds"] = pd.to_datetime(df["ds"])
    df["dow"] = df["ds"].dt.dayofweek
    df["month"] = df["ds"].dt.month
    df["is_weekend"] = df["dow"].apply(lambda x: 1.0 if x >= 5 else 0.0)

    # Cyclical encoding
    df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    df_feats = df[FEATURES].fillna(0.0)

    # Scale
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(df_feats.values)

    # Save scaler
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(SCALER_PATH, "wb") as f:
        pickle.dump(scaler, f)

    # Train / val split (ensure val has enough for at least one batch)
    min_val_len = SEQ_LEN + PRED_LEN + 5
    if len(scaled) > min_val_len + 50:
        split = len(scaled) - min_val_len
    else:
        # Fallback for very small data: 90/10 split
        split = int(len(scaled) * 0.9)

    train_data = scaled[:split]
    val_data   = scaled[split:]

    train_ds = TimeSeriesDataset(train_data, SEQ_LEN, PRED_LEN)
    val_ds   = TimeSeriesDataset(val_data,   SEQ_LEN, PRED_LEN)

    train_dl = torch.utils.data.DataLoader(train_ds, batch_size=BATCH_SIZE, shuffle=True)
    val_dl   = torch.utils.data.DataLoader(val_ds,   batch_size=BATCH_SIZE)

    model     = DemandLSTM().to(DEVICE)
    optimizer = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=epochs)
    criterion = nn.HuberLoss()

    best_val = float("inf")
    patience_counter = 0
    for epoch in range(1, epochs + 1):
        model.train()
        train_loss = 0.0
        for xb, yb in train_dl:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            optimizer.zero_grad()
            pred = model(xb)
            loss = criterion(pred, yb)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            train_loss += loss.item()

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for xb, yb in val_dl:
                xb, yb = xb.to(DEVICE), yb.to(DEVICE)
                val_loss += criterion(model(xb), yb).item()

        scheduler.step()

        if val_loss < best_val:
            best_val = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), MODEL_PATH)
        else:
            patience_counter += 1

        if epoch % 5 == 0:
            print(f"Epoch {epoch:3d}/{epochs} | train={train_loss/len(train_dl):.4f}"
                  f" | val={val_loss/len(val_dl):.4f}")

        if patience_counter >= 15:
            print(f"Early stopping at epoch {epoch}")
            break

    # Reload best
    model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
    print(f"Training complete. Best val loss: {best_val:.4f}")
    return model


# ── Inference ──────────────────────────────────────────────────────────────────
def predict(df: pd.DataFrame, model: DemandLSTM = None) -> dict:
    """
    Given the last SEQ_LEN rows of daily_sales_series, forecast next PRED_LEN days.
    Returns dict with dates, predicted_revenue, confidence_interval.
    """
    if model is None:
        model = DemandLSTM().to(DEVICE)
        model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))

    with open(SCALER_PATH, "rb") as f:
        scaler: MinMaxScaler = pickle.load(f)

    # Feature Engineering for Inference
    df["ds"] = pd.to_datetime(df.index)
    df["dow"] = df["ds"].dt.dayofweek
    df["month"] = df["ds"].dt.month
    df["is_weekend"] = df["dow"].apply(lambda x: 1.0 if x >= 5 else 0.0)
    df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    df_feats = df[FEATURES].fillna(0.0).tail(SEQ_LEN)
    scaled = scaler.transform(df_feats.values)

    x = torch.FloatTensor(scaled).unsqueeze(0).to(DEVICE)  # (1, seq_len, 8)
    model.eval()

    # Monte Carlo dropout for uncertainty estimation
    model.train()  # keep dropout active
    preds = []
    with torch.no_grad():
        for _ in range(50):
            out = model(x).cpu().numpy()[0]          # (pred_len,)
            # Inverse transform (only revenue dimension)
            dummy = np.zeros((PRED_LEN, len(FEATURES)))
            dummy[:, 0] = out
            preds.append(scaler.inverse_transform(dummy)[:, 0])

    preds      = np.stack(preds)                     # (50, pred_len)
    mean_pred  = preds.mean(axis=0)
    lower      = np.percentile(preds, 5,  axis=0)
    upper      = np.percentile(preds, 95, axis=0)

    last_date  = pd.to_datetime(df.index[-1]) if isinstance(df.index[-1], (str,)) \
                 else pd.Timestamp.today()
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=PRED_LEN)

    return {
        "dates"    : future_dates.strftime("%Y-%m-%d").tolist(),
        "forecast" : np.round(mean_pred, 2).tolist(),
        "lower_ci" : np.round(lower, 2).tolist(),
        "upper_ci" : np.round(upper, 2).tolist(),
    }


if __name__ == "__main__":
    # Quick smoke test with synthetic data
    np.random.seed(42)
    dummy = pd.DataFrame({
        "y"      : np.random.uniform(1000, 5000, 200),
        "units"  : np.random.randint(100, 500, 200).astype(float),
        "orders" : np.random.randint(10, 50, 200).astype(float),
    })
    model = train(dummy, epochs=3)
    result = predict(dummy, model)
    print("Forecast:", result)

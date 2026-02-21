import numpy as np
import pandas as pd


SEED        = 42
N_ROWS      = 10_000
START_PRICE = 30_000.0
START_DATE  = "2021-01-01"
OUTPUT_FILE = "data.csv"


def generate_ohlcv(seed: int, n: int, start_price: float, start_date: str) -> pd.DataFrame:
    np.random.seed(seed)

    timestamps = pd.date_range(start=start_date, periods=n, freq="1h")

    returns = np.random.normal(loc=0.0, scale=0.002, size=n)
    close   = start_price * np.exp(np.cumsum(returns))
    close[0] = start_price

    open_  = close * (1 + np.random.normal(0, 0.003, n))

    high   = close * (1 + np.abs(np.random.normal(0, 0.005, n)))

    low    = close * (1 - np.abs(np.random.normal(0, 0.005, n)))

    volume_btc = np.random.uniform(100, 5000, n)
    volume_usd = volume_btc * close

    df = pd.DataFrame({
        "timestamp":  timestamps,
        "open":       np.round(open_,       2),
        "high":       np.round(high,        2),
        "low":        np.round(low,         2),
        "close":      np.round(close,       2),
        "volume_btc": np.round(volume_btc,  4),
        "volume_usd": np.round(volume_usd,  2),
    })

    return df


def main():
    print(f"Generating {N_ROWS:,} rows of synthetic BTC OHLCV data...")

    df = generate_ohlcv(
        seed        = SEED,
        n           = N_ROWS,
        start_price = START_PRICE,
        start_date  = START_DATE,
    )

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved to: {OUTPUT_FILE}")
    print(f"\nFirst 5 rows preview:")
    print(df.head().to_string(index=False))
    print(f"\nColumns: {list(df.columns)}")
    print(f"Shape:   {df.shape}")


if __name__ == "__main__":
    main()

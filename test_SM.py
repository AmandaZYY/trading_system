from signals import *
import pandas as pd
from config.dontshare_settings import DATA_DIR

if __name__ == "__main__":
    symbols = ["BTC/USD", "ETH/USD"]
    data_df = load_data(DATA_DIR)
    data_df, signals = generate_signals(data_df)
    print(signals)
    data = calculate_atr(data_df)
    print(data)
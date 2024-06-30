import pandas as pd
import numpy as np
from filelock import FileLock
from datetime import datetime, timezone

# Market data 5 min candles, then I try SMA10 (50 min), SMA100 (500min)
# TO-DO: Add cache to optimize memory usage; Handle exceptions when market data unavailble 


def get_csv_path(data_dir):
    # get ohlcv data
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{data_dir}/ohlcv_{date_str}.csv"


def load_data(data_dir):
    file_path = get_csv_path(data_dir)
    lock = FileLock(file_path + ".lock")
    with lock:
        data = pd.read_csv(file_path)
    return data


def identify_market_phases(data):
    data["SMA10"] = data["close"].rolling(window=10).mean()
    data["SMA100"] = data["close"].rolling(window=100).mean()

    conditions = [
        (data["SMA10"] > data["SMA100"]),  # Uptrend (Markup)
        (data["SMA10"] < data["SMA100"]),  # Downtrend (Markdown)
    ]
    choices = ["markup", "markdown"]
    data["phase"] = np.select(conditions, choices, default="neutral")

    return data


def detect_volume_spikes(data, threshold=2):
    data["volume_avg"] = data["volume"].rolling(window=20).mean()
    data["volume_spike"] = np.where(
        data["volume"] > threshold * data["volume_avg"], 1, 0
    )
    return data


def identify_support_resistance(data, window=20):
    data["rolling_min"] = data["close"].rolling(window=window).min()
    data["rolling_max"] = data["close"].rolling(window=window).max()
    support = data["rolling_min"].iloc[-1]
    resistance = data["rolling_max"].iloc[-1]
    return support, resistance


def determine_limit_price(data):
    support, resistance = identify_support_resistance(data)
    sma10, sma100 = data["SMA10"].iloc[-1], data["SMA100"].iloc[-1]

    # Determine limit price based on support/resistance and moving averages
    if data["close"].iloc[-1] < sma10:
        limit_price = support  # Buy at support level if below SMA10
    elif data["close"].iloc[-1] > sma100:
        limit_price = resistance  # Sell at resistance level if above SMA100
    else:
        limit_price = sma10  # Use SMA10 as a fallback

    return limit_price


def generate_signals(data):
    data = identify_market_phases(data)
    data = detect_volume_spikes(data)
    data = data.reset_index()  # make sure indexes pair with number of rows
    
    signals = []

    for i, row in data.iterrows():
        if row["phase"] == "markup" and row["volume_spike"] == 1:
            limit_price = determine_limit_price(data[: i + 1])
            signals.append((row["timestamp"], row["symbol"], "buy", limit_price))
        elif data["phase"].iloc[i] == "markdown" and data["volume_spike"].iloc[i] == 1:
            limit_price = determine_limit_price(data[: i + 1])
            signals.append((row["timestamp"], row["symbol"], "sell", limit_price))

    signals_df = pd.DataFrame(signals, columns=["timestamp", "symbol", "signal", "price"])

    return data, signals_df


def calculate_atr(data, period=14):
    """
    Calculate the Average True Range (ATR) for given data.

    :param data: DataFrame with columns 'High', 'Low', 'Close'
    :param period: The period over which to calculate the ATR
    :return: DataFrame with ATR values
    """
    data['High-Low'] = data['High'] - data['Low']
    data['High-PrevClose'] = abs(data['High'] - data['Close'].shift(1))
    data['Low-PrevClose'] = abs(data['Low'] - data['Close'].shift(1))

    true_range = data[['High-Low', 'High-PrevClose', 'Low-PrevClose']].max(axis=1)

    atr = true_range.rolling(window=period, min_periods=1).mean()

    data['ATR'] = atr
    return data
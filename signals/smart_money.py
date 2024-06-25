import pandas as pd
import numpy as np

def identify_market_phases(data):
    data['SMA50'] = data['close'].rolling(window=50).mean()
    data['SMA200'] = data['close'].rolling(window=200).mean()
    
    conditions = [
        (data['SMA50'] > data['SMA200']),  # Uptrend (Markup)
        (data['SMA50'] < data['SMA200'])   # Downtrend (Markdown)
    ]
    choices = ['markup', 'markdown']
    data['phase'] = np.select(conditions, choices, default='neutral')
    
    return data

def detect_volume_spikes(data, threshold=2):
    data['volume_avg'] = data['volume'].rolling(window=20).mean()
    data['volume_spike'] = np.where(data['volume'] > threshold * data['volume_avg'], 1, 0)
    return data

def identify_support_resistance(data, window=20):
    data['rolling_min'] = data['close'].rolling(window=window).min()
    data['rolling_max'] = data['close'].rolling(window=window).max()
    support = data['rolling_min'].iloc[-1]
    resistance = data['rolling_max'].iloc[-1]
    return support, resistance

def determine_limit_price(data):
    support, resistance = identify_support_resistance(data)
    sma50, sma200 = data['SMA50'].iloc[-1], data['SMA200'].iloc[-1]

    # Determine limit price based on support/resistance and moving averages
    if data['close'].iloc[-1] < sma50:
        limit_price = support  # Buy at support level if below SMA50
    elif data['close'].iloc[-1] > sma200:
        limit_price = resistance  # Sell at resistance level if above SMA200
    else:
        limit_price = sma50  # Use SMA50 as a fallback

    return limit_price

def generate_signals(data):
    data = identify_market_phases(data)
    data = detect_volume_spikes(data)
    
    signals = []

    for i in range(1, len(data)):
        if data['phase'].iloc[i] == 'markup' and data['volume_spike'].iloc[i] == 1:
            limit_price = determine_limit_price(data[:i+1])
            signals.append((data['timestamp'].iloc[i], 'buy', limit_price))
        elif data['phase'].iloc[i] == 'markdown' and data['volume_spike'].iloc[i] == 1:
            limit_price = determine_limit_price(data[:i+1])
            signals.append((data['timestamp'].iloc[i], 'sell', limit_price))
    
    signals_df = pd.DataFrame(signals, columns=['timestamp', 'signal', 'price'])
    
    return data, signals_df

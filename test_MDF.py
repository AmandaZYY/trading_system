# test run MarketDataFeed and save ohlcv csv

from data import MarketDataFeed
import asyncio
import signal
from config.settings import API_KEY, API_SECRET, DATA_DIR

if __name__ == "__main__":
    symbols = ["BTC/USD", "ETH/USD"]
    market_data_feed = MarketDataFeed(API_KEY, API_SECRET, DATA_DIR)

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, market_data_feed.stop)

    asyncio.run(market_data_feed.start(symbols))

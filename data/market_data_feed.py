import ccxt.async_support as ccxt
import pandas as pd
import asyncio
from datetime import datetime, timezone
import time
from filelock import FileLock
import os


class AsyncExchangeManager:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    async def __aenter__(self):
        self.exchange = ccxt.coinbase(
            {
                "apiKey": self.api_key,
                "secret": self.api_secret,
            }
        )
        return self.exchange

    async def __aexit__(self, exc_type, exc, tb):
        await self.exchange.close()


class MarketDataFeed:
    def __init__(self, api_key, api_secret, data_dir):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = []
        self.data_dir = data_dir
        self.last_timestamps = {symbol: None for symbol in self.symbols}
        self.running = True
        self.exchange = AsyncExchangeManager(self.api_key, self.api_secret)

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

    def add_subscription(self, symbols):
        self.symbols.extend(symbols)
        temp = {symbol: None for symbol in symbols}
        self.last_timestamps = self.last_timestamps | temp

    def get_csv_path(self, table_name):
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return f"{self.data_dir}/{table_name}_{date_str}.csv"

    async def fetch_ohlcv(self, exchange, symbol, timeframe="5m", since=None):
        last_timestamp = self.last_timestamps.get(symbol)

        if last_timestamp is None:
            # Fetch data from the beginning of today in UTC
            since = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            since = int(since.timestamp() * 1000)  # Convert to milliseconds
        else:
            since = int(last_timestamp.timestamp() * 1000)  # Convert to milliseconds

        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since=since)
        data = pd.DataFrame(
            ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        data["timestamp"] = pd.to_datetime(data["timestamp"], unit="ms")
        data["symbol"] = symbol

        return data

    def store_data(self, data, table_name):
        file_path = self.get_csv_path(table_name)
        lock = FileLock(file_path + ".lock")

        with lock:
            if os.path.exists(file_path):
                data.to_csv(file_path, mode="a", header=False, index=False)
            else:
                data.to_csv(file_path, mode="w", header=True, index=False)

    async def update_market_data(self):
        async with self.exchange as exchange:
            tasks = [self.fetch_ohlcv(exchange, symbol) for symbol in self.symbols]
            results = await asyncio.gather(*tasks)
            all_data = pd.concat(results)
            self.store_data(all_data, "ohlcv")

    async def start(self, symbols):
        self.add_subscription(symbols)
        while self.running:
            await self.update_market_data()
            await asyncio.sleep(5 * 60)  # Wait for 5 minutes

    def stop(self, signum, frame):
        print(f"Received signal {signum}, stopping...")
        self.running = False

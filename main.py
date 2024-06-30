import ccxt
import pandas as pd
import asyncio
from datetime import datetime
from broker import CoinbaseBroker
from data.market_data_feed import MarketDataFeed
from signals.smart_money import generate_signals, load_data, calculate_atr
from execution.order_manager import OrderManager
from config.dontshare_settings import API_KEY, API_SECRET, DATA_DIR
from utils.logger import setup_logger
import signal


class TradingSystem:
    def __init__(self, broker, order_manager, market_data_feed, symbols, interval=5, risk_target=0.25, total_capital=3000, portfolio_size=10, max_loss = 1000):
        self.broker = broker
        self.order_manager = order_manager
        self.market_data_feed = market_data_feed
        self.symbols = symbols
        self.interval = interval
        self.logger = setup_logger("trading_system", "trading_system.log")
        self.risk_target = risk_target
        self.total_capital = total_capital
        self.portfolio_size = portfolio_size
        self.stop_signal_received = False
        self.max_loss = max_loss
        self.current_loss = 0
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self.handle_stop_signal)
        signal.signal(signal.SIGTERM, self.handle_stop_signal)

    def handle_stop_signal(self, signum, frame):
        self.logger.info(f"Received stop signal: {signum}")
        self.stop_signal_received = True

    async def generate_and_execute_signals(self):
        data_df = load_data(DATA_DIR)

        _, signals = generate_signals(data_df)
        self.logger.info(f"Generated signals at {datetime.now()}: {signals}")
        
        _, atr = calculate_atr(data_df)
        # Get the latest ATR for each symbol
        atr = atr.loc[atr.groupby('symbol')['timestamp'].idxmax()]
        # Set the symbol column as the index
        atr.set_index('symbol', inplace=True)
        self.logger.info(f"calculated atr at {datetime.now()}: {atr}")
        
        # Debugging: Print columns and first few rows of signals DataFrame
        print("Signals DataFrame columns:", signals.columns)
        print("First few rows of signals DataFrame:\n", signals.head())
        
        for _, row in signals.iterrows():
            symbol = row["symbol"]
            if row["signal"] == "buy":
                size = self.get_size(symbol, atr)
                if size > 0:
                    self.order_manager.place_order(row["symbol"], size/row["price"], "buy")
            elif row["signal"] == "sell":
                size = self.get_size(symbol, atr)
                if size > 0:
                    self.order_manager.place_order(row["symbol"], size/row["price"], "sell")

        # Process any queued orders
        self.order_manager.process_orders()

        # Try to execute orders with the broker and log the results
        for order in self.order_manager.order_queue:
            result = self.broker.execute_order(
                order["symbol"], order["quantity"], order["side"]
            )
            if result["status"] == "filled":
                self.logger.info(f"Order filled: {result}")
                self.order_manager.update_positions(order)
            else:
                self.logger.info(f"Order not filled: {result}")

    async def start(self):
        while True:
            await self.generate_and_execute_signals()
            await asyncio.sleep(self.interval * 60)  # Convert minutes to seconds
    
    def get_size(self, symbol, atr_df, min_size = 30):
        # if size < min_size usd then don't trade
        # Update orders based on signals
        atr_value = self.get_atr_value(atr_df, symbol)
        if atr_value:
            size = (1 / self.portfolio_size) * self.total_capital * (self.risk_target / atr_value)
            if size < min_size:
                size = 0
        else:
            size = 0
        return size
        
    def check_stop_conditions(self):
        if self.current_loss >= self.max_loss:
            self.logger.info(f"Stopping trading system due to reaching max loss limit: {self.current_loss}")
            return True
        if self.stop_signal_received:
            self.logger.info("Stopping trading system due to received stop signal.")
            return True
        return False
    
    # Function to get the ATR value for a specific symbol
    def get_atr_value(self, atr_df, symbol):
        if symbol in atr_df.index:
            return atr_df.loc[symbol]['atr']
        else:
            return None

if __name__ == "__main__":
    broker = CoinbaseBroker(API_KEY, API_SECRET)
    order_manager = OrderManager(broker)
    market_data_feed = MarketDataFeed(API_KEY, API_SECRET, DATA_DIR)

    symbols = ["BTC/USD", "ETH/USD"]
    trading_system = TradingSystem(
        broker, order_manager, market_data_feed, symbols, interval=5
    )
        
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(trading_system.start())
    finally:
        loop.close()

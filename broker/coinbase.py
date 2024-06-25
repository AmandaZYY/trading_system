from .base import Broker
import ccxt

class CoinbaseBroker(Broker):
    def __init__(self, api_key, api_secret):
        super().__init__(api_key, api_secret)
        self.client = ccxt.coinbase({
            'apiKey': api_key,
            'secret': api_secret,
        })

    def get_account_balance(self):
        return self.client.fetch_balance()

    def place_order(self, symbol, quantity, side, price=None):
        order_type = 'limit' if price is not None else 'market'
        order = self.client.create_order(symbol, order_type, side, quantity, price)
        return order

    def cancel_order(self, order_id):
        return self.client.cancel_order(order_id)

    def get_order_status(self, order_id):
        return self.client.fetch_order(order_id)

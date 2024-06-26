from .base import Broker
import ccxt


class CoinbaseBroker(Broker):
    def __init__(self, api_key, api_secret):
        super().__init__(api_key, api_secret)
        self.client = ccxt.coinbase(
            {
                "apiKey": api_key,
                "secret": api_secret,
            }
        )

    def get_account_balance(self):
        return self.client.fetch_balance()

    def get_orderbook_data(self, symbol):
        order_book = self.client.fetch_order_book(symbol)
        return {
            "best_bid": (
                order_book["bids"][0][0] if len(order_book["bids"]) > 0 else None
            ),
            "best_bid_size": (
                order_book["bids"][0][1] if len(order_book["bids"]) > 0 else None
            ),
            "best_offer": (
                order_book["asks"][0][0] if len(order_book["asks"]) > 0 else None
            ),
            "best_offer_size": (
                order_book["asks"][0][1] if len(order_book["asks"]) > 0 else None
            ),
        }

    def place_order(self, symbol, order_type, amount, side, price=None):
        if order_type == "limit":
            order = self.client.create_limit_order(symbol, side, amount, price)
        else:
            order = self.client.create_market_order(symbol, side, amount)
        return order

    def cancel_order(self, order_id):
        return self.client.cancel_order(order_id)

    def get_order_status(self, order_id):
        return self.client.fetch_order(order_id)

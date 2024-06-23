import json
import threading
from coinbase.rest import RESTClient

class Broker:
    def __init__(self, api_key, api_secret):
        self.client = RESTClient(api_key, api_secret)
        self.lock = threading.Lock()

    def get_accounts(self):
        response = self.client.get_accounts()
        return response['accounts']

    def get_account_balance(self):
        accounts = self.get_accounts()
        balance = {account['currency']: float(account['available_balance']['value']) for account in accounts}
        return balance

    def place_order(self, product_id, side, size, order_type='market'):
        order_config = {
            "market_market_ioc": {
                "base_size": str(size)
            }
        }
        response = self.client.create_order(
            product_id=product_id,
            side=side,
            order_configuration=order_config
        )
        return response

    def execute_order(self, symbol, quantity, side):
        with self.lock:
            order = self.place_order(product_id=symbol, side=side, size=quantity)
            return order

    def get_positions(self):
        with self.lock:
            accounts = self.get_accounts()
            positions = {account['currency']: float(account['available_balance']['value']) for account in accounts}
            return positions

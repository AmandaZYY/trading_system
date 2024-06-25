class Broker:
    def __init__(self, api_key, api_secret, api_passphrase):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase

    def get_account_balance(self):
        raise NotImplementedError

    def place_order(self, symbol, quantity, side):
        raise NotImplementedError

    def cancel_order(self, order_id):
        raise NotImplementedError

    def get_order_status(self, order_id):
        raise NotImplementedError

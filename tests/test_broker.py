import json
import threading
import sys

sys.path.append("..")
from broker import Broker

def test_broker():
    # Replace with your actual API keys

    broker = Broker(api_key, api_secret)
    
    # Get account balance
    balance = broker.get_account_balance()
    print("Account Balance:", balance)

    # Place an order
    order = broker.execute_order('BTC-USD', 0.01, 'buy')
    print("Order Response:", order)

if __name__ == "__main__":
    test_broker()

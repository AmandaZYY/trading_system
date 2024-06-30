import time
from broker import Broker

## inspired by https://qoppac.blogspot.com/2014/10/the-worlds-simplest-execution-algorithim.html

## TO-DO: 1. might change to async when placing orders; 2. curreney limit


class ExecutionAlgo:
    def __init__(self, broker: Broker, symbol):
        self.broker = broker
        self.symbol = symbol
        self.passive_time_limit = 5 * 60  # 5 minutes
        self.total_time_limit = 10 * 60  # 10 minutes
        self.max_imbalance = 5.0

    def execute_trade(self, trade):
        start_time = time.time()
        mode = "Passive"
        order_id = None

        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time
            bookdata = self.broker.get_orderbook_data(self.symbol)

            if mode == "Passive":
                if (
                    (elapsed_time > self.passive_time_limit)
                    or self.is_adverse_price_move(trade, bookdata)
                    or self.is_order_imbalance(trade, bookdata)
                ):
                    mode = self.switch_to_aggressive(order_id)
                else:
                    order_id = self.place_limit_order(trade, bookdata)

            elif mode == "Aggressive":
                if elapsed_time > self.total_time_limit:
                    self.broker.cancel_order(order_id)
                    break
                elif self.is_further_adverse_price_move(trade, bookdata):
                    order_id = self.update_limit_order(trade, order_id, bookdata)

            time.sleep(1)  # Sleep for a while before the next tick

    def place_limit_order(self, trade, bookdata):
        # TO-DO: handle missing data
        limit_price = (
            bookdata["best_bid"] if trade > 0 else bookdata["best_offer"]
        )
        side = "buy" if trade > 0 else "sell"
        amount = abs(trade)
        order = self.broker.place_order(self.symbol, "limit", side, amount, limit_price)
        return order["id"]

    def switch_to_aggressive(self, order_id):
        print(f"Switching to aggressive mode for order {order_id}")
        return "Aggressive"

    def update_limit_order(self, trade, order_id, bookdata):
        new_limit_price = (
            bookdata["best_offer"] if trade > 0 else bookdata["best_bid"]
        )
        side = "buy" if trade > 0 else "sell"
        amount = abs(trade)
        self.broker.cancel_order(order_id)
        order = self.broker.place_order(
            self.symbol, "limit", side, amount, new_limit_price
        )
        return order["id"]

    def is_adverse_price_move(self, trade, bookdata):
        if trade > 0:
            return bookdata["best_offer"] > bookdata["best_bid"]
        else:
            return bookdata["best_bid"] < bookdata["best_offer"]

    def is_further_adverse_price_move(self, trade, bookdata):
        if trade > 0:
            return bookdata["best_offer"] > bookdata["best_bid"]
        else:
            return bookdata["best_bid"] < bookdata["best_offer"]

    def is_order_imbalance(self, trade, bookdata):
        if trade > 0:
            return (
                bookdata["best_offer_size"] / bookdata["best_bid_size"]
                > self.max_imbalance
            )
        else:
            return (
                bookdata["best_bid_size"] / bookdata["best_offer_size"]
                > self.max_imbalance
            )

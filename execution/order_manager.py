from collections import deque


class OrderManager:
    def __init__(self, broker):
        self.broker = broker
        self.order_queue = deque()
        self.executed_orders = []
        self.positions = {}

    def place_order(self, symbol, quantity, side, price=None):
        order = {"symbol": symbol, "quantity": quantity, "side": side, "price": price}
        self.order_queue.append(order)
        return order

    def process_orders(self):
        processed_orders = []
        while self.order_queue:
            order = self.order_queue.popleft()
            processed_orders.append(order)
        self.order_queue = deque(processed_orders)

    def update_positions(self, order):
        symbol = order["symbol"]
        side = order["side"]
        quantity = order["quantity"]
        if side == "buy":
            if symbol in self.positions:
                self.positions[symbol] += quantity
            else:
                self.positions[symbol] = quantity
        elif side == "sell":
            if symbol in self.positions:
                self.positions[symbol] -= quantity

    def get_positions(self):
        return self.positions

    def get_executed_orders(self):
        return self.executed_orders

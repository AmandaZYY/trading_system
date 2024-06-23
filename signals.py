class Signals:
    def __init__(self):
        pass

    def generate_signal(self, market_data):
        signals = {}
        for symbol, data in market_data.items():
            if data > 100:  # simple logic for demonstration
                signals[symbol] = 10
            else:
                signals[symbol] = -10
        return signals
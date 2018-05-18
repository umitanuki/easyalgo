
class Account(object):
    def __init__(self, cash):
        self.cash = cash
        self.positions = {}
        self.trades = []
        self.equities = {}

    @property
    def balance_hist(self):
        timestamps = sorted([t for t in self.equities.keys()])
        data = [self.equities[t] for t in timestamps]
        series = pd.Series(data, index=timestamps)
        return series

    def update(self, prices, timestamp):
        equity = self.cash
        for symbol, pos in self.positions.items():
            shares = pos['shares']
            price = prices[symbol].close.values[-1]
            equity += shares * price
        self.equities[timestamp] = equity

    def fill_order(self, order, price, timestamp, size):
        symbol = order['symbol']
        if order['side'] == 'buy':
            if self.cash < price:
                print(f'{timestamp}: no cash available for {symbol}')
                return
            if size < price:
                print(f'{timestamp}: skip {symbol}')
                return
            shares = size // price
            self.positions[symbol] = {
                'entry_timestamp': timestamp,
                'entry_price': price,
                'shares': shares,
            }
            self.cash -= price * shares
        else:
            position = self.positions.pop(symbol)
            shares = position['shares']
            self.trades.append({
                'symbol': symbol,
                'entry_timestamp': position['entry_timestamp'],
                'entry_price': position['entry_price'],
                'exit_timestamp': timestamp,
                'exit_price': price,
                'profit': price - position['entry_price'],
                'profit_perc': (price - position['entry_price']) / position['entry_price'] * 100,
                'shares': shares,
            })
            self.cash += price * shares

def dry_run(dfs, days=10, equity=500, position_dollar=100):
    # dfs = prices(Universe)
    account = Account(cash=equity)
    orders = []
    tindex = dfs['AAPL'].index
    for t in tindex[-days-1:-1]:
        snapshot = {symbol: df[:t] for symbol, df in dfs.items()}
        for order in orders:
            price = snapshot[order['symbol']].open.values[-1]
            account.fill_order(order, price, t, position_dollar)

        account.update(snapshot, t)
        print(len(account.positions))
        to_buy = set()
        to_sell = set()
        orders.clear()
        cands = calc(snapshot)
        total = len(cands)
        # cands = [c for c in cands if c[1] > -0.2]
        for symbol, _ in cands[:len(cands) // 20]:
            price = snapshot[symbol].close.values[-1]
            if price > equity:
                continue
            to_buy.add(symbol)
        for symbol, pos in account.positions.items():
            if symbol in to_buy:
                to_buy.remove(symbol)
                continue
            else:
                to_sell.add(symbol)
        orders = []
        for symbol in to_sell:
            orders.append({
                'side': 'sell',
                'symbol': symbol,
            })
        for symbol in to_buy:
            orders.append({
                'side': 'buy',
                'symbol': symbol,
            })
    return account

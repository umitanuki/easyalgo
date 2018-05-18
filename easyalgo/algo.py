import alpaca_trade_api as tradeapi
import pandas as pd
import time

from .universe import Universe

api = tradeapi.REST()


def prices(symbols):
    dfs = {}
    result = api.list_bars(symbols, '1D')
    for asset_bar in result:
        symbol = asset_bar.symbol
        bars = asset_bar.bars
        index = []
        d = {
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': [],
        }
        for bar in bars:
            index.append(pd.Timestamp(bar.time))
            d['open'].append(float(bar.open))
            d['high'].append(float(bar.high))
            d['low'].append(float(bar.low))
            d['close'].append(float(bar.close))
            d['volume'].append(int(bar.volume))
        dfs[symbol] = pd.DataFrame(d, index=index)
    return dfs

def calc(dfs, dayindex=-1):
    diffs = {}
    param = 10
    for symbol, df in dfs.items():
        if len(df.close.values) <= param:
            continue
        ema = df.close.ewm(span=param).mean()[dayindex]
        last = df.close.values[dayindex]
        diff = (last - ema) / last
        diffs[symbol] = diff

    return sorted(diffs.items(), key=lambda x: x[1])


def run_logic(position_size=100):
    positions = api.list_positions()
    dfs = prices(Universe)
    cands = calc(dfs)
    to_buy = set()
    to_sell = set()
    print(positions)
    account = api.get_account()
    for symbol, _ in cands[:len(cands) // 20]:
        price = float(dfs[symbol].close.values[-1])
        if price > float(account.cash):
            continue
        to_buy.add(symbol)
    holdings = {p.symbol: p for p in positions}
    holding_symbol = set(holdings.keys())
    to_sell = holding_symbol - to_buy
    to_buy = to_buy - holding_symbol
    for symbol in to_sell:
        shares = holdings[symbol].qty
        print(f'selling {symbol} for {shares}')
        try:
            api.submit_order(symbol=symbol, qty=shares, type='market', side='sell', time_in_force='day')
        except Exception as e:
            print(e)
    for symbol in to_buy:
        shares = position_size // float(dfs[symbol].close.values[-1])
        if shares == 0.0:
            continue
        print(f'buying {symbol} for {shares}')
        try:
            api.submit_order(symbol=symbol, qty=shares, type='market', side='buy', time_in_force='day')
        except Exception as e:
            print(e)

    orders = api.list_orders()
    print(orders)

def main():
    while True:
        if now.strftime('%H:%M') == '15:30':
            run_logic()
        time.sleep(1)


if __name__ == '__main__':
    main()

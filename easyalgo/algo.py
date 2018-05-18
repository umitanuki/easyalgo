import alpaca_trade_api as tradeapi
import pandas as pd
import time
import logging

from .universe import Universe

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

NY = 'America/New_York'
api = tradeapi.REST()

def _dry_run_submit(*args, **kwargs):
    logging.info(f'submit({args}, {kwargs})')
# api.submit_order =_dry_run_submit

def prices(symbols):
    dfs = {}
    now = pd.Timestamp.now(tz=NY)
    end_dt = now
    if now.time() >= pd.Timestamp('09:30', tz=NY).time():
        end_dt = now - pd.Timedelta(now.strftime('%H:%M:%S')) - pd.Timedelta('1 minute')
    result = api.list_bars(symbols, '1D', end_dt=end_dt.isoformat())
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


def get_orders(position_size=100):
    positions = api.list_positions()
    dfs = prices(Universe)
    cands = calc(dfs)
    to_buy = set()
    to_sell = set()
    logger.info(positions)
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
    orders = []
    for symbol in to_sell:
        shares = holdings[symbol].qty
        orders.append({
            'symbol': symbol,
            'qty': shares,
            'side': 'sell',
        })
        logger.info(f'order(sell): {symbol} for {shares}')
        # try:
        #     api.submit_order(symbol=symbol, qty=shares, type='market', side='sell', time_in_force='day')
        # except Exception as e:
        #     print(e)
    for symbol in to_buy:
        shares = position_size // float(dfs[symbol].close.values[-1])
        if shares == 0.0:
            continue
        orders.append({
            'symbol': symbol,
            'qty': shares,
            'side': 'buy',
        })
        logger.info(f'order(buy): {symbol} for {shares}')
        # try:
        #     api.submit_order(symbol=symbol, qty=shares, type='market', side='buy', time_in_force='day')
        # except Exception as e:
        #     print(e)
    return orders

    # orders = api.list_orders()
    # print(orders)

def trade(orders, wait=30):
    sells = [o for o in orders if o['side'] == 'sell']
    for order in sells:
        try:
            logger.info(f'trade(sell): {order}')
            api.submit_order(
                symbol=order['symbol'],
                qty=order['qty'],
                side='sell',
                type='market',
                time_in_force='day',
            )
        except Exception as e:
            logger.error(e)
    count = wait
    while count > 0:
        pending = api.list_orders()
        if len(pending) == 0:
            logger.info(f'all sell orders done')
            break
        logger.info(f'{len(pending)} sell orders pending...')
        time.sleep(1)
        count -= 1
    buys = [o for o in orders if o['side'] == 'buy']
    for order in buys:
        try:
            logger.info(f'trade(buy): {order}')
            api.submit_order(
                symbol=order['symbol'],
                qty=order['qty'],
                side='buy',
                type='market',
                time_in_force='day',
            )
        except Exception as e:
            logger.error(e)
    count = wait
    while count > 0:
        pending = api.list_orders()
        if len(pending) == 0:
            logger.info(f'all buy orders done')
            break
        logger.info(f'{len(pending)} buy orders pending...')
        time.sleep(1)
        count -= 1

def main():
    done = None
    logging.info('start running')
    while True:
        now = pd.Timestamp.now(tz=NY)
        if 0 <= now.dayofweek <= 4 and done != now.strftime('%Y-%m-%d'):
            if now.time() >= pd.Timestamp('09:30', tz=NY).time():
                orders = get_orders()
                trade(orders)
                done = now.strftime('%Y-%m-%d')
                logger.info(f'done for {done}')

        time.sleep(1)


if __name__ == '__main__':
    main()

from curses import noecho
from loguru import logger
from datetime import datetime
from urllib.parse import non_hierarchical
from ib_insync import * 
import random
import math
import pytz
import ntplib
import time
import tests
import ccxt
import sqlalchemy
import pandas as pd
import numpy as np
from os.path import exists
import time
import statistics 
import pandas as pd

bar_size_lookback = '1m'
duration_lookback = 1000
exchange_id = 'binanceusdm'
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class({

    'apiKey': 'rSw15gwfuc3USRqLBi4mm5qQ5CP5HfdElUXN9tHC6222j64FpHtZ0T7E8MKncFC9',
    'secret': 'EqIuEQSJOI8CP4Ge5XhxzQInZ4KTRDRVSOmMspl9guUZGyxVISwZ4YsaWfJrD0kA',
    'enableRateLimit': True,
})

username = 'postgres'  # DB username
password = 'proddb123'  # DB password
host = '34.84.190.250'  # Public IP address for your instance
port = '5432'
database = 'prod'  # Name of database ('postgres' by default)

db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
    username, password, host, port, database)

engine = sqlalchemy.create_engine(db_url)
cloud = engine.connect()

market_info = exchange.loadMarkets()
step_sizes = {}

sleep_time = 1
market_after_idle = False
market_idle_time = 30

use_limit_orders = True
bnb_price_update = exchange.fetchTicker('BNBUSDT')  # APi call on startup

bnb_price = exchange.fetchTicker('BNBUSDT')['last']

# setting up logger
logger.add(
    "orders.log",
    level="INFO",
    format="{time} {level} {message}",
    rotation="00:00",  # each day at 00:00 we create a new log file
    compression="zip",  # archive old log files to save space
    retention="3 days",  # delete logs after 3 days
    serialize=True,  # json format of logs
)

# general script operational functions

def get_step_size(ticker):
    if '/' not in str(ticker): 
        ticker = str(ticker)[:-4] + '/' + str(ticker)[-4:]
    return market_info[ticker]['limits']['amount']['min']

def round_to_step_size(ticker, qty):  # add round function in here
    if qty < 0: 
        neg_flag = True
        qty = abs(qty)
    else: 
        neg_flag = False
    step_size = get_step_size(ticker)
    decimal_places = str(step_size)[::-1].find('.')
    qty_adj = qty*10**decimal_places
    step_size_adj = step_size*10**decimal_places
    diff = qty_adj % step_size_adj
    diff = diff/(10**decimal_places)
    if diff > step_size/2: 
        res = qty + (qty - diff + step_size)
    else: 
        res = qty - diff
    if neg_flag:
        res *= -1
    return round(res, decimal_places) 


def market_order(ticker, buy_sell, quantity, reduce_only=False):
    if reduce_only:
        if buy_sell == 'buy':
            trade = exchange.create_order(ticker, 'market', 'buy', quantity)
        elif buy_sell == 'sell':
            trade = exchange.create_order(ticker, 'market', 'sell', quantity)
    if buy_sell == 'buy':
        trade = exchange.create_order(ticker, 'market', 'buy', quantity)
    elif buy_sell == 'sell':
        trade = exchange.create_order(ticker, 'market', 'sell', quantity)

    return trade

def limit_order(ticker, buy_sell, quantity):
    if buy_sell == 'buy':
            trade = exchange.create_order(ticker, 'limit', 'buy', quantity)
    elif buy_sell == 'sell':
            trade = exchange.create_order(ticker, 'limit', 'sell', quantity)

def get_final_positions():
    end_positions = {}
    res = cloud.execute("SELECT * FROM positions").all()
    current_time = int(time.time()*1000)

    for row in res: 
        quantities = {}
        # checks to ensure our script has updated once in the last 10 minutes, if not it will close the position
        heartbeat_time = row[2]
        if heartbeat_time: 
            heartbeat_time = float(heartbeat_time)
        else: 
            heartbeat_time = 0 
            
        if (heartbeat_time + 600000) < current_time:
            continue
        else: 
            pass
        
        t = cloud.execute("SELECT ticker1,ticker2,ticker3,ticker4,ticker5,ticker6,ticker7,ticker8,ticker9,ticker10,ticker11,ticker12 FROM tickers WHERE pair_number ={pair_number}").fetchall()
        positions = cloud.execute("SELECT ticker1,ticker2,ticker3,ticker4,ticker5,ticker6,ticker7,ticker8,ticker9,ticker10,ticker11,ticker12 FROM positions WHERE pair_number ={pair_number}").fetchall()
        for i in range(len(t)): 
            if t[i] == 'None':
                break

            quantities[t] = positions[i]


        for ticker in quantities: 
            if quantities[ticker] != 0:
                if ticker in end_positions: 
                    end_positions[ticker] += quantities[ticker]
                else: 
                    end_positions[ticker] = quantities[ticker]

    return end_positions

def get_current_positions():
    current_positions = {}
    exchange_data = exchange.fetch_positions()

    for a in exchange_data:
        if float(a['info']['positionAmt']) != 0:
            t = a['info']['symbol']
            amt = float(a['info']['positionAmt'])
            if t in current_positions: 
                current_positions[t] += amt
            else: 
                current_positions[t] = amt

    return current_positions


def compare_positions(final, current):
    changes = {}

    # We pull off the final dictionary, need to look at current to see what we have that should not be there as well.
    for key in final.keys(): 
        if key in current.keys():
            changes[key] = final[key] - current[key]  # ***this should be right but we should check it
        else:
            changes[key] = final[key]

    for key in current.keys(): 
        if key in changes:
            pass
        elif key not in final.keys():
            changes[key] = -1*current[key]

    keys = list(changes.keys())
    for key in keys:  # *** need to check that this works properly.
        adj_key = key[:-4] + '/' + key[-4:]
        if abs(changes[key]) < market_info[adj_key]['limits']['amount']['min']:
            del changes[key]

    # getting rid of anything in current but not final

    return changes

# logging functions

def log_trade(pair_number, ticker, quantity, buy_sell, price, trade_id, timestamp, status, internal_id):
    # need trade
    quantity = round_to_step_size(ticker, abs(quantity)) 
    price = round(float(price), 6)
    fee = 0 
    trades = exchange.fetchMyTrades(ticker, limit=5)

    for trade in trades: 
        if int(trade['order']) == int(trade_id): 
            fee_multiplier = quantity/float(trade['amount'])
            fees = trade['fee']
            if fees['currency'] == 'BNB':
                fee = fees['cost']*bnb_price * fee_multiplier
            elif fees['currency'] == 'USDT':
                fee = fees * fee_multiplier 
    
    logger.info(f'The current status is {status} and the current pair number is {pair_number} with price {price}')
    if status == 'OPEN':

        logger.info('Logging price for stoploss purposes')

        res = cloud.execute(f"SELECT ticker1, ticker2 FROM positions WHERE pair_number={str(pair_number)}").fetchall()[0]

        ticker1 = res[0]
        ticker2 = res[1]

        if ticker == ticker1:
            cloud.execute(f"UPDATE positions SET ticker1_latest_price={price} WHERE pair_number={pair_number}")
            logger.debug(f"ticker1_latest_price set to {price} for pair number {pair_number}")
        elif ticker == ticker2:
            cloud.execute("UPDATE positions SET ticker2_latest_price=" + str(price) + " WHERE pair_number=" + str(pair_number))
            logger.debug(f"ticker2_latest_price set to {price} for pair number {pair_number}")

    elif status == 'CLOSE':
            cloud.execute("UPDATE positions SET ticker1_latest_price=0, ticker2_latest_price=0 WHERE pair_number=" + str(pair_number))
   
    fee = round(fee, 6)

    cloud.execute(f"INSERT INTO trades (pair_number, internal_id, trade_id, timestamp, ticker, buy_sell, price,"
                  f" quantity, fees, status) VALUES ({pair_number},{internal_id},'{trade_id}',{timestamp},"
                  f"'{ticker}','{buy_sell}',{price},{quantity},{fee},'{status}')")
    logger.info('Trade inserted in to trade table.')
    logger.info('Trade Logged!')

def process_internal_orders(): 

    # Perhaps check for the same trade, pair number, and ticker first
    # then check for same pair and ticker
    # then check for the same ticker

    queue = cloud.execute("SELECT * FROM trade_data_queue").all()
    tickers = []

    for row in queue:
        ticker = row[1]
        tickers.append(ticker)

    tickers = list(set(tickers))

    for tick in tickers: 
        data = cloud.execute(f"SELECT * FROM trade_data_queue WHERE ticker='{tick}' ORDER "
                             f"BY shares_remaining DESC").all()
 
        if data[0][2] and data[-1][2]:  # how do we deal with pairs that have the same pair number?
            while float(data[0][2]) > 0 and float(data[-1][2]) < 0: 
                high = data[0] 
                low = data[-1]
                
                price = float(exchange.fetchTicker(tick)['info']['lastPrice'])
                
                # set all values, then get them from data
                pair_number_low, pair_number_high = int(low[0]), int(high[0])
                ticker_low, ticker_high = low[1], high[1]
                quantity_low, quantity_high = float(low[6]), float(high[6])
                timestamp_low, timestamp_high = int(low[7]), int(high[7])
                status_low, status_high = low[3], high[3]
                internalid_low, internalid_high = low[8], high[8]

                quantity_high = round_to_step_size(ticker_high, quantity_high)
                quantity_low = round_to_step_size(ticker_low, quantity_low)

                if pair_number_low == pair_number_high and internalid_high == internalid_low: 
                    trade_id = 999996  # 'SAME_TRADE_INTERNAL'
                elif pair_number_low == pair_number_high: 
                    trade_id = 999997  # 'SAME_PAIR_INTERNAL'
                else: 
                    trade_id = 999998  # 'INTERNAL'
                
                # The high will always be positive, and the low will always be negative!
                if abs(quantity_high) > abs(quantity_low):

                    delta = abs(quantity_high) - abs(quantity_low)
                    delta = round_to_step_size(tick, delta)
                    
                    log_trade(pair_number_high, ticker_high, quantity_low, 'buy',
                              price, trade_id, timestamp_high, status_high, internalid_high)
                    log_trade(pair_number_low, ticker_low, quantity_low, 'sell',
                              price, trade_id, timestamp_low, status_low, internalid_low)

                    cloud.execute(f"DELETE FROM trade_data_queue WHERE ticker='{tick}' AND timestamp={timestamp_low}")
                    cloud.execute(f"UPDATE trade_data_queue SET shares_remaining={delta} WHERE ticker='{tick}'"
                                  f" AND timestamp={timestamp_high}")
                    
                    logger.info(f'Deleting from trade_data_queue where ticker={tick} and timestamp={timestamp_low}')
                    logger.info(f'Updating from trade_data_queue. Setting shares remaining to {delta} where ticker={tick} and timestamp={timestamp_high}')

                    
                elif abs(quantity_low) > abs(quantity_high):
                        
                    delta = abs(quantity_low) - abs(quantity_high)
                    delta = round_to_step_size(tick, delta)

                    remaining = delta*-1 
                    remaining = round_to_step_size(tick, remaining)

                    log_trade(pair_number_high, ticker_high, quantity_high, 'buy',
                              price, trade_id, timestamp_high, status_high, internalid_high)
                    log_trade(pair_number_low, ticker_low, quantity_high, 'sell',
                              price, trade_id, timestamp_low, status_low, internalid_low)

                    cloud.execute(f"DELETE FROM trade_data_queue WHERE ticker='{tick}' AND timestamp={timestamp_high}")
                    cloud.execute(f"UPDATE trade_data_queue SET shares_remaining={remaining}"
                                  f" WHERE ticker='{tick}' AND timestamp={timestamp_low}")

                    logger.info(f'Deleting from trade_data_queue where ticker={tick} and timestamp={timestamp_high}')
                    logger.info(f'Updating from trade_data_queue. Setting shares remaining to {remaining} where ticker={tick} and timestamp={timestamp_low}')

                elif abs(quantity_low) == abs(quantity_high):

                    log_trade(pair_number_high, ticker_high, quantity_high, 'buy',
                              price, trade_id, timestamp_high, status_high, internalid_high)
                    log_trade(pair_number_low, ticker_low, quantity_low, 'sell',
                              price, trade_id, timestamp_low, status_low, internalid_low)

                    cloud.execute(f"DELETE FROM trade_data_queue WHERE ticker='{tick}' AND timestamp={timestamp_high}")
                    cloud.execute(f"DELETE FROM trade_data_queue WHERE ticker='{tick}' AND timestamp={timestamp_low}")

                    logger.info(f"DELETE FROM trade_data_queue WHERE ticker='{tick}' AND timestamp={timestamp_low}")
                    logger.info(f"DELETE FROM trade_data_queue WHERE ticker='{tick}' AND timestamp={timestamp_high}")

                data = cloud.execute(f"SELECT * FROM trade_data_queue WHERE ticker='{tick}' ORDER BY quantity DESC").all()
                
                if len(data) < 2:
                    break

def process_order(trade):  # check order matching, something is wrong here. Issues with avg fill price and others

    time, ticker1, buy_sell = trade['info']['updateTime'], trade['info']['symbol'], trade['side']
    quantity = float(trade['info']['executedQty'])
    price = float(trade['average'])
    trade_id = float(trade['id'])
    current_volume = quantity  # this represents a huge possible bug. are these the same sign?

    if buy_sell == 'sell': 
        res = cloud.execute(f"SELECT * FROM trade_data_queue WHERE ticker='{ticker1}' AND quantity < 0 ORDER BY timestamp ASC").all()
    elif buy_sell == 'buy': 
        res = cloud.execute(f"SELECT * FROM trade_data_queue WHERE ticker='{ticker1}' AND quantity > 0 ORDER BY timestamp ASC").all()
    
    for row in res:  
        pair_number, ticker, status, = int(row[0]), row[1], row[3]
        shares_remaining, timestamp, internal_id = abs(float(row[6])), int(row[7]), int(row[8])
        
        if shares_remaining == current_volume: 

            log_trade(pair_number, ticker1, shares_remaining, buy_sell, price, trade_id, time, status, internal_id)
            cloud.execute(f"DELETE from trade_data_queue WHERE timestamp={timestamp} AND internal_id={internal_id} AND ticker='{ticker1}'")  
            
            logger.info(f'Deleting from trade_data_queue where timestamp={timestamp}, internal_id={internal_id}, and ticker={ticker1}')

            current_volume = 0
            break 

        elif shares_remaining > current_volume: 
            if buy_sell.upper() == 'SELL':
                new_qty = (shares_remaining - current_volume)*-1
            elif buy_sell.upper() == 'BUY':
                new_qty = shares_remaining - current_volume
            
            new_qty = round_to_step_size(ticker, new_qty)

            log_trade(pair_number, ticker1, current_volume, buy_sell, price, trade_id, time, status, internal_id)
            cloud.execute(f"UPDATE trade_data_queue SET shares_remaining ={new_qty} WHERE internal_id={internal_id} AND ticker='{ticker1}'")  

            logger.info(f'Updating trade_data_queue setting shares_remaining={new_qty} where internal_id={internal_id}, and ticker={ticker1}')

            current_volume = 0
            break

        elif shares_remaining < current_volume: 

            current_volume = current_volume-shares_remaining

            log_trade(pair_number, ticker1, shares_remaining, buy_sell, price, trade_id, time, status, internal_id)
            cloud.execute(f"DELETE from trade_data_queue WHERE timestamp={timestamp} AND internal_id={internal_id} AND ticker='{ticker1}'")

            logger.info(f'Deleting from trade_data_queue where timestamp={timestamp}, internal_id={internal_id}, and ticker={ticker1}')

        current_volume = round_to_step_size(ticker1, current_volume)
        if current_volume == 0:
            break
    if current_volume != 0: 
        status = 'NONE'
        logger.error(f"Unable to match the trade. Logging as an error! Label: 900005 Trade ID: {trade_id} Ticker: {ticker1} Time: {time} Buy/Sell: {buy_sell} Quantity: {quantity} Price: {price}")

        log_trade(0, ticker1, current_volume, buy_sell, price, trade_id, time, status, 900005)


# Functions to place orders

def place_market_orders(changes=None):
    for key in changes.keys():
        try:
            if changes[key] > 0: 
                buy_sell = 'buy'
            elif changes[key] < 0: 
                buy_sell = 'sell'

            # try: #if the first order fails, we try a second order that is reduce only.
            # If that fails, we ignore the position since adding a small amount is not that necessary.
            trade = market_order(key, buy_sell, abs(changes[key]))  # issue with orders being too small.

            logger.info(f'Market order submitted to {buy_sell} {changes[key]} shares of {key}!')
            process_order(trade)

        except Exception as e: 

            logger.error("There was an error with trade logging!")
            logger.error(e)


def check_orders(open_orders):
    remove = []
    for o in open_orders:
        try:
            order = exchange.fetchOrder(symbol=open_orders[o], id=o)  # does this need to be an integer or can it be a string?
            # order = exchange.fetchOrder(symbol=open_orders[o], id=int(o))
            stat = order['info']['status']
            if stat == "FILLED": 
                process_order(order)
                remove.append(o)

            elif stat == "CANCELED":
                remove.append(o)
                if float(order['info']['executedQty']) != 0:  # If there was a quantity exchanged, then log the trade
                    process_order(order)
                    logger.info(f"The quantity exchanged. An order is: {o}")
        except Exception as e:

            logger.error("There was an issue finding the order (possible it does not exist)!")
            logger.error(e)

    for r in remove: 
        del open_orders[r]
    return open_orders


def check_notational_value(bid_price, quantity):
    if abs(bid_price*quantity) < 5.5: 
        return True
    else:
        return False


def check_reduce_only(ticker, amount):
    curr_pos = float(exchange.fetch_positions(symbols=[ticker], params={})[0]['info']['positionAmt'])

    if curr_pos > 0 and amount < 0 and abs(amount) <= abs(curr_pos): 
        return True
    elif curr_pos < 0 and amount > 0 and abs(amount) <= abs(curr_pos):
        return True
    else: 
        return False


def submit_limit_order(bid_price, ask_price, ticker, amount, params): 
    trade, current_order = None, None 

    order = exchange.fetchOpenOrders(ticker)

    if len(order) == 1: 
        current_order = order
    elif len(order) > 1: 
        logger.info(f"Consolidating orders for {ticker}, there are {len(order)} orders currently outstanding!")
        order_quantity = consolidate_orders(ticker)
        
    
    if current_order:
        order = current_order[0]
        value = False

        if order['reduceOnly']:
            params = {'reduceOnly': True}
            
        if amount > 0: 
            
            if order['amount'] == abs(amount): 
                value = True

            if order['price'] == bid_price and value:
                pass
            else: 
                amount = round_to_step_size(ticker, amount)
                trade = exchange.edit_order(order['id'], ticker, 'limit', 'buy', amount, bid_price, params=params)

                logger.info(f"Editing order to buy {amount} {ticker} at {bid_price}. ID: {trade['info']['orderId']}"
                      f" Time:{int(time.time()*1000)}")

                open_orders[trade['info']['orderId']] = trade['info']['symbol']
        elif amount < 0:

            if order['amount'] == abs(amount): 
                value = True
            
            if order['price'] == ask_price and value:
                pass
            else: 
                amount = abs(amount)
                amount = round_to_step_size(ticker, amount)
                trade = exchange.edit_order(order['id'], ticker, 'limit', 'sell', abs(amount), ask_price, params=params)

                logger.info(f"Editing order to sell {amount} {ticker} at {ask_price}. ID: {trade['info']['orderId']}"
                      f" Time:{int(time.time()*1000)}")

                open_orders[trade['info']['orderId']] = trade['info']['symbol']
        
    else:
        if amount > 0: 
            amount = round_to_step_size(ticker, amount)

            trade = exchange.create_order(ticker, 'limit', 'buy', amount, bid_price, params=params)
            open_orders[trade['info']['orderId']] = trade['info']['symbol']
            
            logger.info(f"Created order to buy {amount} {ticker} at {bid_price}. ID: {trade['info']['orderId']}"
                  f" Time:{int(time.time()*1000)}")

        elif amount < 0:

            amount = abs(amount)
            amount = round_to_step_size(ticker, amount)
            trade = exchange.create_order(ticker, 'limit', 'sell', abs(amount), ask_price, params=params)
            open_orders[trade['info']['orderId']] = trade['info']['symbol']

            logger.info(f"Created order to sell {amount} {ticker} at {ask_price}. ID: {trade['info']['orderId']}"
                  f" Time:{int(time.time()*1000)}")

    return trade

def consolidate_orders(ticker): 

    orders = exchange.fetchOpenOrders(ticker)
    for order in orders:
        exchange.cancel_order(symbol=ticker, id=order['id'])

    return 0

def place_limit_orders(changes, final, open_orders):
    for change in changes:
        ticker = str(change)
        params = {}

        # get bid and ask
        orderbook = exchange.fetch_order_book(ticker, limit=5)
        bids = [item[0] for item in orderbook['bids']]
        asks = [item[0] for item in orderbook['asks']]
        bid_price, ask_price = bids[0], asks[0]
        
        # check for current balance of ticker
        if ticker in final: 
            final_quantity = float(final[ticker])
        else:
            final_quantity = 0

        curr_quantity = float(exchange.fetch_positions(symbols=[ticker], params={})[0]['info']['positionAmt'])
        
        amount = final_quantity - curr_quantity  # amount = final_quantity - (curr_quantity + order_quantity)
        amount = round_to_step_size(ticker, amount)
        params = {}
        retry_order = False

        try: 
            trade = submit_limit_order(bid_price, ask_price, ticker, amount, params)
            
        except ccxt.ExchangeError as e: 
            logger.error(e)
            logger.error('Likely a reduce only error, checking to see if we can set a reduce only order.')
            res = check_reduce_only(ticker, amount)

            if res == True: 
                logger.info('It is a reduce only issue with a small quantity, attempting to fix')
            elif res == False:
                logger.critical('The issue is not a a reduce only error, please check that the is that the leverage is not too high.')

            if res:
                params = {'reduceOnly': True}
                retry_order = True 
                
            # try to submit the order again

        except Exception as e: 
            logger.error(e)
            logger.error(f"There was an error with the order. Ticker: {ticker} Amount: {amount}")

        if retry_order:
            try: 
                trade = submit_limit_order(bid_price, ask_price, ticker, amount, params)
                #logger.info(f'Limit retry order submitted to {buy_sell} {changes[key]} shares of {key}!')

            except Exception as e: 
                logger.error(e)
                logger.error("Retry order failed!")

    return open_orders


def update_status_table():
    cloud.execute(f"UPDATE statuses SET orders={int(time.time()*1000)}")

def check_stops(): #market order sell anything that has a stop flag enabled
    res = cloud.execute("SELECT pair_number FROM positions WHERE stop_flag=True").fetchall()
    
    if len(res) == 0: 
        return 
    else: 
        changes = {}

        for r in res: 
            tickers = []
            quantities = {}

            pair_number = int(r[0])

            t = cloud.execute("SELECT ticker1,ticker2,ticker3,ticker4,ticker5,ticker6,ticker7,ticker8,ticker9,ticker10,ticker11,ticker12 FROM tickers WHERE pair_number ={pair_number}").fetchall()
            positions = cloud.execute("SELECT ticker1,ticker2,ticker3,ticker4,ticker5,ticker6,ticker7,ticker8,ticker9,ticker10,ticker11,ticker12 FROM positions WHERE pair_number ={pair_number}").fetchall()

            for i in range(len(t)): 
                if t[i] == 'None':
                    break

                changes[t[i]] += positions[i]

            logger.info(f'Stop triggered for pair {pair_number} with tickers {tickers}. Commencing market order liquidation process and setting quantities to zero!')

            for ticker in tickers: 
                cloud.execute(f"UPDATE positions SET ticker1=0,ticker2=0,ticker3=0,ticker4=0,ticker5=0,ticker6=0,ticker7=0,ticker8=0,ticker9=0,ticker10=0,ticker11=0,ticker12=0 stop_flag=False WHERE pair_number={pair_number}")

        place_market_orders(changes)
        logger.info(f'Stops successfully completed.')
            
def get_times():
    pass

def market_order_timeout(orders):

    second_timeout = 30
    time_delta = 1000*second_timeout
    for o in orders:
        position_time_delta = None
        if position_time_delta > time_delta:
            place_market_orders(changes)
        
    
if __name__ == '__main__':
    open_orders = {}

    while True: 
        check_stops() 

        final = get_final_positions()  # Make sure this is rounded properly
        current = get_current_positions()  # Make sure this is rounded properly
        changes = compare_positions(final, current)  # ensure that the rounding is not messed up through this process

        print(final)
        print(current)
        print(changes)

        if use_limit_orders:
            open_orders = place_limit_orders(changes, final, open_orders)
        else: 
            place_market_orders(changes) 

        # process orders
        open_orders = check_orders(open_orders)
        process_internal_orders()

        if market_after_idle == True: 
            times = get_times()
            orders = market_order_timeout(market_idle_time)
    
        # update status hearbeat
        update_status_table()
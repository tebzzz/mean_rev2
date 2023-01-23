from datetime import datetime
from ib_insync import * 
import random
import math
import pandas as pd
import pytz
import ntplib
import time
import ccxt
import ccxt.pro as ccxtpro
import sqlalchemy
import pandas as pd
import numpy as np
from os.path import exists
from pykalman import KalmanFilter
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import requests
from loguru import logger
import json
import concurrent.futures
#import equity_functions as equity

#print(pd.DataFrame(cloud.execute("SELECT t.relname, l.locktype, page, virtualtransaction, pid, mode, granted FROM pg_locks l, pg_stat_all_tables t WHERE l.relation = t.relid ORDER BY relation asc")))
#cloud.execute("SELECT pg_terminate_backend(141229)")

#development:4aX!m{rR[4OQY4q

# That's a notioursly tough question but I do think there are ways to detect some of those events early. Some ideas off top: detect & monitor anomalies in LOB, measure flow toxicity via VPIN or some proprietary method, find levels of concentrated forced liquidation, know when naive retail signals line up and agree on direction, monitor bigger markets to see if they're waggin the tail, use defi to find flow of communities(proxy for attention), etc

use_websockets = True
periods_per_day = 1
trade_status = None #Variable that tells us if we are opening or closing a trade. 
ohlcv = True

#signals
volume_signal = True
vol_notrade_multiple = 5

global_data = pd.DataFrame() 
position_ratios, current_state, last_pos = {} , None, None #Can be manually changed to change the state when restarting a script. 
internal_id = 100000

entry_sigma = 3 #the standard deviation multiplyer we are using for bollinger bands 
exit_sigma = 4
use_stoploss = False
total_order_value = 100 #the current value of our orders

lookback = 50
average_type = 'sma' #***this might be the cause of our problems 
band_type = 'bollinger'
bollinger_type = 'sma'
bollinger_vol_exp = 1

#stop settings
max_loss = -0.03
break_on_loss = True
spread_type = 'log'

#connects to CloudSQL postgres
username = 'postgres'  
password = 'proddb123'
host = '34.84.190.250' 
port = '5432'
database = 'prod' 

db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
    username, password, host, port, database)

engine = sqlalchemy.create_engine(db_url) #,connect_args={'options': '-c lock_timeout=3000 -c statement_timeout=5000'})
cloud = engine.connect()

#connecting to interactive brokers
num = round(random.random()*100)
gateway_port = 4002
ib = IB()
# ib.connect(host='127.0.0.1', port=gateway_port, clientId=num) 

#connecting to ntp server for time 
c = ntplib.NTPClient() 

#instantiating binance class object

log_name = 'rum'

logger.add(
    log_name,
    level="INFO",
    format="{time} {level} {message}",
    rotation="00:00",  # each day at 00:00 we create a new log file
    compression="zip",  # archive old log files to save space
    retention="3 days",  # delete logs after 30 days
    serialize=True,  # json format of logs
)

exchange_id = 'binanceusdm'
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class({
    'apiKey': 'rSw15gwfuc3USRqLBi4mm5qQ5CP5HfdElUXN9tHC6222j64FpHtZ0T7E8MKncFC9',
    'secret': 'EqIuEQSJOI8CP4Ge5XhxzQInZ4KTRDRVSOmMspl9guUZGyxVISwZ4YsaWfJrD0kA',
    'enableRateLimit': True,
})

# get all the arguments from config.json
def read_arguments_from_file(file_name):
    with open(file_name, 'r') as json_arguments:
        arguments = json.load(json_arguments)  # read all the arguments from file (from config.json for example)
        return arguments

# send arguments to the function and start it multiple times using threads
def create_threads(arguments, function_name):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(arguments)) as executor:
        executor.map(lambda f: function_name(**f), arguments)

#functions for equities (likely not complete)
def check_status_func(current_state, quantity_a, quantity_b): 
    if current_state == None: 
        if quantity_a == 0 and quantity_b == 0:
            return True
    elif current_state.lower() == 'long':
        if quantity_a > quantity_b:
            return True
    elif current_state.lower() == 'short': 
        if quantity_a < quantity_b:
            return True
    return False

#General Functions 
def delete_locks(table):
    #get all of the locks and select the ones we want to delete
    locks = cloud.execute("SELECT t.relname, l.locktype, page, virtualtransaction, pid, mode, granted FROM pg_locks l, pg_stat_all_tables t WHERE l.relation = t.relid ORDER BY relation asc")

    #delete them
    for lock in locks:
        if lock[0] == table:
            cloud.execute("SELECT pg_terminate_backend(" + str(lock[4]) + ")")

def get_current_state(data): 

    pair_number = data['pair_number']

    res = cloud.execute(f"SELECT current_status FROM current_data WHERE pair_number = {str(pair_number)}").one()[0]
    if res != None:
        res = res.lower()
    if res == 'none' or res == None:
        return None
    elif res == 'long' or res == 'short':
        return res
    else:
        logger.info('Status is not in the correct program, shutting down script')
        print(break123)

def round_list(list, decimals): #can only run lists of floating point numbers through this 
    res = []
    for i in range(len(list)):
        num = round(list[i], decimals)
        res.append(num)
    return res

def get_quantities(tickers, global_data, data, position_ratios):
    ticker_quantities = {}
    mkt_data = exchange.loadMarkets()

    step_sizes = {}
    min_qty = {}

    total_dollar_value = data['total_order_value']

    prices = {} #Figure out how to get prices from global_data
    for t in tickers: 
        prices[t] = global_data[f'{t}'].iloc[0] #is this the correct value? 

    total_val = 0

    for p in position_ratios:
        total_val += abs(position_ratios[p])*prices[p]

    multiplier = total_dollar_value/total_val #muliplier of the position ratios. 

    keys = list(position_ratios.keys())
    

    for key in keys: 
        ticker_quantities[key] = multiplier*position_ratios[key]

    #need to fix this, issue iwth the "/" in the ticker
    ut = format_tickers(tickers, reverse=True)
    
    #getting step sizes and minimum quantities 

    for i in range(len(tickers)): 
        step_sizes[tickers[i]] = float(mkt_data[ut[i]]['info']['filters'][1]['stepSize'])
        min_qty[tickers[i]] = mkt_data[ut[i]]['limits']['amount']['min']

    #ensure we are at the most recent step size and above the minimum
    for a in min_qty: 
        if abs(ticker_quantities[a]) < min_qty[a]:
            ticker_quantities[a] = min_qty[a]
        else:
            ticker_quantities[a] = ticker_quantities[a]-(ticker_quantities[a]%step_sizes[a])

    return ticker_quantities

def format_tickers(tickers, reverse=False):
    if reverse == True:
        formatted_tickers = []

        for t in tickers:
            if t[-5:] == '/USDT':
                formatted_tickers.append(t)
            elif t[-5:] == '/BUSD':
                formatted_tickers.append(t)
            elif t[-4:] == 'USDT':
                formatted_tickers.append( t[:-4] + '/USDT')
            elif t[-4:] == 'BUSD':
                formatted_tickers.append( t[:-4] + '/BUSD')
            else: 
                formatted_tickers.append(t)

        return formatted_tickers
    else: 
        formatted_tickers = []

        for t in tickers:
            if t[-5:] == '/USDT':
                formatted_tickers.append(t.split('/')[0] + 'USDT')
            elif t[-5:] == '/BUSD':
                formatted_tickers.append(t.split('/')[0] + 'BUSD')
            else: 
                formatted_tickers.append(t)

        return formatted_tickers

##pairs trading specific functions

def set_positions_database(internal_id, tickers, ticker_quantities, prior_state, current_state, data): #*** need some more work done here
    pair_number = data['pair_number']

    if internal_id == None: 
        internal_id = 300000

    heartbeat = int(time.time()*1000)

    res = cloud.execute("SELECT * FROM positions WHERE pair_number=" + str(pair_number)).all()[0] #positions
    res_tickers = cloud.execute("SELECT * FROM tickers WHERE pair_number=" + str(pair_number)).all()[0] #tickers
    
    current_positions = {}
    for i in range(len(tickers)):
        current_positions[tickers[i]] = float(res[4+i])

    final_quantities = {}

    #get quantities that we want to end up with 

    if current_state == None: 

        open_close = 'CLOSE'

        if prior_state == None: 
            
            for t in tickers: 
                final_quantities[t] = 0 

        elif prior_state.lower() == 'long':

            final_quantities = {}
            final_quantities = ticker_quantities

        elif prior_state.lower() == 'short':

            final_quantities = {}
            final_quantities = ticker_quantities*-1

    elif current_state.lower() == 'long':

        open_close = 'OPEN'
        final_quantities = ticker_quantities

    elif current_state.lower() == 'short':

        open_close = 'OPEN'
        final_quantities = ticker_quantities*-1

    if open_close == 'OPEN':
        internal_id = str(pair_number) + str(heartbeat)[-5:]

    for i in range(len(tickers)):
        col = 'ticker'+str(i)
        ticker_delta = final_quantities[tickers[i]] - current_positions[tickers[i]] 

        if ticker_delta == 0: #and ticker == ticker_curr_ticker: Keep this as a check to make sure we have the right ticker? 
            pass

        else: 
            if current_positions[tickers[i]] != final_quantities[tickers[i]]: 

                cloud.execute(f"INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values ({str(pair_number)},'{str(tickers[i])}',{str(ticker_delta)},'{open_close}', {str(ticker_delta)},{str(heartbeat)},{str(internal_id)})") 
                logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {tickers[i]} Ticker Delta: {ticker_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

            cloud.execute(f"UPDATE positions SET ticker={str(final_quantities[tickers[i]])} WHERE pair_number={str(pair_number)}")
            cloud.execute(f"UPDATE tickers SET {col}='{str(tickers[i])}', pos_change_time = {str(heartbeat)} WHERE pair_number={str(pair_number)}") #*** this likely needs to be adjusted 
            #*** set ticker position 
            if open_close == 'open':
                logger.info(f"Updating positions setting {tickers[i]} quantity to {final_quantities[tickers[i]]} under pair {pair_number} at timestamp {heartbeat}")
            if open_close == 'close':
                logger.info(f"Updating positions pair number {pair_number} {tickers[i]} positions set to zero at timestamp {heartbeat}")


    cloud.execute(f"UPDATE positions SET pos_change_time = {str(heartbeat)} WHERE pair_number={str(pair_number)}")

    return internal_id

def decide_positions(z, moving_avg, upper_band, lower_band, reset_flag, current_pos='short'):

    if current_pos == None:
        reset_flag = False
        if z > upper_band:
            return 'short', reset_flag, 'open'
        elif z < lower_band: 
            return 'long', reset_flag, 'open'
        else:
            return None, reset_flag, None

    elif current_pos.lower() == 'long':
        if z >= moving_avg:
            reset_flag = False
            return None, reset_flag, None
        elif z < moving_avg: 
            return 'long', reset_flag, None

    elif current_pos.lower() == 'short':
        if z <= moving_avg:
            reset_flag = False
            return None, reset_flag, None
        elif z > moving_avg:
            return 'short', reset_flag, None

#Mean Rev math specific functions exit
def get_spread(position_ratios, prices = None, global_data = None): 
 
    #get prices from global data
    if not prices: 
        prices = {}
        for t in position_ratios: 
            prices[t] = global_data[f'{t}']
    prices_df = pd.DataFrame(prices)
    coeff = pd.DataFrame(position_ratios, index=[0])
    #get spread over time (position ratios*prices)

    for ticker in prices_df.columns:
        prices_df[ticker] *= coeff[ticker].iloc[0] 

    spread = prices_df.sum(axis=1)

    return spread.tolist()

def get_bollinger_bands(zscore, sigma):
    zscore_df = pd.DataFrame(zscore)
    moving_avg = get_moving_avg(zscore)
    span = lookback
    if bollinger_type == 'ema':
        std = zscore_df.ewm(span).std()
    else: 
        std = zscore_df.rolling(lookback).std()
    bollinger_up = moving_avg + std**bollinger_vol_exp * sigma 
    bollinger_down = moving_avg - std**bollinger_vol_exp * sigma 

    return bollinger_up, bollinger_down

def get_bands(zscore, sigma=None):
    if sigma == None: 
        sigma = entry_sigma
    if band_type == 'bollinger':
        upper_band, lower_band = get_bollinger_bands(zscore, sigma)
    # elif band_type == 'keltner':
    #     upper_band, lower_band = get_keltner_channel(zscore) #will need additional information
    return upper_band, lower_band

def get_kalman_filter(zscore):

    res = []
    zscore = zscore[lookback*2-2:]

    kf = KalmanFilter(transition_matrices = [1],    # The value for At. It is a random walk so is set to 1.0
                  observation_matrices = [1],   # The value for Ht.
                  initial_state_mean = 0,       # Any initial value. It will converge to the true state value.
                  initial_state_covariance = 1, # Sigma value for the Qt in Equation (1) the Gaussian distribution
                  observation_covariance=1,     # Sigma value for the Rt in Equation (2) the Gaussian distribution
                  transition_covariance=.01)    # A small turbulence in the random walk parameter 1.0

# Get the Kalman smoothing
    state_means, _ = kf.filter(zscore)

    for num in state_means:
        res.append(num[0])

    return [0]*(lookback*2-2) + res 

def get_moving_avg(prices, override=None):
    prices_df = pd.DataFrame(prices)
    if not override: 
        type = average_type
    else: 
        type = override
    if type =='sma':
        ma_df = prices_df.rolling(lookback).mean()
    elif type == 'ema':
        span = lookback
        ma_df = prices_df.ewm(span).mean()
    elif type == 'kalman':
        ma_df = get_kalman_filter(prices)
        ma_df = pd.DataFrame(ma_df)
    return ma_df

def get_zscore(spread):
    spread_df = pd.DataFrame(spread)
    moving_avg_df = get_moving_avg(spread, override='sma')
    std_df = spread_df.rolling(lookback).std()
    zscore = []
    for i in range(len(spread)):
        spd = float(spread_df.iloc[i])
        mva = float(moving_avg_df.iloc[i])
        std1 = float(std_df.iloc[i])
        zscore.append((spd-mva)/std1)
    return zscore

def get_data(spread=None, global_data=None, position_ratios=None):
    if not spread:
        spread = get_spread(position_ratios, global_data=global_data)
    zscore = get_zscore(spread)
    band_up_df, band_down_df = get_bands(zscore) 
    z_moving_avg_df = get_moving_avg(zscore) #This can be easily changed to a kalman filter to implement within the code

    if math.isnan(z_moving_avg_df.iloc[-1]) == True: 
        logger.error('The duration we are looking back at is likely not longer than the lookback period! Fix to get correct functionality!')

    return zscore, band_up_df[0].tolist(), band_down_df[0].tolist(), z_moving_avg_df[0].tolist()
    
#Mean reversion specific functions 

def record_data(data, tickers, current_state, global_data): 

    pair_number = data['pair_number']
    bar_size = data['bar_size']
    curr_time = int(time.time()*1000)
    prices = {}

    #Get quant data

    print(global_data)
    
    latest_time = global_data['Time'].iloc[0] 
    current_z = global_data['Zscore'].iloc[0]
    current_average = global_data['Moving Average'].iloc[0]
    current_upper_band = global_data['Upper Band'].iloc[0]
    current_lower_band = global_data['Lower Band'].iloc[0]
    latest_spread = global_data['Spread'].iloc[0]

    for ticker in tickers: 
        prices[ticker] = global_data[f'{ticker}'].iloc[0]
    
    #latest_volume_a, latest_volume_b = global_data['Volume A'].iloc[0], global_data['Volume B'].iloc[0]

    #Log total_equity for that pair 

    res = cloud.execute(f"SELECT total_pl from current_data WHERE pair_number={str(pair_number)}")
    current_equity = res.one()[0]
    if current_equity == None:
        current_equity = 0

    table_name = 'pair_data_' + str(pair_number)

    #udpate pair table

    latest_recorded_time = int(cloud.execute("SELECT timestamp from " + table_name + " ORDER BY timestamp desc").all()[0][0])
    #issue here, global data and current time not occuring properly 
    if latest_recorded_time != latest_time: 
        cloud.execute(f"INSERT INTO {table_name} (timestamp, zscore, upper_band, lower_band ,moving_average, spread, equity) VALUES ({str(latest_time)},{str(current_z)},{str(current_upper_band) },{str(current_lower_band)},{str(current_average)},{str(latest_spread)},{str(current_equity)})")
    else:
        cloud.execute(f"UPDATE {table_name} SET zscore={str(current_z)},upper_band={str(current_upper_band)},lower_band={str(current_lower_band)},moving_average= {str(current_average)} ,spread={str(latest_spread)},equity= {str(current_equity)} WHERE timestamp={str(int(latest_recorded_time))}") #this needs to be edited to include prices

    #update current data

    tickers = [str(a).upper() for a in tickers]

    cloud.execute(f"UPDATE current_data SET bar_size = '{bar_size}', lookback = {str(lookback)}, current_z = {str(current_z)}, upper_band = {str(current_upper_band)}, lower_band = {str(current_lower_band)}, moving_average = {str(current_average)}, current_status = '{str(current_state)}',heartbeat = {str(curr_time)},spread={str(latest_spread)} WHERE pair_number = {str(pair_number)}")
    cloud.execute(f"UPDATE positions SET script_update_time = {str(curr_time)} WHERE pair_number={str(pair_number)}")

def check_exit_conditions(internal_id, global_data, tickers, ticker_quantities, current_status, data, percent_stop=False, totalpl_stop=False): #return true if the conditions pass, return false if no longer cointegrated or we are outside of standard deviation or spread parameters
    #internal id cannot be none
    pair_number = data['pair_number']
    bar_size = data['bar_size']

    total_order_value = data['total_order_value']
    max_loss = data['max_loss']
    break_on_loss = data['break_on_loss']

    permitted_loss = 0.05
    total_capital = total_order_value*2
    
    adj_tickers = format_tickers(tickers)

    if totalpl_stop == True:
        res = cloud.execute(f"SELECT total_pl FROM current_data where pair_number = {str(pair_number)}")
        pl = float(res.all()[0][0])

        if pl < total_capital*(permitted_loss)*-1:
            logger.info('We are below the stop loss for percent of capital lost. Closing position and shutting down script!')

            trade_status = None
            internal_id = set_positions_database(internal_id, tickers, 0,0,None, None,data,stop=True) #If there is an error check the prior state. 
            print(break123)

    #trade goes below a % loss
    if percent_stop == True:
        res = cloud.execute("SELECT * FROM prices WHERE pair_number=" + str(pair_number)).fetchall()[0]
        open_prices = {}

        if current_status == None or current_status == 'None':
            return

        #get open prices from the sql table
        for price in res:
            pass

        current_prices = {}
        for t in tickers:
            current_prices[t] = get_prices_crypto_websocket(t, bar_size, 1, ohclv=False, return_all_data=True)[0][4]

        if len(price) == 0: #if there are no values above zero. 
            return

        if current_status.lower() == 'long':
            current_trade_pl = 0
            for t in tickers: 
                current_trade_pl += ticker_quantities[t]*(current_prices[t]-price[t])
            
        elif current_status.lower() == 'short': 
            current_trade_pl = 0
            for t in tickers: 
                current_trade_pl += ticker_quantities[t]*(current_prices[t]-price[t])

    if current_trade_pl < total_capital*(max_loss): 
        logger.info('The current trade p/l is:' + str(current_trade_pl))
        logger.info('The total capital*max loss is:' + str(total_capital*(max_loss)))

        if break_on_loss == True:

            logger.info('We are below the stop loss for percent of capital lost, shutting down script!')
            open_close = 'CLOSE'

            #ticker deltas
            ticker_deltas = {}
            ticker1_delta = float(cloud.execute(f"SELECT ticker1_pos FROM positions WHERE pair_number={pair_number}").fetchone()[0])

            heartbeat = int(time.time()*1000)

            for ticker in tickers: 

                cloud.execute(f"INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values ({str(pair_number)},'{str(ticker)}',{str(ticker_deltas[ticker])},'{open_close}',{str(ticker_deltas[ticker])},{str(heartbeat)},{str(internal_id)})")
                logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker} Ticker Delta: {str(ticker_deltas[ticker])} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")
            
            cloud.execute(f"UPDATE current_data SET current_status='None' WHERE pair_number={pair_number}")
            cloud.execute(f"UPDATE positions SET stop_flag=True WHERE pair_number={str(pair_number)}")

            time.sleep(10)

            set_positions_database(internal_id, tickers, 0,0,None, None,data,stop=True)
            internal_id = 100000
            print(break123)

        elif break_on_loss == False: 
            reset_flag = True 
            logger.info('We hit the stop loss for this position, resetting. ')

            open_close = 'CLOSE'

            ticker_deltas = {}
            ticker1_delta = float(cloud.execute(f"SELECT ticker1_pos FROM positions WHERE pair_number={pair_number}").fetchone()[0])

            heartbeat = int(time.time()*1000)
            
            for ticker in tickers: 

                cloud.execute(f"INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values ({str(pair_number)},'{str(ticker)}',{str(ticker_deltas[ticker])},'{open_close}',{str(ticker_deltas[ticker])},{str(heartbeat)},{str(internal_id)})")
                logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker} Ticker Delta: {str(ticker_deltas[ticker])} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

            cloud.execute(f"UPDATE positions SET stop_flag=True WHERE pair_number={str(pair_number)}")
            cloud.execute(f"UPDATE current_data SET current_status='Stop' WHERE pair_number={pair_number}")

            time.sleep(10)

            set_positions_database(internal_id, tickers, 0,0,None, None,data,stop=True)
            internal_id = 100000

            return internal_id

#websocket specific functions 

def wait_for_tables(tickers, bar_size): #***this function needs to be tested
    tables = []
    tables_current = False

    for t in tickers: 
        table = (f'ohlcv_{str(t)}_{str(bar_size)}').lower()
        tables.append(table)

    tables_exist = tables
    tables_current = tables

    #this may need a bit of editing and refactoring

    while len(tables_exist) > 0: 
        logger.info('Not all tables currently exist!')

        table = tables_exist[0]

        try: 
            price = cloud.execute(f"SELECT close FROM {str(table)} ORDER BY timestamp DESC").all()[0][0]
            if type(price) == type(float(1.0)):
                tables_exist.remove(table)
            else:
                logger.info(f"{table} price not a float")
                #do we need to break here?
        except: 
            logger.info(f"{table} has not been created yet!")
            time.sleep(1)

    logger.info('Tables all exist, checking dates!')

    while len(tables_current) > 0:

        current_time = int(time.time()*1000)

        table = tables_current[0]
        last_time_table = cloud.execute(f"SELECT timestamp FROM {str(table)} ORDER BY timestamp DESC").fetchone()[0]

        if last_time_table + (60*30*1000) > current_time:
            tables_current.remove(table)
        else: 
            logger.info(f"{table} is not currently up to date!")
            time.sleep(1)

    logger.info('Tables are all up to date! Moving forward!')

def prepare_table(data, ticker, tables, base_table, bar_size, heartbeat):

    pair_number = data['pair_number']

    id = pair_number + random.randint(0,1000000)
    table = (str(ticker) + '_' +  str(bar_size)).lower()
    limit = 1000 #Limit on dataframe can be set here

    if table in tables: 
        cloud.execute("DROP TABLE " + str(table))
    
    res = cloud.execute(f"SELECT * FROM {str(base_table)} WHERE ticker_name='{str(ticker.upper())}' AND bar_size='{str(bar_size)}'").all()
    if len(res) == 1: 
        cloud.execute(f"UPDATE {str(base_table)} SET heartbeat={str(heartbeat)} WHERE ticker_name='{str(ticker)}' AND bar_size ='{str(bar_size)}'") 
    else: 
        try: 
            cloud.execute(f"DELETE FROM {str(base_table)} WHERE ticker_name='{str(ticker.upper())}' AND bar_size='{str(bar_size)}'")
        except:
            pass
            
        if base_table == 'price_websockets':
            cloud.execute(f"INSERT INTO price_websockets (id, ticker_name, bar_size, heartbeat) VALUES ({str(id)},'{str(ticker.upper())}','{str(bar_size)}',{str(heartbeat)})")
        elif base_table == 'ohlcv_websockets': 
            cloud.execute(f"INSERT INTO ohlcv_websockets (id, ticker_name, bar_size, limits, heartbeat) VALUES ({str(id)},'{str(ticker.upper())}','{str(bar_size)}',{str(limit)},{str(heartbeat)})")


def check_websockets(data, tickers, bar_size, ohlcv=False): 
    heartbeat = int(time.time()*1000)

    if ohlcv == True: 
        base_table = 'ohlcv_websockets'
    else: 
        base_table = 'price_websockets'

    #get list of all tables in postgres, check if tables exist 
    tables = pd.DataFrame(cloud.execute("SELECT table_name FROM information_schema.tables"))
    tables = tables['table_name'].values.tolist()

    for t in tickers:    
        prepare_table(data, t, tables, base_table, bar_size, heartbeat)

    #waiting for the tables to be populated 
    wait_for_tables(tickers, bar_size) #this is going to need to be rewritten
    logger.info('Both price tables are currently present, script starting!')

def update_websockets(tickers, bar_size, ohlcv=False):
    if ohlcv == True: 
        table_type = 'ohlcv_websockets'
    else: 
        table_type = 'price_websockets'

    current_time = int(time.time()*1000)

    for t in tickers: 
        cloud.execute("UPDATE " + str(table_type) + " SET heartbeat="+ str(current_time) + " WHERE ticker_name='" + str(t) + "' AND bar_size='" + str(bar_size) + "'")

def create_model(tickers, bar_size='15m', bars_back=1000):

    prices = {}

    for t in tickers: 
        prices[t] = get_prices_crypto(t, bar_size, bars_back)

    prices = pd.DataFrame(prices)

    jres = coint_johansen(prices, det_order=0, k_ar_diff=1)
    coeff = jres.evec[:,0]

    for i in range(len(tickers)): 
        position_ratios[tickers[i]] = coeff[i]

    return position_ratios
    
def check_tables(data, tickers):

    pair_number = data['pair_number']
    bar_size = data['bar_size']

    logger.info("Checking if appropriate sql tables exist! Creating tables that don't exist")
    table_list = ['current_data', 'trades', 'pair_data_' + str(pair_number), 'completed_trades','positions','trade_data_queue', 'equity_data', 'statuses', 'pair_trades', 'tickers', 'latest_prices'] #equity_data
    to_create = []
    create_cloud = []   

    position_ratios = create_model(tickers)  #can also enter manual model here                       

    for table in table_list: 
        res = cloud.execute(f"SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_name = '{table}');")
        a = res.one()[0]
        if a == True: 
            pass
        else: 
            create_cloud.append(table)

    if len(to_create) > 0:
        logger.info('Creating tables :' + str(to_create))

    #tables need to be reformatted 
    '''
    Relational Schema 

    current data -  pair_number numeric, bar_size varchar(256), lookback numeric, current_z numeric, upper_band numeric, lower_band numeric, moving_average numeric, current_equity numeric, current_status varchar(256), total_pl numeric, raw_pl numeric, fees numeric, heartbeat numeric, price_a numeric, price_b numeric, spread numeric
    trades - pair_number numeric, internal_id numeric, trade_id numeric, timestamp varchar(256), ticker varchar(256), buy_sell varchar(256), price numeric, quantity numeric, fees numeric, status varchar(256)
    pair_data_x - timestamp numeric, zscore numeric, upper_band numeric, lower_band numeric, moving_average numeric, spread numeric, prices_a numeric, prices_b numeric, volume_a numeric, volume_b numeric, volume_ma_a numeric, volume_ma_b numeric, equity numeric
    completed_trades - pair_number numeric, ticker varchar(256), p_l numeric, fees numeric, close_time numeric, duration numeric, internal_id numeric, special_status varchar(256))
    tickers - pair_number numeric, ticker1 varchar(256),ticker2 varchar(256),ticker3 varchar(256),ticker4 varchar(256),ticker5 varchar(256),ticker6 varchar(256),ticker7 varchar(256),ticker8 varchar(256),ticker9 varchar(256),ticker10 varchar(256),ticker11 varchar(256),ticker12 varchar(256)
    positions - pair_number numeric, pos_change_time numeric, script_update_time numeric, stop_flag bool, ticker1 numeric, ticker2 numeric,ticker3 numeric, ticker4 numeric, ticker5 numeric, ticker6 numeric, ticker7 numeric, ticker8 numeric, ticker9 numeric, ticker10 numeric, ticker11 numeric, ticker12 numeric)
    latest_prices - pair_number numeric, ticker1 numeric, ticker2 numeric,ticker3 numeric, ticker4 numeric,ticker5 numeric, ticker6 numeric, ticker7 numeric, ticker8 numeric, ticker9 numeric, ticker10 numeric, ticker11 numeric, ticker12 numeric
    trade_data_queue - pair_number numeric, ticker varchar(256), quantity numeric, open_close varchar(256),shares_remaining numeric, timestamp numeric, internal_id numeric
    equity_data - timestamp numeric, equity numeric, unrealized_gains numeric
    statuses - row numeric, display numeric, orders numeric, websockets numeric, unrealized numeric, latest_equity numeric
    #pair_trades - 
    
    '''

    for table in create_cloud:
        if table == 'current_data':
            cloud.execute("CREATE TABLE current_data (pair_number numeric, bar_size varchar(256), lookback numeric, current_z numeric, upper_band numeric, lower_band numeric, moving_average numeric, current_equity numeric, current_status varchar(256), total_pl numeric, raw_pl numeric, fees numeric, heartbeat numeric, price_a numeric, price_b numeric, spread numeric)")
            logger.info(f"Creating table {table}")

        if table == 'trades':
            cloud.execute("CREATE TABLE trades (pair_number numeric, internal_id numeric, trade_id numeric, timestamp varchar(256), ticker varchar(256), buy_sell varchar(256), price numeric, quantity numeric, fees numeric, status varchar(256))") 
            logger.info(f"Creating table {table}")

        if table == 'completed_trades':
            cloud.execute("CREATE TABLE completed_trades (pair_number numeric, ticker varchar(256), p_l numeric, fees numeric, close_time numeric, duration numeric, internal_id numeric, special_status varchar(256))")
            logger.info(f"Creating table {table}")

        if table == 'tickers':
            cloud.execute("CREATE TABLE tickers (pair_number numeric, ticker1 varchar(256),ticker2 varchar(256),ticker3 varchar(256),ticker4 varchar(256),ticker5 varchar(256),ticker6 varchar(256),ticker7 varchar(256),ticker8 varchar(256),ticker9 varchar(256),ticker10 varchar(256),ticker11 varchar(256),ticker12 varchar(256))")
            logger.info(f"Creating table {table}")

        if table == 'positions':
            cloud.execute("CREATE TABLE positions (pair_number numeric, pos_change_time numeric, script_update_time numeric, stop_flag bool, ticker1 numeric, ticker2 numeric, ticker3 numeric, ticker4 numeric, ticker5 numeric, ticker6 numeric, ticker7 numeric, ticker8 numeric, ticker9 numeric, ticker10 numeric, ticker11 numeric, ticker12 numeric)")
            logger.info(f"Creating table {table}")
        
        if table == 'latest_prices':
            cloud.execute("CREATE TABLE latest_prices (pair_number numeric, ticker1 numeric, ticker2 numeric,ticker3 numeric, ticker4 numeric,ticker5 numeric, ticker6 numeric, ticker7 numeric, ticker8 numeric, ticker9 numeric, ticker10 numeric, ticker11 numeric, ticker12 numeric)")
            logger.info(f"Creating table {table}")

        if table == 'trade_data_queue':
            cloud.execute("CREATE TABLE trade_data_queue (pair_number numeric, ticker varchar(256), quantity numeric, open_close varchar(256),shares_remaining numeric, timestamp numeric, internal_id numeric)")
            logger.info(f"Creating table {table}")

        if table == 'equity_data':
            cloud.execute("CREATE TABLE equity_data (timestamp numeric, equity numeric, unrealized_gains numeric)")
            logger.info(f"Creating table {table}")

        if table == 'statuses': 
            cloud.execute("CREATE TABLE statuses (row numeric, display numeric, orders numeric, websockets numeric, unrealized numeric, latest_equity numeric)")
            logger.info(f"Creating table {table}")
            cloud.execute("INSERT INTO statuses (row, display, orders, websockets) VALUES (1,0,0,0) ")

        # if table == 'pair_data_' + str(pair_number):
        #     cloud.execute("CREATE TABLE " + table + " (timestamp numeric, zscore numeric, upper_band numeric, lower_band numeric, moving_average numeric, spread numeric, equity numeric)") 
        #     logger.info(f"Creating table {table}")

        #Do not believe this is currently needed 
        # if table == 'pair_trades':
        #     cloud.execute("CREATE TABLE pair_trades (pair_number numeric, internal_id numeric, pl numeric, fees numeric, total_pl numeric, latest_close_time numeric)")
        #     logger.info(f"Creating table {table}")

    cloud.execute(f"DELETE FROM tickers WHERE pair_number={pair_number}")
    cloud.execute(f"DELETE FROM positions WHERE pair_number={pair_number}")
    cloud.execute(f"DELETE FROM latest_prices WHERE pair_number={pair_number}")

    cloud.execute(f"INSERT INTO tickers (pair_number) VALUES ({pair_number})")
    cloud.execute(f"INSERT INTO latest_prices (pair_number) VALUES ({pair_number})")
    cloud.execute(f"INSERT INTO positions (pair_number) VALUES ({pair_number})")

    for i in range(len(tickers)):
        t = 'ticker' + str(i+1)
        cloud.execute(f"UPDATE tickers SET {t}='{tickers[i]}' WHERE pair_number={pair_number}")
        cloud.execute(f"UPDATE latest_prices SET {t}=0 WHERE pair_number={pair_number}")
        cloud.execute(f"UPDATE positions SET {t}=0 WHERE pair_number={pair_number}")
        
    pos_table = cloud.execute(f"SELECT * FROM positions WHERE EXISTS (SELECT * FROM positions WHERE pair_number = {str(pair_number)})")
    pos_data = pos_table.all()

    if len(pos_data) == 0: 
        cloud.execute(f"INSERT INTO positions (pair_number) VALUES ({str(pair_number)})")

    res = cloud.execute(f"SELECT * FROM positions WHERE pair_number={str(pair_number)}").fetchall()[0]
    
    #get all necessary info here, only doing the first 1000 bars to start

    data = {}

    for t in tickers: 
        data[t] = get_prices_crypto(t, bar_size,500,return_all_data=True) #may want to implement websocket as an option 

    prices = {}
    timestamps = []
    volume = {}

    for d in data:
        timestamps = [a[0] for a in data[d]]
        prices[d] = [a[4] for a in data[d]]
        #volume[d] = [a[5] for a in data[d]]

    spread = get_spread(position_ratios, prices) 

    zscore, upper_band, lower_band, z_moving_avg = get_data(spread=spread)
    zscore, moving_avg, upper_band, lower_band = zscore[lookback*2+2:], z_moving_avg[lookback*2+2:], upper_band[lookback*2+2:], lower_band[lookback*2+2:]

    spread = spread[lookback*2+2:]
    timestamps = timestamps[lookback*2+2:]
    equity = [0 for i in range(len(spread))]

    # volume_ma = {}
    # for t in tickers: 
    #     volume_ma[t] = get_moving_avg(volume[t], override='sma').values.tolist()

    #volume_a, volume_b = volume_a[lookback*2+2:], volume_b[lookback*2+2:] leaving volume out for now 
    #volume_a_ma, volume_b_ma = volume_a_ma[lookback*2+2:], volume_b_ma[lookback*2+2:] leaving out for now 

    #I have a feeling that lock issues are causing problems here
    data = {'timestamp':timestamps, 'zscore':zscore, 'upper_band':upper_band, 'lower_band':lower_band, 'moving_average':moving_avg,'spread':spread, 'equity':equity}
    data_df = pd.DataFrame(data)

    delete_locks('pair_data_' + str(pair_number))
    #cloud.execute(f"DELETE FROM pair_data_{str(pair_number)}")
    data_df.to_sql('pair_data_' + str(pair_number), con=cloud, if_exists='replace', method='multi')

    #create row in current_data if it does not exist. 
        
    res = cloud.execute(f"SELECT * FROM current_data WHERE EXISTS (SELECT * FROM current_data WHERE pair_number = {str(pair_number)})")
    data = res.all()

    if len(data)==0:
        cloud.execute(f"INSERT INTO current_data (pair_number) VALUES ({str(pair_number)})")

    return position_ratios 

#Crypto specific functions 

def update_global_data_crypto(tickers, data, position_ratios, global_data=[]): #this is going to require some serious work to remain effective

    duration = data['duration']
    bar_size = data['bar_size']
    global_data_max_size = data['global_data_max_size']

    update, replace_df = False, False

    if len(global_data) == 0:  
        logger.info('Global data is empty, initializing data!')
        replace_df = True
    else:
        latest_time_pulled = get_prices_crypto_websocket(tickers[0], bar_size, 2, ohclv=False, return_all_data=True)
        delta_time = int(abs((latest_time_pulled[0][0] - latest_time_pulled[1][0])/1000)) #in seconds

        latest_df_time = global_data['Time'].iloc[0]
        differential = latest_time_pulled[0][0]- latest_df_time
        if differential > 0: 
            delta_bars = ((latest_time_pulled[0][0] - latest_df_time)/(delta_time*1000))*1.2
            if delta_bars > len(global_data): 
                replace_df = True #update entire dataframe

        elif differential == 0: 
            update = True

    data_latest = {}
    for t in tickers: 
        data = get_prices_crypto_websocket(t, bar_size, duration, ohclv=True, return_all_data=True)[:global_data_max_size]
        data = data[::-1]
        data_latest[t] = data

    times= []
    data_close = {}
    data_low = {}
    data_high = {}
    volume = {}
    volume_ma = {}

    times = [d[0] for d in data]

    for t in tickers: 
        data_close[t] = [a[4] for a in data_latest[t]]
        data_low[t] = [a[3] for a in data_latest[t]]
        data_high[t] = [a[2] for a in data_latest[t]]
        volume[t] = [a[5] for a in data_latest[t]]
        volume_ma[t] = None

    spread = get_spread(position_ratios, data_close) #this needs to be looked at 

    zscore, upper_band, lower_band, moving_avg = get_data(spread=spread) #this needs to be looked at 

    #all the data and times need to be reversed, prices also need to be reverse

    #reverse prices here

    spread, zscore, upper_band, lower_band, moving_avg = spread[::-1], zscore[::-1], upper_band[::-1], lower_band[::-1], moving_avg[::-1]
    times = times[::-1]
    
    #volume_a, volume_b, volume_ma_a, volume_ma_b  = volume_a[::-1], volume_b[::-1], volume_ma_a[::-1], volume_ma_b[::-1]

    df = pd.DataFrame({'Time':times,'Spread': spread,'Zscore': zscore, 'Upper Band': upper_band, 'Lower Band': lower_band, 'Moving Average': moving_avg})
    
    for t in tickers: #add to the df dataframe 
        df[f'{t}'] = data_close[t][::-1]

    if update == True: 
        global_data.iloc[0] = df.iloc[0]

    elif replace_df == False and update == False: 
        df_time = df['Time'].iloc[-1]
        rows = 0
        for t in global_data['Time']:
            rows += 1
            if df_time == t: 
                break 
        if rows > 0: 
            global_data = global_data.truncate(before=rows)
            global_data = pd.merge(df,global_data, how="outer")

    elif replace_df == True:
        global_data = df

    if len(global_data) < lookback*2.2: #this should solve any issues that we have with this. If the length is too short we just replace it. 
        global_data = df
        logger.error('Replacing df -- error 11')

    global_data = global_data[:global_data_max_size]

    return global_data

def get_positions_crypto(tickers): #tickers enteread in a list with no seperators
    res = {}
    tickers = format_tickers(tickers) #need to format these
    account_balance = exchange.fetch_balance()['info']['positions']

    for i in account_balance: 
        try: #if the value is zero it will return an error, so we use an exception to set it to zero
            if i['symbol'] in tickers: 
                res[i['symbol']] = i['positionAmt']
        except Exception as e: 
            logger.error(e)
    return res

def get_prices_crypto(ticker, bar_period,bars_back,return_all_data=False):

    if bars_back <= 1000:
        data = exchange.fetch_ohlcv(ticker,bar_period,limit=bars_back)
        
    elif bars_back > 1000: #***finish this for pagenation, how do we get a certain period? 
        date_list, data = [], []
        if bar_period == '1m':
            time_delta = 60
        elif bar_period == '5m':
            time_delta = 60*5
        elif bar_period == '15m':
            time_delta = 60*15
        elif bar_period == '30m':
            time_delta = 60*30
        elif bar_period == '1h':
            time_delta = 60*60
        elif bar_period == '2h':
            time_delta = 60*120
        elif bar_period == '4h':
            time_delta = 60*4*60
        elif bar_period == '6h':
            time_delta = 60*6*60
        elif bar_period == '1d':
            time_delta = 60*24*60
        else: 
            logger.error('Please enter an appropriate time period or add the timedelta!')
            print(Break123)

        now = int(time.time()*1000)
        res = math.floor(bars_back/1000)

        for i in range(1,res+1):
            date_list.append(now-(time_delta*i*1000*1000))

        date_list = date_list[::-1]

        for date in date_list:
            data = data + exchange.fetch_ohlcv(ticker,bar_period,since=date,limit=1000)
            
    if return_all_data == True: 
        return data
    else: 
        prices = []

        for d in data: 
            prices.append(d[4])
        return prices

        # if bars_back%1000 != 0:
        #     extra = bars_back%1000
        #     e_date = date_list[0]+extra*time_delta
        #     data = exchange.fetch_ohlcv(ticker,bar_period,since=e_date,limit=extra) + data

def get_prices_crypto_websocket(ticker, bar_size, bars, ohclv=False, return_all_data=False): #this is going to be the one we want to use next, and going to require some further editing 
    table = 'ohlcv_' + str(ticker).lower() + '_' + str(bar_size)
    if return_all_data == False: 
        res = pd.DataFrame(cloud.execute(f"SELECT close FROM {str(table)} ORDER BY timestamp desc").all())['close'].values.tolist()
        return res[:bars] #results are returned most recent bar first
    else:
        res = pd.DataFrame(cloud.execute(f"SELECT * FROM {str(table)} ORDER BY timestamp desc").all()).values.tolist()
        return res[:bars]

def check_entry_signals(global_data): #this is on hold until the rest of the script is working with multivariate

    if volume_signal == True: 

        last_bars_vol = {}

        last_5bars_vol_a = max(global_data['Volume A'].iloc[:5].values.tolist())
        last_5bars_vol_b = max(global_data['Volume B'].iloc[:5].values.tolist())

        vol_ma = {}

        vol_ma_a = get_moving_avg(global_data['Volume A'][::-1], override='sma')['Volume A'].values.tolist()
        vol_ma_b = get_moving_avg(global_data['Volume B'][::-1], override='sma')['Volume B'].values.tolist()

        if vol_notrade_multiple*vol_ma_a[-1] < last_5bars_vol_a or vol_notrade_multiple*vol_ma_b[-1] < last_5bars_vol_b:
            return False

    return True

def run_script_crypto(tickers, data, current_state=current_state):

    entry_signal_flag = True 
    internal_id = 100000
    reset_flag = False
    pair_number = data['pair_number']
    bar_size = data['bar_size']

    logger.info(f'Starting Script for pair {pair_number}! Running pre-initialization functions!')

    formatted_tickers = format_tickers(tickers)
    position_ratios = check_tables(data, formatted_tickers) 
    check_websockets(data, formatted_tickers, bar_size, ohlcv=ohlcv)
    
    global_data = update_global_data_crypto(formatted_tickers,data, position_ratios) #I believe prices are backwards
    current_state = get_current_state(data)

    new_quantities = get_quantities(formatted_tickers, global_data, data, position_ratios) #the quantities do not seem correct, may be an issue with prices being backwards? 
    internal_id = set_positions_database(internal_id, formatted_tickers, new_quantities, None, current_state,data) 

    logger.info('Starting pair ' + str(pair_number))

    while True:

        global_data = update_global_data_crypto(formatted_tickers, data, position_ratios, global_data=global_data)

        curr_zscore, curr_moving_avg, curr_upper_band, curr_lower_band = global_data['Zscore'].iloc[0], global_data['Moving Average'].iloc[0], global_data['Upper Band'].iloc[0], global_data['Lower Band'].iloc[0]
        prior_state = current_state

        current_state, reset_flag, trade_status = decide_positions(curr_zscore, curr_moving_avg, curr_upper_band, curr_lower_band,reset_flag, current_state)

        if use_stoploss == True: 
            internal_id = check_exit_conditions(internal_id, global_data, tickers, new_quantities,current_state, data, percent_stop=True)

        # if current_state == None: 
        #     entry_signal_flag = check_entry_signals(global_data)

        if (prior_state != current_state) and reset_flag == False:
            if prior_state == None and entry_signal_flag == False: 
                logger.info('Trade averted due to entry signal flag being false')

            else: 
                logger.info(f"Position has changed, submitting trades. The current state is {current_state} and the previous state is {prior_state}")
                internal_id = set_positions_database(internal_id, tickers, new_quantities, prior_state, current_state,data)

        record_data(data, formatted_tickers, current_state, global_data) 
        update_websockets(formatted_tickers, bar_size,ohlcv=ohlcv) #updates the heartbaet on our websockets.py

        time.sleep(0.5)

#Required -- Pair Number, tickers, Lookback Period (reference only), Bar Size, Spread Lookback Period (Operational), Std Dev
def run_crypto(pair_number1, tickers, bar_size, lookback,sigma,order_val_input=total_order_value, avg_type_input=average_type,stop_loss=max_loss,stoploss_break=break_on_loss, band_vol_exp=bollinger_vol_exp, bol_band_type=bollinger_type):
    data = {}
    
    data['pair_number'] = pair_number1 # pair_number = pair_number1
    data['duration'] = lookback*5 #duration_lookback = lookback_input*5
    data['bar_size'] = bar_size #bar_size = bar_size
    data['lookback'] = lookback #lookback = lookback_input
    data['entry_sigma '] = sigma #entry_sigma = sigma
    data['total_order_value'] = order_val_input #total_order_value = order_val_input
    data['average_type'] = avg_type_input #average_type = avg_type_input
    data['max_loss'] = stop_loss #max_loss = stop_loss
    data['break_on_loss'] = stoploss_break #break_on_loss = stoploss_break
    data['bollinger_vol_exp'] = band_vol_exp # bollinger_vol_exp = band_vol_exp
    data['bollinger_type'] = bol_band_type #bollinger_type = bol_band_type
    data['global_data_max_size'] = int(float(lookback)*2.5) #global_data_max_size = int(float(lookback)*2.5) #max size of global_data
    
    run_script_crypto(tickers, data)

if __name__ == "__main__":
    create_threads(read_arguments_from_file('config.json'), run_crypto)

#t.run_crypto(47,['BTC/USDT','ETH/USDT','BNB/USDT'],'1d',50,2.75,avg_type_input='ema',stop_loss=-0.02, stoploss_break=True, order_val_input=100)
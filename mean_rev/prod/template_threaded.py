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
import requests
from loguru import logger
import json
import concurrent.futures
import asyncio
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
hedge_ratio, current_state, last_pos = None, None, None #Can be manually changed to change the state when restarting a script. 
internal_id = 100000

entry_sigma = 3 #the standard deviation multiplyer we are using for bollinger bands 
exit_sigma = 4
use_stoploss = True
total_order_value = 10 #the current value of our orders

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

def get_current_state(data): 

    pair_number = data['pair_number']

    res = cloud.execute("SELECT current_status FROM current_data WHERE pair_number = " + str(pair_number)).one()[0]
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

def get_quantities(ticker1, ticker2, global_data, data, hedge_ratio):

    dollar_value = data['total_order_value']
    last_price = global_data['Ticker 1 Price'].iloc[0] #are these the most recent prices? 
    ticker1_quantity = float(dollar_value/last_price)
    ticker2_price = global_data['Ticker 2 Price'].iloc[0] #similar, are these the most recent prices? 

    mkt_data = exchange.loadMarkets() #This call is fine 

    step_size_ticker1 = float(mkt_data[ticker1]['info']['filters'][1]['stepSize'])
    step_size_ticker2 = float(mkt_data[ticker2]['info']['filters'][1]['stepSize'])

    ticker1_min = mkt_data[ticker1]['limits']['amount']['min']
    ticker2_min = mkt_data[ticker2]['limits']['amount']['min']

    if ticker1_quantity*ticker2_price * hedge_ratio < 6.50: 
        ticker2_quantity = float(6.50/ticker2_price)
        step_up = ticker2_quantity%step_size_ticker2
        ticker2_quantity += (step_size_ticker2 - step_up)
        logger.info('Order value is less than $6, putting this as a minimum to ensure it goes through!')
    else: 
        ticker2_quantity = ticker1_quantity*hedge_ratio

    if ticker1_quantity < ticker1_min:
        ticker1_quantity = ticker1_min
    if ticker2_quantity < ticker2_min: 
        ticker2_quantity = ticker2_min

    #add a move to the most recent step size
    ticker1_quantity = ticker1_quantity-(ticker1_quantity%step_size_ticker1)
    ticker2_quantity = ticker2_quantity-(ticker2_quantity%step_size_ticker2)

    return round(ticker1_quantity, 5), round(ticker2_quantity,5)

def format_ticker(tick1=None, ticker2=None, use_list=False, ticker_list=None):

    if tick1 and not ticker2: 
        if tick1[-4:] == 'USDT':
            return tick1.split('/')[0] + 'USDT'
        elif tick1[-4:] == 'BUSD':
            return tick1.split('/')[0] + 'BUSD'

    if use_list == False: 
        res = []
        if tick1[-4:] == 'USDT':
            res.append(tick1.split('/')[0] + 'USDT')
        elif tick1[-4:] == 'BUSD':
            res.append(tick1.split('/')[0] + 'BUSD')

        if ticker2[-4:] == 'USDT':
            res.append(ticker2.split('/')[0] + 'USDT')
        elif ticker2[-4:] == 'BUSD':
            res.append(ticker2.split('/')[0] + 'BUSD')
        return res[0], res[1]
    elif use_list == True: 
        adj_list = []
        for ticker in ticker_list:
            if ticker[-4:] == 'USDT':
                adj_list.append(ticker.split('/')[0] + 'USDT')
            elif ticker[-4:] == 'BUSD':
                adj_list.append(ticker.split('/')[0] + 'BUSD')
        return adj_list

##pairs trading specific functions

def set_positions_database(internal_id, ticker1, ticker2, ticker1_quantity, ticker2_quantity, prior_state, current_pos, data, use_crypto=False, stop=False): 

    pair_number = data['pair_number']

    if internal_id == None: 
        internal_id = 300000

    heartbeat = int(time.time()*1000)

    ticker1, ticker2 = format_ticker(tick1=ticker1, ticker2=ticker2)
    if use_crypto == True:
        pass
    else: 
        logger.error("Script is not set up for equities, please change to normal")

    res = cloud.execute("SELECT * FROM positions WHERE pair_number=" + str(pair_number)).all()[0]
    if res[2]:
        curr_pos_1, curr_pos_2 = float(res[2]), float(res[4]) 
    else:
        curr_pos_1, curr_pos_2 = 0,0

    if current_pos == None: 
        ticker1_final_quantity, ticker2_final_quantity = 0,0

        open_close = 'CLOSE'
        if prior_state == None: 
            
            if curr_pos_1 != 0:
                ticker1_delta = curr_pos_1*-1 #make sure this logic makes sense 
                cloud.execute("INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values (" + str(pair_number) + ",'" + str(ticker1)+ "'," + str(ticker1_delta) + ",'" + open_close + "', " + str(ticker1_delta) + "," + str(heartbeat) + "," + str(internal_id) + ")") 
                logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker1} Ticker Delta: {ticker1_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")
            if curr_pos_2 != 0: 
                ticker2_delta = curr_pos_2*-1
                cloud.execute("INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values (" + str(pair_number) + ",'" + str(ticker2)+ "'," + str(ticker2_delta) + ",'" + open_close + "', " + str(ticker2_delta) + "," + str(heartbeat) + "," + str(internal_id) + ")") 
                logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker2} Ticker Delta: {ticker2_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

            cloud.execute("UPDATE positions SET ticker1='" + str(ticker1) + "', ticker2 = '" + str(ticker2) + "', ticker1_pos=0, ticker2_pos=0, pos_change_time = " + str(heartbeat) + " WHERE pair_number=" + str(pair_number)) 
            logger.info(f"Updating positions pair number {pair_number}{ticker1} and {ticker2} positions set to zero at timestamp {heartbeat}")
            return

        elif prior_state.lower() == 'long':
            ticker1_delta, ticker2_delta = curr_pos_1*-1, curr_pos_2*-1
        elif prior_state.lower() == 'short':
            ticker1_delta, ticker2_delta = curr_pos_1*-1, curr_pos_2*-1 

    elif current_pos.lower() == 'long':
        ticker1_final_quantity, ticker2_final_quantity = ticker1_quantity,ticker2_quantity*-1
        
        ticker1_delta, ticker2_delta = ticker1_quantity, ticker2_quantity*-1

        open_close = 'OPEN'
    elif current_pos.lower() == 'short':

        ticker1_final_quantity, ticker2_final_quantity = ticker1_quantity*-1,ticker2_quantity

        ticker1_delta, ticker2_delta = ticker1_quantity*-1, ticker2_quantity
        open_close = 'OPEN'

    res = cloud.execute('SELECT * FROM positions WHERE pair_number = ' + str(pair_number)).all()[0]
    ticker1_pos, ticker2_pos = res[2], res[4]

    ticker1_curr_ticker,ticker2_curr_ticker = res[1], res[3]

    if ticker1_pos == None:
        pass
    else: 
        ticker1_pos = float(ticker1_pos)

    if ticker2_pos == None:
        pass
    else: 
        ticker2_pos = float(ticker2_pos)

    if open_close == 'OPEN':
        internal_id = str(pair_number) + str(heartbeat)[-5:]

    if ticker1_pos == ticker1_final_quantity and ticker1 == ticker1_curr_ticker:
        pass 
    else: 
        if ticker1_final_quantity != ticker1_pos: 
            cloud.execute("INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values (" + str(pair_number) + ",'" + str(ticker1)+ "'," + str(ticker1_delta) + ",'" + open_close + "', " + str(ticker1_delta) + "," + str(heartbeat) + "," + str(internal_id)+ ")") 
            logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker1} Ticker Delta: {ticker1_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

        cloud.execute("UPDATE positions SET ticker2 = '" + str(ticker1) + "', ticker1_pos=" + str(ticker1_final_quantity) + " WHERE pair_number=" + str(pair_number))
        logger.info(f"Updating positions setting {ticker1} quantity to {ticker1_final_quantity} under pair {pair_number}")

    if ticker2_pos == ticker2_final_quantity and ticker2 == ticker2_curr_ticker:
        pass 
    else: 
        if ticker2_final_quantity != ticker2_pos:  
            cloud.execute("INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values (" + str(pair_number) + ",'" + str(ticker2)+ "'," + str(ticker2_delta) + ",'" + open_close + "', " + str(ticker2_delta) + "," + str(heartbeat) + "," + str(internal_id)+ ")") 
            logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker2} Ticker Delta: {ticker2_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

        cloud.execute("UPDATE positions SET ticker2 = '" + str(ticker2) + "', ticker2_pos=" + str(ticker2_final_quantity) + " WHERE pair_number=" + str(pair_number))
        logger.info(f"Updating positions setting {ticker2} quantity to {ticker2_final_quantity} under pair {pair_number}")

    cloud.execute("UPDATE positions SET pos_change_time = " + str(heartbeat) + " WHERE pair_number=" + str(pair_number))

    return internal_id

def decide_positions(z, moving_avg, upper_band, lower_band, reset_flag, current_pos='short'):

    if current_pos == None:
        reset_flag = False
        if z > upper_band:
            trade_status = 'open'
            return 'short', reset_flag, trade_status
        elif z < lower_band: 
            trade_status = 'open'
            return 'long', reset_flag, trade_status
        else:
            return None, reset_flag, None

    elif current_pos.lower() == 'long':
        if z >= moving_avg:
            reset_flag = False
            trade_status = None
            return None, reset_flag, trade_status
        elif z < moving_avg: 
            return 'long', reset_flag, None

    elif current_pos.lower() == 'short':
        if z <= moving_avg:
            reset_flag = False
            trade_status = None 
            return None, reset_flag, trade_status
        elif z > moving_avg:
            return 'short', reset_flag, None

#Mean Rev math specific functions 
def get_spread(hedge_ratio, global_data=None ,prices_a=None, prices_b=None): 

    if prices_a == None or prices_b == None: 
        df = global_data['Spread'][::-1]
        return df.tolist()

    spread = []
    for i in range(len(prices_a)):
        if spread_type == 'log':
            spread.append(math.log(prices_a[i]) - hedge_ratio*math.log(prices_b[i]))
        elif spread_type == 'absolute':
            spread.append(prices_a[i] - hedge_ratio*prices_b[i])
    return spread

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

def get_data(spread=None, global_data=None, hedge_ratio=None):

    if spread == None:
        spread = get_spread(hedge_ratio, global_data=global_data)

    zscore = get_zscore(spread)
    band_up_df, band_down_df = get_bands(zscore) 
    z_moving_avg_df = get_moving_avg(zscore) #This can be easily changed to a kalman filter to implement within the code

    if math.isnan(z_moving_avg_df.iloc[-1]) == True: 
        logger.error('The duration we are looking back at is likely not longer than the lookback period! Fix to get correct functionality!')

    return zscore, band_up_df[0].tolist(), band_down_df[0].tolist(), z_moving_avg_df[0].tolist()
    
#Mean reversion specific functions 

def record_data(data, security_a, security_b, current_state, global_data, use_crypto=False): 

        pair_number = data['pair_number']
        bar_size = data['bar_size']

        curr_time = int(time.time()*1000)

        #Get quant data
        
        latest_time, current_z, current_average, current_upper_band, current_lower_band = global_data['Time'].iloc[0], global_data['Zscore'].iloc[0], global_data['Moving Average'].iloc[0], global_data['Upper Band'].iloc[0], global_data['Lower Band'].iloc[0]# we are likely aligning the wrong thing with the wrong times
        latest_price_a, latest_price_b = global_data['Ticker 1 Price'].iloc[0], global_data['Ticker 2 Price'].iloc[0]
        latest_spread = global_data['Spread'].iloc[0]
        latest_volume_a, latest_volume_b = global_data['Volume A'].iloc[0], global_data['Volume B'].iloc[0]

        #Log total_equity for that pair 

        res = cloud.execute("SELECT total_pl from current_data WHERE pair_number=" + str(pair_number))
        current_equity = res.one()[0]
        if current_equity == None:
            current_equity = 0

        table_name = 'pair_data_' + str(pair_number)

        #Write to pair Data, may be moved to display.py later

        latest_recorded_time = int(cloud.execute("SELECT timestamp from " + table_name + " ORDER BY timestamp desc").all()[0][0])
        if latest_recorded_time != latest_time: 
            cloud.execute("INSERT INTO " + table_name + " (timestamp, zscore, upper_band, lower_band ,moving_average, spread, prices_a, prices_b, volume_a, volume_b, equity) VALUES (" + str(latest_time) + "," + str(current_z) + "," + str(current_upper_band) + "," + str(current_lower_band) + "," + str(current_average) + "," + str(latest_spread) + "," + str(latest_price_a) + "," + str(latest_price_b) + "," + str(latest_volume_a) + "," + str(latest_volume_b) + "," + str(current_equity) + ")")
        else:
            cloud.execute("UPDATE " + table_name + " SET zscore=" + str(current_z) + ",upper_band=" + str(current_upper_band) +  ",lower_band=" + str(current_lower_band) + ",moving_average= " + str(current_average)+ " ,spread=" + str(latest_spread) + " ,prices_a=" + str(latest_price_a) + ",prices_b= " + str(latest_price_b) +  ", volume_a=" + str(latest_volume_a) + ",volume_b=" + str(latest_volume_b) + ",equity= " + str(current_equity) + " WHERE timestamp=" + str(int(latest_recorded_time)))

        #Write to current data

        ticker1, ticker2  = [str(security_a).upper()], [str(security_b).upper()]

        cloud.execute(f"UPDATE current_data SET pair_1= '{ticker1[0]}', pair_2 = '{ticker2[0]}', bar_size = '{bar_size}', lookback = {str(lookback)}, current_z = {str(current_z)}, upper_band = {str(current_upper_band)}, lower_band = {str(current_lower_band)}, moving_average = {str(current_average)}, current_status = '{str(current_state)}',heartbeat = {str(curr_time)},price_a={str(latest_price_a)},price_b={str(latest_price_b)},spread={str(latest_spread)} WHERE pair_number = {str(pair_number)}")
        cloud.execute(f"UPDATE positions SET script_update_time = {str(curr_time)} WHERE pair_number={str(pair_number)}")

def check_exit_conditions(internal_id, global_data, ticker1, ticker2, new_quantity_a, new_quantity_b, current_status, data, percent_stop=False, totalpl_stop=False): #return true if the conditions pass, return false if no longer cointegrated or we are outside of standard deviation or spread parameters
    #internal id cannot be none
    pair_number = data['pair_number']
    bar_size = data['bar_size']

    total_order_value = data['total_order_value']
    max_loss = data['max_loss']
    break_on_loss = data['break_on_loss']

    permitted_loss = 0.05
    total_capital = total_order_value*2
    
    adj_ticker1, adj_ticker2 = format_ticker(tick1=ticker1, ticker2=ticker2)

    if totalpl_stop == True:
        res = cloud.execute("SELECT total_pl FROM current_data where pair_number = " + str(pair_number))
        pl = float(res.all()[0][0])

        if pl < total_capital*(permitted_loss)*-1:
            logger.info('We are below the stop loss for percent of capital lost. Closing position and shutting down script!')

            trade_status = None
            internal_id = set_positions_database(internal_id, ticker1, ticker2, 0,0,None, None,data,use_crypto=True,stop=True) #If there is an error check the prior state. 

            print(break123)

    #trade goes below a % loss
    if percent_stop == True:
        res = cloud.execute("SELECT ticker1_latest_price, ticker2_latest_price FROM positions WHERE pair_number=" + str(pair_number)).fetchall()[0]
        ticker1_current_price = get_prices_crypto_websocket(ticker1, bar_size, 1, ohclv=False, return_all_data=True)[0][4]
        ticker2_current_price = get_prices_crypto_websocket(ticker2, bar_size, 1, ohclv=False, return_all_data=True)[0][4]

        ticker1_open_price = float(res[0])
        ticker2_open_price = float(res[1])

        if ticker1_open_price == 0 or ticker2_open_price == 0:
            return

        ticker1_quantity = new_quantity_a
        ticker2_quantity = new_quantity_b

        if current_status == 'LONG' or current_status == 'long':
            current_trade_pl = ticker1_quantity*(ticker1_current_price-ticker1_open_price) + ticker2_quantity*-1*(ticker2_current_price-ticker2_open_price)
        elif current_status == 'SHORT' or current_status == 'short': 
            current_trade_pl = ticker1_quantity*-1*(ticker1_current_price-ticker1_open_price) + ticker2_quantity*(ticker2_current_price-ticker2_open_price)
        elif current_status == None or current_status == 'None':
            return

    if current_trade_pl < total_capital*(max_loss): 
        logger.info('The current trade p/l is:' + str(current_trade_pl))
        logger.info('The total capital*max loss is:' + str(total_capital*(max_loss)))

        if break_on_loss == True:
            trade_status = None

            logger.info('We are below the stop loss for percent of capital lost, shutting down script!')

            open_close = 'CLOSE'
            ticker1_delta = float(cloud.execute(f"SELECT ticker1_pos FROM positions WHERE pair_number={pair_number}").fetchone()[0])
            ticker2_delta = float(cloud.execute(f"SELECT ticker2_pos FROM positions WHERE pair_number={pair_number}").fetchone()[0])
            heartbeat = int(time.time()*1000)

            cloud.execute(f"INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values ({str(pair_number)},'{str(ticker1)}',{str(ticker1_delta)},'{open_close}',{str(ticker1_delta)},{str(heartbeat)},{str(internal_id)})")
            logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker1} Ticker Delta: {ticker1_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

            cloud.execute(f"INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values ({str(pair_number)},'{str(ticker2)}',{str(ticker2_delta)},'{open_close}',{str(ticker2_delta)},{str(heartbeat)},{str(internal_id)})")
            logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker2} Ticker Delta: {ticker2_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")
            
            cloud.execute(f"UPDATE current_data SET current_status='None' WHERE pair_number={pair_number}")
            cloud.execute(f"UPDATE positions SET stop_flag=True WHERE pair_number={str(pair_number)}")

            time.sleep(10)

            set_positions_database(internal_id, ticker1, ticker2, 0,0,None, None,data,use_crypto=True,stop=True)
            internal_id = 100000
            print(break123)

        elif break_on_loss == False: 
            reset_flag = True 
            trade_status = None
            logger.info('We hit the stop loss for this position, resetting. ')

            open_close = 'CLOSE'
            ticker1_delta = float(cloud.execute(f"SELECT ticker1_pos FROM positions WHERE pair_number={pair_number}").fetchone()[0])
            ticker2_delta = float(cloud.execute(f"SELECT ticker2_pos FROM positions WHERE pair_number={pair_number}").fetchone()[0])
            heartbeat = int(time.time()*1000)

            cloud.execute(f"INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values ({str(pair_number)},'{str(ticker1)}',{str(ticker1_delta)},'{open_close}',{str(ticker1_delta)},{str(heartbeat)},{str(internal_id)})")
            logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker1} Ticker Delta: {ticker1_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

            cloud.execute(f"INSERT into trade_data_queue (pair_number, ticker, quantity, open_close, shares_remaining, timestamp, internal_id) values ({str(pair_number)},'{str(ticker2)}',{str(ticker2_delta)},'{open_close}',{str(ticker2_delta)},{str(heartbeat)},{str(internal_id)})")
            logger.info(f"Inserting into trade_data_queue. Pair number: {pair_number} Ticker: {ticker2} Ticker Delta: {ticker2_delta} Open/Close: {open_close} Timesatmp: {heartbeat} Internal Id: {internal_id}")

            cloud.execute(f"UPDATE positions SET stop_flag=True WHERE pair_number={str(pair_number)}")
            cloud.execute(f'UPDATE current_data SET current_status=Stop WHERE pair_number={pair_number}')

            time.sleep(10)
            set_positions_database(internal_id, ticker1, ticker2, 0,0,None, None,data,use_crypto=True,stop=True)
            internal_id = 100000

            return internal_id

#websocket specific functions 

def wait_for_tables(ticker1, ticker2, bar_size):
    
    table1 = ('ohlcv_' + str(ticker1) + '_' +  str(bar_size)).lower()
    table2 = ('ohlcv_' + str(ticker2) + '_' + str(bar_size)).lower()

    price_ticker1, price_ticker2 = None, None 

    while price_ticker1 == None or price_ticker2 == None: 
        try: 
            price = cloud.execute("SELECT close FROM " + str(table1) + " ORDER BY timestamp DESC").all()[0][0]
            if type(price) == type(float(1.0)):
                price_ticker1 = price
            else:
                logger.info("Table 1 price not a float")
        except: 
            logger.info("Table 1 has not been created yet!")
            time.sleep(10)
        
        try: 
            price = cloud.execute("SELECT close FROM " + str(table2) + " ORDER BY timestamp DESC").all()[0][0]
            if type(price) == type(float(1.0)):
                price_ticker2 = price
            else:
                logger.info("Table 2 price not a float")
        except: 
            logger.info("Table 2 has not been created yet!")
            time.sleep(10)

    #ensure that both tables are close to the recent time (within an hour or so) to ensure that we aren't working off an old table. 

    logger.info('Tables both exist, checking dates!')

    current_time = int(time.time()*1000)
    last_time_table1, last_time_table2 = cloud.execute("SELECT timestamp FROM " + str(table1) + " ORDER BY timestamp DESC").fetchone()[0], cloud.execute("SELECT timestamp FROM " + str(table2) + " ORDER BY timestamp DESC").fetchone()[0]
    try: 
        while last_time_table1+(60*30*1000) < current_time or last_time_table2+(60*30*1000) < current_time:
            current_time = int(time.time()*1000)
            last_time_table1, last_time_table2 = cloud.execute("SELECT timestamp FROM " + str(table1) + " ORDER BY timestamp DESC").fetchone()[0], cloud.execute("SELECT timestamp FROM " + str(table2) + " ORDER BY timestamp DESC").fetchone()[0]
            logger.info('Table dates are not recent. Ensure websockets.py is running properly!')
            time.sleep(5)
    except: 
        logger.info("Likely the table was recreated due to being old. Script will continue!")

def prepare_table(data, ticker, tables, base_table, bar_size, heartbeat):

    pair_number = data['pair_number']

    id = pair_number + random.randint(0,1000000)
    table = (str(ticker) + '_' +  str(bar_size)).lower()
    limit = 2000 #Might have to be changed if there is a super long lookback

    if table in tables: 
        res = cloud.execute("SELECT * FROM " + str(base_table) + " WHERE ticker_name='" + str(ticker.upper()) + "' AND bar_size='" + str(bar_size) + "'").all()
        if len(res) == 1: 
            cloud.execute("UPDATE " + str(base_table) + " SET heartbeat="+ str(heartbeat) + " WHERE ticker_name='" + str(ticker) + "' AND bar_size ='" + str(bar_size) + "'") #update heartbeat 
        else: 
            try: 
                cloud.execute("DELETE FROM "+ str(base_table) + " WHERE ticker_name='" + str(ticker.upper()) + "' AND bar_size='" + str(bar_size) + "'")
            except:
                pass
            cloud.execute("DROP TABLE " + str(table))

            if base_table == 'price_websockets':
                cloud.execute("INSERT INTO price_websockets (id, ticker_name, bar_size, heartbeat) VALUES (" + str(id) + ",'" + str(ticker.upper()) + "','" + str(bar_size) + "'," + str(heartbeat) + str(")"))
            elif base_table == 'ohlcv_websockets': 
                cloud.execute("INSERT INTO ohlcv_websockets (id, ticker_name, bar_size, limits, heartbeat) VALUES (" + str(id) + ",'" + str(ticker.upper()) + "','" + str(bar_size) + "'," + str(limit) + "," + str(heartbeat) + str(")"))

    else: #add to list 
        try: 
            cloud.execute("DELETE FROM "+ str(base_table) + " WHERE ticker_name='" + str(ticker.upper()) + "' AND bar_size='" + str(bar_size) + "'")
        except:
            pass

        if base_table == 'price_websockets':
            cloud.execute("INSERT INTO price_websockets (id, ticker_name, bar_size, heartbeat) VALUES (" + str(id) + ",'" + str(ticker.upper()) + "','" + str(bar_size) + "'," + str(heartbeat) + str(")"))
        elif base_table == 'ohlcv_websockets':
            cloud.execute("INSERT INTO ohlcv_websockets (id, ticker_name, bar_size, limits, heartbeat) VALUES (" + str(id) + ",'" + str(ticker.upper()) + "','" + str(bar_size) + "'," + str(limit) + "," + str(heartbeat) + str(")"))

def check_websockets(data, ticker1, ticker2, bar_size, ohlcv=False): 
    heartbeat = int(time.time()*1000)

    if ohlcv == True: 
        base_table = 'ohlcv_websockets'
    else: 
        base_table = 'price_websockets'

    #get list of all tables in postgres, check if tables exist 
    tables = pd.DataFrame(cloud.execute("SELECT table_name FROM information_schema.tables"))
    tables = tables['table_name'].values.tolist()

    prepare_table(data, ticker1, tables, base_table, bar_size, heartbeat)
    prepare_table(data, ticker2, tables, base_table, bar_size, heartbeat)

    #waiting for the tables to be populated 
    wait_for_tables(ticker1, ticker2, bar_size)
    logger.info('Both price tables are currently present, script starting!')

def update_websockets(ticker1, ticker2, bar_size, ohlcv=False):
    if ohlcv == True: 
        table_type = 'ohlcv_websockets'
    else: 
        table_type = 'price_websockets'

    current_time = int(time.time()*1000)

    cloud.execute("UPDATE " + str(table_type) + " SET heartbeat="+ str(current_time) + " WHERE ticker_name='" + str(ticker1) + "' AND bar_size='" + str(bar_size) + "'")
    cloud.execute("UPDATE " + str(table_type) + " SET heartbeat="+ str(current_time) + " WHERE ticker_name='" + str(ticker2) + "' AND bar_size='" + str(bar_size) + "'")

def check_tables(data, ticker1, ticker2):

    pair_number = data['pair_number']
    bar_size = data['bar_size']

    logger.info("Checking if appropriate sql tables exist and setting up hedge ratio! Creating tables that don't exist")
    table_list = ['current_data', 'current_positions', 'trades', 'pair_data_' + str(pair_number), 'completed_trades','positions','trade_data_queue', 'equity_data', 'statuses', 'pair_trades'] #equity_data
    to_create = []
    create_cloud = []   

    prices_a_hr, prices_b_hr = get_prices_crypto(ticker1, '15m', 1000), get_prices_crypto(ticker2, '15m', 1000)

    m,b = np.polyfit(prices_b_hr,prices_a_hr,1)
    hedge_ratio = m

    for table in table_list: 
        res = cloud.execute("SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_name = '" + table + "');")
        a = res.one()[0]
        if a == True: 
            pass
        else: 
            create_cloud.append(table)

    if len(to_create) > 0:
        logger.info('Creating tables :' + str(to_create))

    for table in create_cloud:
        if table == 'current_data':
            cloud.execute("CREATE TABLE current_data (pair_number numeric, pair_1 varchar(256), pair_2 varchar(256), bar_size varchar(256), lookback numeric, current_z numeric, upper_band numeric, lower_band numeric, moving_average numeric, current_equity numeric, current_status varchar(256), total_pl numeric, raw_pl numeric, fees numeric, heartbeat numeric, price_a numeric, price_b numeric, spread numeric)")
            logger.info(f"Creating table {table}")

        if table == 'trades':
            cloud.execute("CREATE TABLE trades (pair_number numeric, internal_id numeric, trade_id numeric, timestamp varchar(256), ticker varchar(256), buy_sell varchar(256), price numeric, quantity numeric, fees numeric, status varchar(256))") 
            logger.info(f"Creating table {table}")

        if table == 'pair_data_' + str(pair_number):
            cloud.execute("CREATE TABLE " + table + " (timestamp numeric, zscore numeric, upper_band numeric, lower_band numeric, moving_average numeric, spread numeric, prices_a numeric, prices_b numeric, volume_a numeric, volume_b numeric, volume_ma_a numeric, volume_ma_b numeric, equity numeric)") #equity numeric
            logger.info(f"Creating table {table}")

        if table == 'completed_trades':
            cloud.execute("CREATE TABLE completed_trades (pair_number numeric, ticker varchar(256), p_l numeric, fees numeric, close_time numeric, duration numeric, internal_id numeric, special_status varchar(256))")
            logger.info(f"Creating table {table}")

        if table == 'positions':
            cloud.execute("CREATE TABLE positions (pair_number numeric, ticker1 varchar(256), ticker1_pos numeric, ticker2 varchar(256), ticker2_pos numeric, pos_change_time numeric, script_update_time numeric, ticker1_latest_price numeric, ticker2_latest_price numeric, stop_flag bool)")
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
            
        if table == 'pair_trades':
            cloud.execute("CREATE TABLE pair_trades (pair_number numeric, internal_id numeric, pl numeric, fees numeric, total_pl numeric, latest_close_time numeric)")
            logger.info(f"Creating table {table}")

    cloud.execute("DELETE FROM pair_data_" + str(pair_number))

    pos_table = cloud.execute("SELECT * FROM positions WHERE EXISTS (SELECT * FROM positions WHERE pair_number = " + str(pair_number) + ")")
    pos_data = pos_table.all()

    if len(pos_data) == 0: 
        cloud.execute("INSERT INTO positions (pair_number) VALUES (" + str(pair_number) + ")")

    res = cloud.execute("SELECT * FROM positions WHERE pair_number=" + str(pair_number)).fetchall()[0]
    if res[7] == None or res[8] == None:
        cloud.execute("UPDATE positions SET ticker1_latest_price=0, ticker2_latest_price=0 WHERE pair_number=" + str(pair_number)) 
    
    #get all necessary info here, only doing the first 1000 bars to start

    data_a, data_b = get_prices_crypto(ticker1, bar_size,5000,return_all_data=True), get_prices_crypto(ticker2, bar_size,5000,return_all_data=True) #need to replace with websockets here as an option
    prices_a, prices_b, timestamps = [], [], []
    volume_a, volume_b = [], []

    for a in data_a:
        prices_a.append(a[4])
        volume_a.append(a[5])
        
    for b in data_b:
        prices_b.append(b[4])
        volume_b.append(b[5])

    for t in data_a: 
        timestamps.append(t[0])

    spread = get_spread(hedge_ratio, prices_a=prices_a, prices_b=prices_b)

    zscore, upper_band, lower_band, z_moving_avg = get_data(spread=spread)

    #may need to be adjusted
    volume_a_ma = get_moving_avg(volume_a, override='sma').values.tolist()
    volume_b_ma = get_moving_avg(volume_b, override='sma').values.tolist()
    
    spread = spread[lookback*2+2:]
    zscore, moving_avg, upper_band, lower_band = zscore[lookback*2+2:], z_moving_avg[lookback*2+2:], upper_band[lookback*2+2:], lower_band[lookback*2+2:]
    timestamps, prices_a, prices_b, volume_a, volume_b = timestamps[lookback*2+2:], prices_a[lookback*2+2:], prices_b[lookback*2+2:], volume_a[lookback*2+2:], volume_b[lookback*2+2:]
    volume_a_ma, volume_b_ma = volume_a_ma[lookback*2+2:], volume_b_ma[lookback*2+2:]

    for i in range(0,len(zscore)): 
        cloud.execute("INSERT INTO pair_data_" + str(pair_number) + " (timestamp, zscore, upper_band, lower_band, moving_average, spread, prices_a, prices_b, volume_a, volume_b, volume_ma_a, volume_ma_b) VALUES (" + str(timestamps[i]) + "," + str(zscore[i]) + "," + str(upper_band[i]) + "," + str(lower_band[i]) + "," + str(moving_avg[i]) + "," + str(spread[i]) + "," + str(prices_a[i]) + "," + str(prices_b[i]) + "," + str(volume_a[i]) + "," + str(volume_b[i]) + "," + str(volume_a_ma[i][0]) + "," + str(volume_b_ma[i][0]) +")")
    
    #create row in current_data if it does not exist. 
        
    res = cloud.execute("SELECT * FROM current_data WHERE EXISTS (SELECT * FROM current_data WHERE pair_number = " + str(pair_number) + ")")
    data = res.all()

    if len(data)==0:
        cloud.execute("INSERT INTO current_data (pair_number) VALUES (" + str(pair_number) + ")")

    return hedge_ratio

#Crypto specific functions 

def update_global_data_crypto(ticker1, ticker2, data, hedge_ratio, global_data=[]): 

    duration = data['duration']
    bar_size = data['bar_size']
    global_data_max_size = data['global_data_max_size']

    update, replace_df = False, False

    if len(global_data) == 0:  
        logger.info('Global data is empty, initializing data!')
        replace_df = True
    else:
        latest_time_pulled = get_prices_crypto_websocket(ticker1, bar_size, 2, ohclv=False, return_all_data=True) #latest_time_pulled = get_prices_crypto(ticker1, bar_size, 2, return_all_data=True)
        delta_time = int(abs((latest_time_pulled[0][0] - latest_time_pulled[1][0])/1000)) #in seconds

        latest_df_time = global_data['Time'].iloc[0]
        differential = latest_time_pulled[0][0]- latest_df_time
        if differential > 0: 
            delta_bars = ((latest_time_pulled[0][0] - latest_df_time)/(delta_time*1000))*1.2
            if delta_bars > len(global_data): 
                replace_df = True #update entire dataframe
        elif differential == 0: 
            update = True
            
    data1 = get_prices_crypto_websocket(ticker1, bar_size, duration, ohclv=True, return_all_data=True) #data1 = get_prices_crypto(ticker1,bar_size, math.ceil(duration),return_all_data=True) 
    data2 = get_prices_crypto_websocket(ticker2, bar_size, duration, ohclv=True, return_all_data=True)  #data2 = get_prices_crypto(ticker2,bar_size, math.ceil(dur  ation),return_all_data=True)
    
    data1 = data1[:global_data_max_size]
    data2 = data2[:global_data_max_size]

    data1, data2 = data1[::-1], data2[::-1] #needed to reverse list

    times = [data1[i][0] for i in range(len(data1))]

    data1_close = [data1[i][4] for i in range(len(data1))]
    data2_close = [data2[i][4] for i in range(len(data2))]

    data1_low = [data1[i][3] for i in range(len(data1))]
    data2_low = [data2[i][3] for i in range(len(data2))]

    data1_high = [data1[i][2] for i in range(len(data1))]
    data2_high = [data2[i][2] for i in range(len(data2))]

    volume_a = [data1[i][5] for i in range(len(data1))]
    volume_b = [data2[i][5] for i in range(len(data2))]
    volume_ma_a = volume_a
    volume_ma_b = volume_b

    spread = get_spread(hedge_ratio, prices_a = data1_close, prices_b = data2_close, global_data=global_data) #this needs to be looked at 

    zscore, upper_band, lower_band, moving_avg  = get_data(spread=spread) #this needs to be looked at 

    times, data1_close, data1_low, data1_high, data2_close, data2_low, data2_high, spread, zscore, upper_band, lower_band, moving_avg = times[::-1], data1_close[::-1], data1_low[::-1], data1_high[::-1], data2_close[::-1], data2_low[::-1], data2_high[::-1], spread[::-1], zscore[::-1], upper_band[::-1], lower_band[::-1], moving_avg[::-1]
    volume_a, volume_b, volume_ma_a, volume_ma_b  = volume_a[::-1], volume_b[::-1], volume_ma_a[::-1], volume_ma_b[::-1]

    df = pd.DataFrame({'Time':times, 'Ticker 1 Price':data1_close,'Ticker 1 Low':data1_low, 'Ticker 1 High':data1_high,'Ticker 2 Price':data2_close, 'Ticker 2 Low':data2_low, 'Ticker 2 High':data2_high,'Spread': spread,'Zscore': zscore, 'Upper Band': upper_band, 'Lower Band': lower_band, 'Moving Average': moving_avg, 'Volume A':volume_a, 'Volume B': volume_b})

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
    tickers = format_ticker(use_list=True, ticker_list = tickers) #need to format these
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
    ticker = format_ticker(tick1=ticker)
    table = 'ohlcv_' + str(ticker).lower() + '_' + str(bar_size)
    if return_all_data == False: 
        res = pd.DataFrame(cloud.execute("SELECT close FROM " + str(table) + " ORDER BY timestamp desc").all())['close'].values.tolist()
        return res[:bars] #results are returned most recent bar first
    else:
        res = pd.DataFrame(cloud.execute("SELECT * FROM " + str(table) + " ORDER BY timestamp desc").all()).values.tolist()
        return res[:bars]

def check_entry_signals(global_data):

    if volume_signal == True: 
        last_5bars_vol_a = max(global_data['Volume A'].iloc[:5].values.tolist())
        last_5bars_vol_b = max(global_data['Volume B'].iloc[:5].values.tolist())

        vol_ma_a = get_moving_avg(global_data['Volume A'][::-1], override='sma')['Volume A'].values.tolist()
        vol_ma_b = get_moving_avg(global_data['Volume B'][::-1], override='sma')['Volume B'].values.tolist()

        if vol_notrade_multiple*vol_ma_a[-1] < last_5bars_vol_a or vol_notrade_multiple*vol_ma_b[-1] < last_5bars_vol_b:
            return False

    return True

def run_script_crypto(ticker1, ticker2, data, current_state=current_state, last_pos=last_pos):

    entry_signal_flag = True 
    internal_id = 100000
    reset_flag = False
    pair_number = data['pair_number']
    bar_size = data['bar_size']

    logger.info(f'Starting Script for pair {pair_number}! Running pre-initialization functions!')

    ticker1_formatted, ticker2_formatted = format_ticker(tick1=ticker1, ticker2=ticker2)
    hedge_ratio = check_tables(data, ticker1, ticker2)

    check_websockets(data, ticker1_formatted, ticker2_formatted, bar_size, ohlcv=ohlcv)
    
    global_data = update_global_data_crypto(ticker1, ticker2,data, hedge_ratio) #this will trip an error, need to fix the global data
    current_state = get_current_state(data)

    new_quantity_a, new_quantity_b = get_quantities(ticker1, ticker2, global_data, data, hedge_ratio) 

    internal_id = set_positions_database(internal_id, ticker1, ticker2, new_quantity_a, new_quantity_b, None, current_state,data,use_crypto=True)

    logger.info('Starting pair ' + str(pair_number))

    while True: 
        global_data = update_global_data_crypto(ticker1, ticker2, data, hedge_ratio, global_data=global_data)

        curr_zscore, curr_moving_avg, curr_upper_band, curr_lower_band = global_data['Zscore'].iloc[0], global_data['Moving Average'].iloc[0], global_data['Upper Band'].iloc[0], global_data['Lower Band'].iloc[0]

        if use_stoploss == True: 
            internal_id = check_exit_conditions(internal_id, global_data, ticker1, ticker2, new_quantity_a, new_quantity_b,current_state, data, percent_stop=True)

        prior_state = current_state

        current_state, reset_flag, trade_status = decide_positions(curr_zscore, curr_moving_avg, curr_upper_band, curr_lower_band,reset_flag, current_state)

        if prior_state != None: 
            prior_state = prior_state.lower()
        if current_state != None: 
            current_state = current_state.lower()

        if current_state == None: 
            #for now we will only avoid entering new trades
            entry_signal_flag = check_entry_signals(global_data)

        if (prior_state != current_state) and reset_flag == False:
            if prior_state == None and entry_signal_flag == False: 
                logger.info('Trade averted due to entry signal flag being false')

            else: 
                logger.info(f"Position has changed, submitting trades. The current state is {current_state} and the previous state is {prior_state}")
                internal_id = set_positions_database(internal_id, ticker1, ticker2, new_quantity_a, new_quantity_b, prior_state, current_state,data,use_crypto=True)
            
        record_data(data, ticker1, ticker2, current_state, global_data, use_crypto=True) 
        update_websockets(ticker1_formatted, ticker2_formatted, bar_size,ohlcv=ohlcv) #updates the heartbaet on our websockets.py

        time.sleep(1)

#Required -- Pair Number, Ticker1, Ticker2, Lookback Period (reference only), Bar Size, Spread Lookback Period (Operational), Std Dev
def run_crypto(pair_number1, ticker1, ticker2, bar_size, lookback,sigma,order_val_input=total_order_value, avg_type_input=average_type,stop_loss=max_loss,stoploss_break=break_on_loss, band_vol_exp=bollinger_vol_exp, bol_band_type=bollinger_type):
#def run_crypto(pair_number1, tickers, bar_size, lookback,sigma,order_val_input=total_order_value, avg_type_input=average_type,stop_loss=max_loss,stoploss_break=break_on_loss, band_vol_exp=bollinger_vol_exp, bol_band_type=bollinger_type):
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
    
    run_script_crypto(ticker1, ticker2, data)

if __name__ == "__main__":
    create_threads(read_arguments_from_file('config.json'), run_crypto)
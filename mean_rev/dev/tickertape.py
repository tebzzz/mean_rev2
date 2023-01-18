import statistics
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import math
from statistics import * 
import random
import sqlite3 
import ccxt
import csv
import time
import disco
from pykalman import KalmanFilter
import sqlalchemy
import yfinance as yf

from datetime import datetime, timedelta
from ib_insync import *

import warnings
warnings.filterwarnings("ignore")
#212 870 6331

username = 'postgres'  # DB username
password = 'proddb123'  # DB password
host = '34.84.190.250'  # Public IP address for your instance
port = '5432'
database = 'prod'  # Name of database ('postgres' by default)

db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
    username, password, host, port, database)

engine = sqlalchemy.create_engine(db_url)
cloud = engine.connect()

conn = sqlite3.connect('test.db')
d = conn.cursor()

exchange = ccxt.binanceusdm() 

status = None 
equity_curve = []

tickers = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

#data
bar_period = '1d'
bars_back = 499
bar_model_time = 1000 #***the model period must be deleted from the backtest, and only the out of sample data can be tested

#model stats
model_date_start = None
mode_date_end = None

flag_sigma = 5
entry_sigma, exit_sigma = 2.75, None
zscore_avg_period = None #Need to implement this... What is this? 

lookback = 20
average_type = 'sma' #ema, sma, or kalman
band_type = 'bollinger'

#Keltner Channel
keltner_multiplier = 5

#Bolling Bands
bollinger_type = 'sma' #ema or sma
bollinger_vol_exp = 1 #volatility exponent for a bollinger bandp

#Second Band Variables
use_second_band = False
second_band_sigma = 3.4

#Various Options 
use_indicators = False
include_fees = True

#Stop variables
use_stops = False
stop_percent_loss = 0.0005

#Volume indicator variables
use_vol_indicator = False
vol_multiplier = 7

manual_model_entry = True
manual_model = {'BNBUSDT':1.13862006e-02,'BTCUSDT':2.40162555e-06, 'ETHUSDT':-1.28487443e-03}

#Database specific functions

def delete_locks(table):
    #get all of the locks and select the ones we want to delete
    locks = cloud.execute("SELECT t.relname, l.locktype, page, virtualtransaction, pid, mode, granted FROM pg_locks l, pg_stat_all_tables t WHERE l.relation = t.relid ORDER BY relation asc")

    #delete them
    for lock in locks:
        if lock[0] == table:
            cloud.execute("SELECT pg_terminate_backend(" + str(lock[4]) + ")")

#multivariate functions 

def get_model(bars_back_start, bars_back_length, assets):
    model = {}

    if manual_model_entry == True:
        model = manual_model
    else: 
        model = create_multivariate_model(bars_back_start, bars_back_length, assets)

    return model

def create_multivariate_model(bars_back_start=None, bars_back_length=None, assets=tickers):
    values, model = {}, {}

    for a in assets:
        prices, data, volume = get_prices_crypto(a, bars_back_start=bars_back_start, bars_back_override=bars_back_length)
        values[a] = prices 

    values = pd.DataFrame(values)

    jres = coint_johansen(values, det_order=0, k_ar_diff=1)
    vals = pd.DataFrame(jres.evec)

    for i in range(len(assets)):
        model[assets[i]] = vals[0][i]
    return model

def get_multivariate_price_time_series(bars_back, model):

    coeff = {}
    values = {}

    for a in model:
        prices, data, volume = get_prices_crypto(a, bars_back_override=bars_back) #we will need to get 
        values[a] = prices  
        coeff[a] = model[a]

    prices = pd.DataFrame(values)
    values = list(coeff.values())

    print(prices)
    print(values)
    
    # print(break123)

    portf_n_assets = (prices * values).sum(axis=1) #*** this may be inaccurate
    price_time_series = portf_n_assets

    return price_time_series

#indicator functions 

def plot_research_indicators():
    df_indicators = pd.DataFrame()
    df_indicators.to_sql(name='display_indicators', con=cloud, if_exists='replace')

def vol_indicator(i, volume_array, volume_ma_array): 
    last_bars = {}

    #this needs to be re-written 

    assets = list(volume_array.keys())

    for a in assets: 

        if len(volume_array[a]) == 1: #this is likely going to have to need to change 
            last_bars[a] = volume_array[a][0]
        else: 
            last_bars[a] = max(volume_array[i:i+5])

    for a in assets: 
        if vol_multiplier*volume_ma_array[a][i] < last_bars[a]:
            return False 

    return True 

#bands 

def get_bollinger_bands(zscore, sigma=entry_sigma):
    zscore_df = pd.DataFrame(zscore)
    moving_avg = get_moving_avg(zscore)
    if bollinger_type == 'ema':
        std = zscore_df.ewm(span=lookback).std()
    else: 
        std = zscore_df.rolling(lookback).std()
    bollinger_up = moving_avg + std**bollinger_vol_exp * sigma 
    bollinger_down = moving_avg - std**bollinger_vol_exp * sigma 
    return bollinger_up, bollinger_down

def get_atr(ohlcv_a, ohlcv_b): #this needs to be reworked if we want to use it 
    spread_low, spread_high ,spread_close =  [], [], []
    moving_avg = get_moving_avg(spread_close)

    #get the ATR, likely need to pull from global data
    tr1 = pd.DataFrame(spread_high - spread_low) 
    tr2 = pd.DataFrame(abs(spread_high - z_close.shift()))
    tr3 = pd.DataFrame(abs(spread_low - z_close.shift()))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis = 1, join = 'inner').max(axis = 1)
    atr = tr.ewm(alpha = 1/lookback).mean()

    return atr, moving_avg

def get_keltner(ohlcv_a, ohlcv_b):
    
    #setting up the data needed

    moving_avg, atr = get_atr(ohlcv_a, ohlcv_b)

    ma_values = [i[0] for i in moving_avg.values]
    atr_values = [i for i in atr.values]
    
    #moivng average could be an ema, ma, kelter channel, or other. 
    kc_upper, kc_lower = [], []

    for i in range(len(ma_values)):
        kc_upper.append(ma_values[i] + keltner_multiplier * atr_values[i])
        kc_lower.append(ma_values[i] - keltner_multiplier * atr_values[i])

    return pd.DataFrame(kc_upper), pd.DataFrame(kc_lower)

def get_kalman_filter(zscore):
    zscore = zscore[lookback*2-2:]

    res = []
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

#math functions for mean rev

def sortino_ratio(equity_curve):#check the math...something is likely wrong....
    if bar_period == '5m':
        intervals = 105120
    elif bar_period == '15m':
        intervals = 35040
    elif bar_period == '30m':
        intervals = 17520
    neg_returns = []
    returns = []
    for i in range(len(equity_curve)-1):
        r = equity_curve[i+1]/equity_curve[i]
        if r >= 1:
            pass
        else: 
            neg_returns.append((1-r)**2)

        returns.append(r)

    avg = sum(returns)/len(returns) - 1

    neg = sum(neg_returns) 
    std = stdev(returns)
    ratio = (avg / std) * intervals**(1/2)

    return ratio

def get_bands(zscore, sigma=entry_sigma):
    if band_type == 'bollinger':
        upper_band, lower_band = get_bollinger_bands(zscore)
    # Keltner needs some adjustment for multivariate 
    # elif band_type == 'keltner':
    #     upper_band, lower_band = get_keltner(ohlcv_a, ohlcv_b)
    return upper_band, lower_band

def get_prices_crypto(ticker, bars_back_override=None, bars_back_start=None):
    global bars_back
    bars_back_flag = False

    if bars_back_override: 
        bars_back = bars_back_override

    if bars_back_start and bars_back_override:
        bars_back =  bars_back_start
        bars_back_flag = True

    try:
        volume = []

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
                print('Please enter an appropriate time period or add the timedelta!')
                print(Break123)

            now = exchange.milliseconds()
            res = math.floor(bars_back/1000)

            for i in range(1,res+1):
                date_list.append(now-(time_delta*i*1000*1000))

            date_list = date_list[::-1]

            for date in date_list:
                data = data + exchange.fetch_ohlcv(ticker,bar_period,since=date,limit=1000)

            # if bars_back%1000 != 0:
            #     extra = bars_back%1000
            #     e_date = date_list[0]+extra*time_delta
            #     data = exchange.fetch_ohlcv(ticker,bar_period,since=e_date,limit=extra) + data

        prices = []

        for d in data: 
            prices.append(d[4])
            volume.append(d[-1])
    
    except Exception as e:
        print(e)
        print(break123)

    if bars_back_flag: 
        bars_back_override = int(bars_back_override)

        prices, data, volume = prices[:bars_back_override], data[:bars_back_override], volume[:bars_back_override]

    return prices, data, volume

def find_spread_two_asset(price_array):
    keys = list(price_array.keys())
    prices_a = price_array[keys[0]]
    prices_b = price_array[keys[1]]

    m,b = np.polyfit(prices_b,prices_a,1)

    spread = []

    for i in range(len(prices_a)):
        spread.append(math.log(prices_a[i]) - m*math.log(prices_b[i]))    

    position_ratios = {tickers[0]: 1, tickers[1]: m}
     
    return spread, position_ratios

def find_spread_absolute_two_asset(prices_a, prices_b):
    
    m,b = np.polyfit(prices_b,prices_a,1)
    spread = []

    for i in range(len(prices_a)):
        spread.append(prices_a[i] - m*prices_b[i])
    
    return spread

def get_moving_avg(prices, override=None):
    prices_df = pd.DataFrame(prices)
    if not override: 
        type = average_type
    else: 
        type = override
    if type =='sma':
        ma_df = prices_df.rolling(lookback).mean()
    elif type == 'ema':
        ma_df = prices_df.ewm(span=lookback).mean()
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

def get_data(price_array,ohlc_array=None):

    if len(price_array) > 2:

        model = get_model(bars_back_start=bars_back, bars_back_length=bar_model_time, assets=tickers)
        spread = get_multivariate_price_time_series(bars_back, model) 
        zscore = get_zscore(spread)
        position_ratios = model

    elif len(price_array) == 2: 

        spread, position_ratios = find_spread_two_asset(price_array)
        zscore = get_zscore(spread)
    
    z_moving_avg_df = get_moving_avg(zscore)
    band_up_df, band_down_df = get_bands(zscore, ohlc_array) 

    return zscore, band_up_df[0].tolist(), band_down_df[0].tolist(), z_moving_avg_df[0].tolist(), spread, position_ratios

def get_second_band(zscore):

    second_upper_band, second_lower_band = get_bollinger_bands(zscore, sigma = second_band_sigma)

    return second_upper_band[0].tolist(), second_lower_band[0].tolist()

#Loop functions

def stopped_out(dict1, i, tickers, prices, positions, position_prices, curr_val):

    print('Stopped out')

    for ticker in tickers: 
        if positions[ticker] > 0:
            buy_sell = 'sell'
        elif positions[ticker] < 0: 
            buy_sell = 'buy'

        dict1 = write_to_dict(dict1, i, ticker, buy_sell, prices[ticker][i+1], abs(positions[ticker]))
        
    curr_val = determine_equity(status, position_prices, prices, positions, i)
    
    return dict1, curr_val

def write_to_dict(dict1, i, ticker, buy_sell, next_price, shares):
    dict1['Time'].append(i)
    dict1['Ticker'].append(ticker), 
    dict1['Buy/Sell'].append(buy_sell)
    dict1['Price'].append(next_price)
    dict1['Quantity'].append(shares)
    return dict1

def get_fees(position_ratios): 
    total = 0
    val = 0

    #dollar values need to be normalized
    for pos in position_ratios: 

        if pos[-4:] == 'BUSD': 
            val += position_ratios[pos] * 0.000108

        elif pos[-4:] == 'USDT':
            val += position_ratios[pos] * 0.000108

        total += position_ratios[pos]

    fees = val/total

    return fees

def change_positions(new_status, position_ratios, dict1, i, prices): 

    #need to account for when we close the trade to ensure that we log the trades 
    positions = {}
    buy_sell = None 

    assets = tickers

    if new_status == None: 
        return {}

    elif new_status.lower() == 'long': 
        for a in assets: 
            positions[a] = position_ratios[a]

    elif new_status.lower() == 'short':
        for a in assets: 
            positions[a] = position_ratios[a]*-1

    for pos in positions: 
        if positions[pos] > 0: 
            buy_sell = 'buy'
        elif positions[pos] < 0:
            buy_sell = 'sell'

        dict1 = write_to_dict(dict1, i, pos, buy_sell, prices[pos][i+1], abs(positions[pos]))

    return positions 

def set_position_prices(prices, i):

    position_prices = {}

    for ticker in prices: 
        position_prices[ticker] = prices[ticker][i+1]

    return position_prices

def run_stop_check(position_prices, prices, positions, status, equity, position_ratios, dict1, i):

    if use_stops:
        
        if not check_stops(position_prices, prices, positions, status, equity):

            if status == 'long':
                status = "stop_long"

            elif status == 'short':
                status = 'stop_short'

            positions = change_positions(None, position_ratios, dict1)
            stopped_out(dict1, i, tickers, prices, positions, position_prices, equity)

        return status, positions
    else: 
        return status, positions

#Needs to be re-adapted for multivariate
def check_stops(position_prices, prices, positions, status, curr_val): #return true if the conditions pass, return false if no longer cointegrated or we are outside of standard deviation or spread parameters
    current_position_values = []

    # val_a = qty_a*(pos_price_a - curr_a)
    # val_b = qty_b*(curr_b - pos_price_b)
    
    if status == 'Short':

        for price in position_prices:
            current_position_values.append()

    elif status == 'Long':

        for price in position_prices:
            current_position_values.append()

    val = sum(current_position_values)

    if val/curr_val < 100 * stop_percent_loss*-1: 
        return False

    return True

#this function needs to be designed 
def determine_equity(equity, position_prices, prices, positions, i, position_ratios, open_close=None): #I believe something is wrong here and the function needs to be reworked
    delta_val = 0

    #should add a trade open option for this as well
    if not open_close: 
        order_val = i
    else:
        order_val = i+1

    if len(positions) == 0:
        equity_value = equity
        return equity_value, equity

    equity_value = equity

    #this likely needs to be changed
    for pos in positions: 
        #likely right but may need to be updated
        if positions[pos] > 0: 
            delta_val += positions[pos]*(prices[pos][order_val] - position_prices[pos])  
        elif  positions[pos] < 0: 
            delta_val -= abs(positions[pos])*(prices[pos][order_val] - position_prices[pos]) 

    if open_close == 'close': 
        print(delta_val)
        equity += delta_val
    
    else:
        equity_value += delta_val

    #this needs to be tested and likely debugged
    # if (open_close == 'close' or open_close == 'open') and include_fees:
    #     equity = equity * (1-get_fees(position_ratios))

    return equity_value, equity 

def check_indicators(i, volume_array, volume_ma_array):
    if use_vol_indicator: 
        res = vol_indicator(i, volume_array, volume_ma_array)

    if res == False: 
        print('Passed due to an indicator!')

def get_quantities(prices, position_ratios,equity):
    ratio_capital = 0
    qty = {}

    for ticker in position_ratios:
        ratio_capital += abs(position_ratios[ticker])*prices[ticker][0]

    multiplier = equity/ratio_capital

    for ticker in position_ratios: 
        qty[ticker] = position_ratios[ticker]*multiplier

    return qty

#Different types of backtesting loops 
def run_loop(z_moving_avg, zscore, upper_band, lower_band, prices, position_ratios, volume_array=None, volume_ma_array=None):

    dict1 = {'Time': [],'Ticker':[],'Buy/Sell':[],'Price':[],'Quantity':[]}
    equity = 10000
    status = None

    positions = {}
    position_prices = {}
    position_quantities = get_quantities(prices, position_ratios, equity)

    for i in range(len(zscore)):

        if status == 'Long' or status == 'Short':
            print(status)

        if status == None: 

            #res = check_indicators(i, volume_array, volume_ma_array)
            # if not res: 
            #     equity_curve.append(equity)
            #     continue 

            if zscore[i] > upper_band[i]:

                status = 'Short'

                positions = change_positions(status, position_quantities, dict1, i, prices)
                position_prices = set_position_prices(prices, i)
                equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities, open_close='open')

            elif zscore[i] < lower_band[i]:

                status = 'Long'

                positions = change_positions(status, position_quantities, dict1, i, prices)
                position_prices = set_position_prices(prices, i)
                equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities, open_close='open')

            else:
                equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities)
                status = None

        elif status == 'Long':

            if zscore[i] < z_moving_avg[i]:

                status = 'Long'

                equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities)
                status, positions = run_stop_check(position_prices, prices, positions, status, equity, position_quantities, dict1, i)

            elif zscore[i] >= z_moving_avg[i]:

                status = None

                equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities, open_close='close')
                positions = change_positions(status, position_quantities, dict1, i, prices)
            
        elif status == 'Short':

            if zscore[i] > z_moving_avg[i]:

                status = 'Short'
                equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities)
                status, positions = run_stop_check(position_prices, prices, positions, status, equity, position_quantities, dict1, i)

            elif zscore[i] <= z_moving_avg[i]:

                status = None
                equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities, open_close='close')
                positions = change_positions(status, position_quantities, dict1, i, prices)

        elif status == 'stop_long': 

            equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities, trade_close = 'close-stop')

            if zscore[i] >= z_moving_avg[i]: 
                status = None 

        elif status == 'stop_short':

            equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities, trade_close = 'close_stop')

            if zscore[i] <= z_moving_avg[i]:
                status = None

        equity_curve.append(equity_value)
        print("Equity value: "  + str(equity_value))
        print("Equity: " + str(equity))

    return equity_curve, dict1

def run_loop_two_bands(z_moving_avg, zscore, upper_band, lower_band, second_upper_band, second_lower_band, prices, position_quantities):

    dict1 = {'Time': [],'Ticker':[],'Buy/Sell':[],'Price':[],'Quantity':[]}
    equity = 10000
    status = None
    band_flag = False

    position_quantities = get_quantities(prices, position_ratios, equity)

    for i in range(len(zscore)):

        if band_flag == False:

            if zscore[i] >= second_upper_band[i]:
                band_flag = True 
                equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities)

            elif zscore[i] <= second_lower_band[i]:
                band_flag = True
                equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities)

            else: 
                equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities)

        elif band_flag == True: 

            if status == None: 

                if zscore[i] < upper_band[i] and zscore[i] > z_moving_avg[i]:
                    
                    status = 'Short'

                    positions = change_positions(status, position_quantities, dict1, i, prices)
                    position_prices = set_position_prices(prices, i)
                    equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities)

                elif zscore[i] > lower_band[i] and zscore[i] < z_moving_avg[i]:
                    status = 'Long'

                    position_prices = set_position_prices(prices, i)
                    equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities)
                    positions = change_positions(status, position_quantities, dict1, i, prices)

                else:
                    equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities)
                    status = None

            elif status == 'Long':
                if zscore[i] < z_moving_avg[i]:
                    status = 'Long'

                    equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities)

                elif zscore[i] >= z_moving_avg[i]:
                    band_flag = False
                    status = None

                    equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities, trade_close=True)
                    positions = change_positions(status, position_quantities, dict1, i, prices)
                    
                
            elif status == 'Short':
                if zscore[i] > z_moving_avg[i]:
                    status = 'Short'

                    equity_value, equity = determine_equity(equity, position_prices, prices, positions, i, position_quantities)

                elif zscore[i] <= z_moving_avg[i]:
                    band_flag = False
                    status = None

                    equity_value, equity  = determine_equity(equity, position_prices, prices, positions, i, position_quantities, trade_close=True)
                    positions = change_positions(status, position_quantities, dict1, i, prices)

        equity_curve.append(equity_value)

    return equity_curve, dict1

def get_volume_ma_array(volume_array):
    volume_ma_array = {}
    
    for ticker in volume_array: 
        volume_ma = get_moving_avg(volume_array[ticker], override='sma')[lookback*2-2:][0].values.tolist()
        volume_ma_array[ticker] = volume_ma

    return volume_ma_array

def tickertape(prices, lookback, ohlc_array, volume_array):

    equity_curve = []
    zscore, upper_band, lower_band, z_moving_avg, spread, position_ratios = get_data(prices, ohlc_array)

    #special cases and indicators here
    if use_vol_indicator: 
        volume_ma_array = get_volume_ma_array(volume_array)
    else: 
        volume_ma_array = None

    if use_second_band: 
        second_upper_band, second_lower_band = get_second_band(zscore, ohlc_array)
        second_upper_band, second_lower_band = second_upper_band[lookback*2-2:], second_lower_band[lookback*2-2:]

    spread = spread[lookback*2-2:]
    zscore = zscore[lookback*2-2:]
    z_moving_avg = z_moving_avg[lookback*2-2:]
    upper_band = upper_band[lookback*2-2:]
    lower_band = lower_band[lookback*2-2:]

    prices_adj = {}
    for p in prices: 
        prices_adj[p] = prices[p][lookback*2-2:]

    #ensure the moving average arrays get cut down as well (may be done in a different function)

    if use_second_band: 
        equity_curve, dict1 = run_loop_two_bands(z_moving_avg, zscore, upper_band, lower_band, second_upper_band, second_lower_band, prices_adj, position_ratios, volume_array, volume_ma_array)
        return equity_curve, dict1, zscore, upper_band, lower_band, second_upper_band, second_lower_band, z_moving_avg, spread
    else: 
        equity_curve, dict1 = run_loop(z_moving_avg, zscore, upper_band, lower_band, prices_adj, position_ratios, volume_array, volume_ma_array)
        return equity_curve, dict1,zscore, upper_band, lower_band, z_moving_avg, spread

def main(tickers): 
    price_array = {}
    ohlc_array = {}
    volume_array = {}

    for ticker in tickers:
        prices, ohcl, volumes = get_prices_crypto(ticker)
        price_array[ticker] = prices
        ohlc_array[ticker] = ohcl
        volume_array[ticker] = volumes

    #add and format volume to the rest
    if use_second_band:
        equity_curve, dict1, zscore, upper_band, lower_band, second_upper_band, second_lower_band, z_moving_avg, spread = tickertape(price_array, lookback, ohlc_array, volume_array)
    else:
        equity_curve, dict1, zscore, upper_band, lower_band, z_moving_avg, spread = tickertape(price_array, lookback, ohlc_array, volume_array)

    df_trades = pd.DataFrame.from_dict(dict1)

    for vol in volume_array:
        volume_ma_array = {}
        volume_ma_array[vol] = pd.Series(volume_array[vol]).rolling(lookback).mean().tolist()

    #cut volume and volume MA arrays down
    #need to be put into some sort of chart as well (or combined with the current one)

    #error here, not everthing is the same length
    if use_second_band:
        df = pd.DataFrame({'Equity': equity_curve,'Z-Score':zscore, 'Upper Band': upper_band, 'Lower Band': lower_band, 'Second Upper Band': second_upper_band, 'Second Lower Band': second_lower_band, 'Moving Average': z_moving_avg,'Spread':spread})
    else:
        df = pd.DataFrame({'Equity': equity_curve,'Z-Score':zscore, 'Upper Band': upper_band, 'Lower Band': lower_band, 'Moving Average': z_moving_avg,'Spread':spread})
    #plot_research_indicators()

    #cloud.execute("DELETE FROM backtest_trades")
    delete_locks('backtest_trades')
    df_trades.to_sql(name='backtest_trades', con=cloud, if_exists='replace')
    delete_locks('backtest_data')
    df.to_sql(name='backtest_data', con=cloud, if_exists='replace')

    return(equity_curve[-1])

#discovery/backtest functions

def run_discovery(bar_times):
    res = disco.multiple_test(bar_times)
    bulk_test(res)

def bulk_test(list1,val=110): #runs many tickers on one scenerio to see the results 
    high = []
    for pair in list1:
        try: 
            res = main(pair[0], pair[1])
            print(pair)
            if res > val:
                high.append(pair)
            print(high)
        except:
            print('Pair Failed')

def run_fowrward_test(windows): #should do this to get the results quickly
    results = []

    for w in windows:
        model_start = w[0]
        model_end = w[1]
        start = w[2]
        end = w[3]
        
        #run the backtest and append to results
        res = tickertape()
        results.append(res)

    print(results)

            
if __name__ == '__main__': 

    main(tickers)

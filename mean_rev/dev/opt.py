import statistics
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss
import math
from statistics import * 
import matplotlib.pyplot as plt
import random
import quantstats as qs
import sqlite3
import ccxt
import tickertape

from datetime import datetime, timedelta
from ib_insync import *

#finish test std dev function
#*** there is an error somewhere in here to debug later!

exchange = ccxt.binance() 
bar_period = '1h'
bars_back = 1000

today = datetime.now()

conn = sqlite3.connect('test.db')
d = conn.cursor()

bar_size= '1 day' 
duration = '5 Y'
from_date = today 
status = None 
equity_curve = []
entry_sigma = 3
moving_avg_period = 50

num = round(random.random()*100)
gateway_port = 4002

# ib = IB()
# ib.connect(host='127.0.0.1', port=gateway_port, clientId=num) 

capital, curr_val = 100, 0
shares_a, shares_b = 0, 0

def sharpe_ratio(equity_curve = None, return_curve = None, rfr=0/250):
    if equity_curve == None and return_curve == None: 
        print('Please ensure there is a equity curve or return curve!')
        return None
    if len(equity_curve) < 2:
        return 0
    elif return_curve != None:
        #we have the return curve, moving on
        pass
    elif equity_curve != None: 
        #need to turn equity curve into return curve 
        return_curve = []
        for i in range(len(equity_curve)-1):
            return_curve.append(equity_curve[i+1]/equity_curve[i]-1)
    else:
        print('Something went wrong finding sharpe ratio!')
        return None

    mean = statistics.mean(return_curve)
    stdev = np.std(return_curve)

    return (mean-rfr)/stdev * 255**(1/2)

#support functions
def find_spread(ticker_a_prices, ticker_b_prices):
    global hedge_ratio

    m,b = np.polyfit(ticker_a_prices,ticker_b_prices,1)
    spread = []
    
    hedge_ratio = m

    for i in range(len(ticker_a_prices)):
        spread.append(math.log(ticker_a_prices[i]) - m*math.log(ticker_b_prices[i]))    
    
    return spread

def get_sma(prices, rate):
    df_sma = prices.rolling(rate).mean()
    sma_list = [df_sma.values.tolist()[i][0] for i in range(len(df_sma))]
    return df_sma, sma_list

def get_zscore(spread, moving_avg, std):
    zscore = []
    for i in range(len(spread)):
        spd = spread[i]
        mva = moving_avg.values.tolist()[i][0]
        std1 = std.values.tolist()[i][0]
        zscore.append((spd-mva)/std1)

    return zscore

def get_historical_prices(ticker1, bar_size1=bar_size, duration1=duration):
    today = datetime.today()
    data1 = []
    stock = Stock(ticker1, 'SMART','USD')
    ib.qualifyContracts(stock)

    data = ib.reqHistoricalData(stock, from_date, duration1,bar_size1, 'ASK', True, formatDate=1, keepUpToDate=False, chartOptions=[], timeout=60)
    for d in data:
        data1.append(d.close)
    return data1

def get_prices_crypto(ticker):
    data = exchange.fetch_ohlcv(ticker,bar_period,limit=bars_back)
    prices = []

    for d in data: 
        prices.append(d[4])

    return prices
    
#different tests

def test_lookback(ticker1, ticker2): #need to manually adjust the parameters 
    if len(ticker1) > 5:
        prices_a = get_prices_crypto(ticker1)
    else: 
        prices_a = get_historical_prices(ticker1)
    if len(ticker2) > 5:
        prices_b = get_prices_crypto(ticker2)
    else: 
        prices_b = get_historical_prices(ticker2)

    arr = [i*10 for i in range(1,40)]
    returns, sharpe_arr = [], []

    for a in arr: 
        print(a)
        equity_curve, final_capital = tickertape.tickertape(prices_a, prices_b, a)
        sharpe = sharpe_ratio(equity_curve=equity_curve)
        returns.append(final_capital)
        sharpe_arr.append(sharpe*100)
        print(final_capital)

    return arr, returns, sharpe_arr

def test_barsize(ticker1, ticker2):
    #sizes = {240:'30 mins', 120:'1 hour', 60:'2 hours', 30:'4 hours', 15:'1 day'}
    sizes = {600:'2 hours', 300:'4 hours', 150:'1 day'}
    arr = [i for i in sizes.keys()]
    print(arr)
    returns = []

    for s in sizes.keys():

        prices_a, prices_b = get_historical_prices(ticker1, bar_size1 = sizes[s]), get_historical_prices(ticker2, bar_size1 = sizes[s])
        equity_curve, final_capital = tickertape.tickertape(prices_a, prices_b, s)
        returns.append(final_capital)

    return arr, returns

def test_st_dev(ticker1, ticker2): #should work, has not been bug tested 
    st_dev, returns  = [0.5,0.75,1,1.25,1.5,1.75,2,2.25,2.5,2.75,3], []
    prices_a, prices_b = get_historical_prices(ticker1), get_historical_prices(ticker2)
    
    for dev in st_dev:
        equity_curve, final_capital = tickertape.tickertape(prices_a, prices_b, moving_avg_period, st_dev=dev) #add sharpe 
        returns.append(final_capital)
    print(final_capital)

    return st_dev, returns


def mult_pair_returns(pairs):
    returns = {}
    for pair in pairs: 
        prices_a, prices_b = get_historical_prices(pair[0]), get_historical_prices(pair[1])
        equity_curve, curr_val = tickertape.tickertape(prices_a, prices_b, 100)
        ret = test_lookback(pair[0], pair[1])
        returns[str(pair)] = ret
    return returns


def plot_data(df_list): #going to need to change the way this is fundamentally done, might want to use sqlite 
    if df_list == 1:
        df_list[0].to_csv('opt_chart.csv')

def lookback_sigma(lookback_range, interval,st_dev_list):
    pass


if __name__ == '__main__': #this should be checked, but I think it is correct

    # prices_a, prices_b = get_prices_crypto('ADA/USDT'), get_prices_crypto('ZEC/USDT')
    # equity_curve, curr_val = tickertape(prices_a, prices_b, 17)
    # print(curr_val)
    # print(break123)

    x, y, sharpe_arr = test_lookback('ADA/USDT', 'OCEAN/USDT')

    #write ret and sharpe_df imto sqlite 
    d.execute("DROP TABLE optimize")
    df = pd.DataFrame({'X': x , 'returns': y, 'sharpe': sharpe_arr})
    df.to_sql(name='optimize', con=conn)


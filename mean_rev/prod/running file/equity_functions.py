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

def reconnect(): 
    logger.info('Connection was likely disconnected. Pausing for 10 minutes and then will attempt to reconnect.')
    now = market_time()
    logger.info('The current time is ' + str(now))
    ib.sleep(600)
    logger.info("Attempting to reconnect")
    try: 
        num = round(random.random()*100)
        ib.connect(host='127.0.0.1', port=gateway_port, clientId=num) 
        ib.sleep(5)
    except Exception as e: 
        logger.info(e)

def get_historical_prices(ticker1, complete_data=False):
    today = datetime.today()
    data1 = []
    stock = Stock(ticker1, 'SMART','USD')
    ib.qualifyContracts(stock)

    data = ib.reqHistoricalData(stock, today, duration_lookback, bar_size_lookback, 'MIDPOINT', True, formatDate=1, keepUpToDate=False, chartOptions=[], timeout=60)
    if complete_data == False: 
        for d in data:
            data1.append(d.close)
        return data1
    elif complete_data == True: #***Probably will need to complete but setting complete data to true will get all ohlcv data
        return data

def get_price(ticker, return_time=False):
    stock = Stock(ticker, 'SMART', 'USD')
    ib.qualifyContracts(stock)
    ticker = ib.reqTickers(stock)
    price = ticker[0].last

    if return_time == True:
        dur = bar_format(3, bar_size_lookback)
        time1 = market_time()
        data = ib.reqHistoricalData(stock, time1, dur, bar_size_lookback, 'MIDPOINT', True)
        return time.mktime(data[-1].date.timetuple())

    if math.isnan(price) == True: #if we cannot get last_price for some reason, we pull the most recent price from historical data. 
        today = datetime.now()
        data = ib.reqHistoricalData(stock, today, '1 M', '1 day', 'MIDPOINT', True, formatDate=1, keepUpToDate=False, chartOptions=[], timeout=60)
        price = data[-1].open

    return price

def market_time():
    for i in range(0,100):
        if i%2 == 0:
            try:
                response = c.request('time.google.com')
                now = datetime.fromtimestamp(response.tx_time, pytz.timezone('US/Eastern'))
                break 
            except Exception as e: 
                logger.error(e)
        else: 
            try:
                response = c.request('pool.ntp.org')
                now = datetime.fromtimestamp(response.tx_time, pytz.timezone('US/Eastern'))
                break 
            except Exception as e: 
                logger.error(e)

    return now

def get_current_positions(security_a, security_b):
    positions = ib.positions()
    sec_a_amount = 0
    sec_b_amount = 0

    for pos in positions: 
        if pos[1].secType == 'STK' and pos[1].symbol == security_a.upper():
            sec_a_amount = int(pos[2]) ## &need to verify this contract

    for pos in positions: 
        if pos[1].secType == 'STK' and pos[1].symbol == security_b.upper():
            sec_b_amount = int(pos[2])

    return sec_a_amount, sec_b_amount

def market_open():
    try: 
        contract = Stock('SPY', 'SMART', 'USD')
        ib.qualifyContracts(contract)
        d = ib.reqContractDetails(contract)
        times = d[0].liquidHours.split(';')
        array = []
        for time in times: 
            a = time.split('-')
            b =  a[0].split(':')
            if b[1] == 'CLOSED':
                continue
            array.append([datetime.strptime(a[0], '%Y%m%d:%H%M'),datetime.strptime(a[1], '%Y%m%d:%H%M')])

        now = market_time()
        est = pytz.timezone('US/Eastern')

        for periods in array: 
            start = est.localize(periods[0])
            end = est.localize(periods[1])
            if start <= now <= end: 
                return True
    except Exception as e: 
        logger.info(e)
        logger.info('There was an issue seeing if the market was open, attempting to reconnect!')
        reconnect()

    return False

def bar_format(bars): #*** returns the number of bars to 
    bar_size_lookback=bar_size_lookback
    if bar_size_lookback == '1 day' or bar_size_lookback == '8 hours' or bar_size_lookback == '4 hours' or bar_size_lookback == '2 hours' or bar_size_lookback == '1 hour':
        mult = 1
        if bar_size_lookback == '4 hours':
            mult = 2
        if bar_size_lookback == '2 hours':
            mult = 4 
        if bar_size_lookback == '1 hour':
            mult = 8
        if bars < 7*mult: 
            return str(math.ceil(bars/mult)) + " D"
        elif bars < 30*mult:
            return str(math.ceil(bars/7*mult)) + " W"
        elif bars < 365*mult:
            return str(math.ceil(bars/30*mult)) + " M"
        else:    
            return str(math.ceil(bars/365*mult)) + " Y"

    elif bar_size_lookback == '30 mins' or bar_size_lookback == '20 mins' or bar_size_lookback == '15 mins' or bar_size_lookback == '10 mins' or bar_size_lookback == '5 mins' or bar_size_lookback == '3 mins' or bar_size_lookback == '2 mins' or bar_size_lookback == '1 min' or bar_size_lookback == '30 sec':
        mult = 1
        if bar_size_lookback == '20 mins':
            mult = 1.5
        if bar_size_lookback == '15 mins':
            mult = 2
        if bar_size_lookback == '10 mins':
            mult = 3
        if bar_size_lookback == '5 mins': 
            mult = 6
        if bar_size_lookback == '3 mins': 
            mult = 10
        if bar_size_lookback == '2 mins': 
            mult = 15
        if bar_size_lookback == '1 min': 
            mult = 30
        if bar_size_lookback == '30 sec':
            mult = 60
        if bars < 336*mult: 
            return str(math.ceil(bars/(48*mult))) + " D"
        elif bars < 1344*mult:
            return str(math.ceil(bars/(336*mult))) + " W"
        else:
            return str(math.ceil(bars/1344*mult)) + " M"

def run_script(security_a, security_b, current_state=current_state, last_pos=last_pos): #used to run the script from the outside 
    global global_data

    check_tables()
    market_time()
    update_global_data(security_a, security_b)
    new_quantity_a, new_quantity_b = get_quantities()
    current_state = get_current_state()
    quantity_a, quantity_b = equity.get_current_positions(security_a, security_b)
    check_status = check_status_func(current_state, quantity_a, quantity_b)

    while True: 
        equity.market_time()
        update_global_data(security_a, security_b)
        now = equity.market_time()
        zscore, upper_band, lower_band, moving_avg = get_data()
        try:
            if market_open() == True: 

                if use_stoploss == True: 
                    if check_exit_conditions() == False:
                        logger.info("We are outside of our stop parameters, stopping program")
                        print(break123)
            
                zscore, upper_band, lower_band, moving_avg = get_data()
                
                prior_state = current_state

                current_state = decide_positions(zscore[-1], moving_avg[-1], upper_band[-1], lower_band[-1], current_state)

                if prior_state != None: 
                    prior_state = prior_state.lower()
                if current_state != None: 
                    current_state = current_state.lower()

                if prior_state != current_state or check_status == False:
                    logger.info('Position has changed, submitting trades')
                    check_status = True

                quantity_a, quantity_b = equity.get_current_positions(security_a, security_b)

                logger.info(str(now) + ' ' + str(current_state).upper() + ' ' + str(security_a).upper() + ':' + str(quantity_a) + ' ' + str(security_b).upper() + ':' + str(quantity_b))

                record_data(security_a, security_b, current_state) #records the data into sqlite 
            
            elif market_open() == False:    
                logger.info('Market is closed ' + str(now))

        except ConnectionError as e:
            equity.reconnect()


def update_global_data(ticker1, ticker2): 
    global global_data,hedge_ratio, duration_lookback, bar_size_lookback
    duration = duration_lookback

    if len(global_data) == 0: 
        logger.info('Global data is empty, initializing data!')
        replace_df = True
    else:

        latest_time_pulled = get_price(ticker1, return_time=True) 
        try: 
            latest_time_pulled = latest_time_pulled.timestamp()
        except: 
            latest_time_pulled = time.mktime(latest_time_pulled.timetuple())
        #get the latest time in the df
        latest_df_time = global_data['Time'].iloc[0]
        differential = latest_time_pulled - latest_df_time
        if differential > 10: 
            delta_days = (latest_time_pulled - latest_df_time)/86400
            delta_bars = math.ceil(delta_days*periods_per_day*1.25)
            if delta_bars > len(global_data): 
                replace_df = True #update entire dataframe
            else: 
                replace_df = False #updates necessary part of dataframe
                duration = bar_format(delta_bars, bar_size_lookback)
        else:
            replace_df = False
            return

    if replace_df == True: #set duration to 1000 for purposes of calculating hedge ratio
        duration = 1000
        
    #get x bars back
    t1, t2 = Stock(ticker1, 'SMART','USD'), Stock(ticker2, 'SMART','USD')

    ib.qualifyContracts(t1)
    ib.qualifyContracts(t2)

    data_time = datetime.fromtimestamp(global_time)
    data1 = ib.reqHistoricalData(t1, data_time, duration, bar_size_lookback, 'MIDPOINT', True) #global time will likely have to be changed. 
    data2 = ib.reqHistoricalData(t2, data_time, duration, bar_size_lookback, 'MIDPOINT', True)

    times = [time.mktime(data1[i].date.timetuple()) for i in range(len(data1))]

    data1, data2, times = data1[::-1], data2[::-1], times[::-1]

    data1_close = [data1[i].close for i in range(len(data1))]
    data2_close = [data2[i].close for i in range(len(data2))]

    data1_low = [data1[i].low for i in range(len(data1))]
    data2_low = [data2[i].low for i in range(len(data2))]

    data1_high = [data1[i].high for i in range(len(data1))]
    data2_high = [data2[i].high for i in range(len(data2))]

    spread = []
    for i in range(len(data1_close)):
        if spread_type == 'log':
            spread.append(math.log(data1_close[i]) - math.log(hedge_ratio*data2_close[i]))
        elif spread_type == 'absolute':
            spread.append(data1_close[i] - hedge_ratio*data2_close[i])

    df = pd.DataFrame({'Time':times, 'Ticker 1 Price':data1_close,'Ticker 1 Low':data1_low, 'Ticker 1 High':data1_high,'Ticker 2 Price':data2_close, 'Ticker 2 Low':data2_low, 'Ticker 2 High':data2_high,'Spread': spread})

    if replace_df == False: 
        df_time = df['Time'].iloc[-1]
        rows = 0
        for t in global_data['Time']:
            rows += 1
            if df_time == t: 
                break 
            
        if rows > 0: 
            global_data = global_data.truncate(before=rows)
            #concatenate 
            global_data = pd.merge(df,global_data, how="outer")
    else: 
        global_data = df

    global_data = global_data[:global_data_max_size]

def run(pair_number1, ticker1, ticker2, duration_lookback_input, bar_size_lookback_input, lookback_input, total_periods_input): 
    global pair_number, duration_lookback, bar_size_lookback, lookback, total_periods
    pair_number, duration_lookback, bar_size_lookback, lookback, total_periods = pair_number1, duration_lookback_input, bar_size_lookback_input, lookback_input, total_periods_input
    run_script(ticker1, ticker2)
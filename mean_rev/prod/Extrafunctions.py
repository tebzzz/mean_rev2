#Functions are placed here when they are largely no longer used, but still may prove useful in the future. 

def log_crypto_trade(trade):
    global pair_number, trade_status
    
    try:
        time, ticker1, buy_sell, price, quantity ,tradeid = trade['info']['updateTime'], trade['info']['symbol'],trade['side'], float(trade['average']), float(trade['info']['executedQty']), float(trade['id'])
        fee = price*quantity*0.0004
        #retrieve current p/l from the sql database
        res = cloud.execute("SELECT total_pl FROM current_data WHERE pair_number =" + str(pair_number))
        num = res.one()[0]
        if num == None:
            num = 0
        else: 
            num = int(num)

        if buy_sell == 'buy':
            res = num - (price*quantity)
        elif buy_sell == 'sell':
            res = num + (price*quantity)
        cloud.execute("UPDATE current_data SET total_pl = " + str(res) + " WHERE pair_number = " + str(pair_number))
        
        #time, ticker char, buy_sell char, price double, quantity double.
        print('Trade Status is: ' + str(trade_status))
        if trade_status == 'open':
            status = 'OPEN'
        else: 
            status = 'CLOSE'
            res = cloud.execute("SELECT * FROM trades WHERE pair_number = " + str(pair_number) + " AND ticker = '" + str(ticker1) + "' AND status = 'OPEN' ORDER BY timestamp desc")
            data = res.all()[0] 
            close_time = float(time)
            duration = close_time - float(data[2])
            if buy_sell == 'buy':
                p_l = (float(data[5]) - price)*quantity #***need to add logic for buying and selling
            if buy_sell == 'sell':
                p_l = (price - float(data[5]))*quantity
            fees = fee + float(data[7]) 
            cloud.execute("INSERT INTO completed_trades (pair_number, ticker, p_l, fees, close_time, duration) VALUES (" + str(pair_number) + ",'" + ticker1 + "'," + str(p_l) + "," + str(fees) + "," + str(close_time) + "," + str(duration) + ")")
        #SQL
        cloud.execute("INSERT INTO trades (pair_number, trade_id, timestamp, ticker, buy_sell, price, quantity, fees, status) VALUES (" + str(pair_number) + "," + str(tradeid) + " , '" + time + "','" + ticker1 + "','" + buy_sell + "'," + str(price) + "," + str(quantity) + "," + str(fee) + ",'" + status +"')")  

        print('Crypto Trade Logged')
    except Exception as e: 
        print(e)

def crypto_order(ticker, type, buy_sell, quantity):
    print("Submitting crypto order! " + str(buy_sell) + " " + str(quantity) + ' ' + str(ticker))
    try: 
        if type == 'market':
            if buy_sell == 'buy':
                trade = exchange.create_order(ticker, 'market','buy', quantity)
            elif buy_sell == 'sell':
                trade = exchange.create_order(ticker, 'market','sell', quantity)  
        elif type == 'limit':
            if buy_sell == 'buy':
                trade = exchange.create_order(ticker, 'limit', 'buy',quantity)
            elif buy_sell == 'sell':
                trade = exchange.create_order(ticker, 'limit', 'sell',quantity)

        log_crypto_trade(trade)
    except Exception as e: 
        print(e)

def log_trade(trade): 
    try: 
        time, ticker1, buy_sell, price, quantity = str(trade.log[0].time)[:22], str(trade.contract.symbol), str(trade.order.action), trade.fills[0].execution.avgPrice, trade.order.totalQuantity
        fee = price*quantity*0.0004

        pair_number = int(pair_number)

        #retrieve current p/l from the sql database
        result = cloud.execute("SELECT total_pl FROM current_data WHERE pair_number =" + str(pair_number))
        num = result.one()[0]
        if buy_sell == 'buy':
            res = num - (price*quantity)
        elif buy_sell == 'sell':
            res = num + (price*quantity)
        cloud.execute("UPDATE current_data SET total_pl = " + str(res) + "' WHERE pair_number = " + str(pair_number))

        if trade_status == 'open':
            status = 'OPEN'
        else: 
            status = 'CLOSE'
            res = cloud.execute("SELECT * FROM trades WHERE pair_number = " + str(pair_number) + " AND status = 'open' ORDER BY timestamp desc")
            data = res.all()[0]
            close_time = time
            duration = close_time - data[2]
            p_l = data[4] - price 
            cloud.execute("INSERT INTO completed_trades (pair_number, ticker, p_l, close_time, duration) VALUES (" + str(pair_number) + ",'" + ticker1 + "'," + str(p_l) + "," + str(close_time) + "," + str(duration) + ")")
        #SQL
        cloud.execute("INSERT INTO trades (pair_number, timestamp, ticker, buy_sell, price, quantity, fees, status,) VALUES (" + str(pair_number) + " , '" + time + "','" + ticker1 + "','" + buy_sell + "'," + str(price) + "," + str(quantity) + "," + str(fee) + ",'" + status +"')")  
        
        #time, ticker char, buy_sell char, price double, quantity double.
        print('Trade Successfully Logged')
    except Exception as e: 
        print(e)

def set_positions(security_a, security_b, current_pos,last_pos,new_quantity_a, new_quantity_b, use_crypto=False):
    trade = None #will return none if there are no trades done

    if use_crypto == False:
        quantity_a, quantity_b = get_current_positions(security_a, security_b)
    else: 
        positions = get_positions_crypto([security_a, security_b])
        ticker1_formatted, ticker2_formatted = format_ticker(ticker1=security_a, ticker2=security_b)
        quantity_a, quantity_b = float(positions[ticker1_formatted]), float(positions[ticker2_formatted])

    if current_pos == 'long': #long a, sell b, checked
        final_quantity_a = new_quantity_a
        final_quantity_b = new_quantity_b*-1

    elif current_pos == 'short': 
        final_quantity_a = new_quantity_a*-1
        final_quantity_b = new_quantity_b
    
    elif current_pos == None: 
        final_quantity_a = 0
        final_quantity_b = 0

    if quantity_a == final_quantity_a: 
        pass
    elif quantity_a < final_quantity_a: 
        order_value = abs(final_quantity_a - quantity_a)
        if use_crypto == True: 
            trade = crypto_order(security_a, 'market', 'buy', order_value)
        else: 
            trade = adaptive_order(security_a, 'buy', order_value, pause_until_complete=True, )
    elif quantity_a > final_quantity_a:
        order_value = abs(quantity_a - final_quantity_a)
        if use_crypto == True: 
            trade = crypto_order(security_a, 'market', 'sell', order_value)
        else: 
            trade = adaptive_order(security_a, 'sell', order_value, pause_until_complete=True)

    if quantity_b == final_quantity_b: 
        pass
    elif quantity_b < final_quantity_b: 
        order_value = abs(final_quantity_b - quantity_b)
        if use_crypto == True:
            trade = crypto_order(security_b, 'market', 'buy', order_value)
        else: 
            trade = adaptive_order(security_b, 'buy', order_value, pause_until_complete=True)
    elif quantity_b > final_quantity_b:
        order_value = abs(quantity_b - final_quantity_b)
        if use_crypto == True: 
            trade = crypto_order(security_b, 'market', 'sell', order_value)
        else: 
            trade = adaptive_order(security_b, 'sell', order_value, pause_until_complete=True)

    return trade, last_pos

def get_historical_prices_crypto(ticker,bars_back=1000):
    if bars_back <= 1000:
        data = exchange.fetch_ohlcv(ticker,bar_size_lookback,limit=bars_back) #websocket
        
    elif bars_back > 1000: #***finish this for pagenation, how do we get a certain period? 
        date_list, data = [], []
        if bar_size_lookback == '1m':
            time_delta = 60
        elif bar_size_lookback == '5m':
            time_delta = 60*5
        elif bar_size_lookback == '15m':
            time_delta = 60*15
        elif bar_size_lookback == '30m':
            time_delta = 60*30
        elif bar_size_lookback == '1h':
            time_delta = 60*60
        elif bar_size_lookback == '2h':
            time_delta = 60*120
        elif bar_size_lookback == '4h':
            time_delta = 60*4*60
        elif bar_size_lookback == '6h':
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
            data = data + exchange.fetch_ohlcv(ticker,bar_size_lookback,since=date,limit=1000)

        if bars_back%1000 != 0:
            extra = bars_back%1000
            e_date = date_list[0]+extra*time_delta
            data = data + exchange.fetch_ohlcv(ticker,bar_size_lookback,since=e_date,limit=extra)

    prices = []

    for d in data: 
        prices.append(d[4])

    return prices, data
    
def wwma(values, n):
    return values.ewm(alpha=1/n, adjust=False).mean()

def get_keltner_channels(atr_mult=2): #***need to add calculations for average true range 
    global global_data
    data_a = global_data(['Ticker 1 Price', 'Ticker 1 Low', 'Ticker 1 High'])
    data_b = global_data(['Ticker 2 Price', 'Ticker 2 Low', 'Ticker 2 High'])
    atr = get_atr(data_a, data_b) #***need to set up to pull from global data

    spread = global_data['Spread']
    ema = get_ema(spread) #***will need to manipulate this data
    keltner_up = ema + (atr_mult*atr)
    keltner_down = ema - (atr_mult*atr)

    return keltner_up, keltner_down

def get_atr(data_a, data_b):
    #***data should be pulled from global data
    n=14
    df = df.rename(columns={'oldName1': 'newName1', 'oldName2': 'newName2'})
    df_a = data_a.rename(columns={'Ticker 1 Price':'close', 'Ticker 1 Low':'low', 'Ticker 1 High':'high'}) #**set up pulls for high low and close and change column names
    df_b = data_b.rename(columns={'Ticker 2 Price':'close', 'Ticker 2 Low':'low', 'Ticker 2 High':'high'})

    a_high, a_low, a_close = df_a['high'], df_a['low'], df_a['close']
    b_high, b_low, b_close = df_b['high'], df_b['low'], df_b['close']

    #high
    m_high,b = np.polyfit(a_high, b_high,1)
    m_low,b = np.polyfit(a_low, b_low,1)
    m_close,b = np.polyfit(a_close, b_close,1)
    spread_high,spread_low,spread_close = [],[],[]

    for i in range(len(a_high)):
        spread_high.append(a_high.iloc[i]-m_high*b_high.iloc[i])
        spread_low.append(a_low.iloc[i]-m_low*b_low.iloc[i])
        spread_close.append(a_close.iloc[i]-m_close*b_close.iloc[i])

    high = pd.DataFrame(spread_high)
    low = pd.DataFrame(spread_low)
    close = pd.DataFrame(spread_close)
    df = pd.DataFrame()
    df['tr0'] = abs(high - low)
    df['tr1'] = abs(high - close.shift())
    df['tr2'] = abs(low - close.shift())
    tr = df[['tr0', 'tr1', 'tr2']].max(axis=1)
    atr = wwma(tr, n)
    return round(atr,2) #returns it as a dataframe

def adaptive_order(security, buy_sell, quantity, order_type='market', price=0, pause_until_complete=False):
    now = market_time()
    sec = Stock(security, 'SMART', 'USD')
    ib.qualifyContracts(sec)
    print(str(now)[5:16] + ' Submitting Trade: ' + str(buy_sell).upper() + ' ' + str(quantity) + ' ' + str(security).upper() + '!')
    if order_type.lower() == 'limit': 
        order = LimitOrder(buy_sell.upper(), quantity, price, algoStrategy='Adaptive', algoParams = [TagValue('adaptivePriority', 'Patient')])
    elif order_type == 'market': 
        order = MarketOrder(buy_sell.upper(), quantity, algoStrategy='Adaptive', algoParams = [TagValue('adaptivePriority', 'Urgent')])
    else: 
        print('Please make sure limit is set to limit or market') 

    trade = ib.placeOrder(sec, order)

    if pause_until_complete == True: #*** If this works we can simply use the fill amount from the order to log. This will be best logged in SQLite after initial tests are done with the current_state
        while trade.orderStatus.status != 'Filled':
            time = market_time()
            time1 = ' The Current Time is: ' + str(time)[:-7]
            print('Waiting for adaptive order to fill. The current status is ' + trade.orderStatus.status + time1)
            ib.sleep(1.5)

    log_trade(trade)

    return trade
def limit_orders(change_position):
    wait_time =  1
    for pos in change_position: 
        ticker = pos.keys()
        transaction_id = ''
        order_book_previous = exchange.fetch_order_book(ticker, limit=10)
        a = 0
        while a < 3:
            a += 1
            # print("order_book_previous", order_book_previous)
            buys_previous = [item[0] for item in order_book_previous['bids']]
            # print("buys_previous",buys_previous)
            average_buys_previous = statistics.mean(buys_previous)
            # print("average_buys_previous",average_buys_previous)
            sells_previous = [item[0] for item in order_book_previous['asks']]
            # print("sells_previous",sells_previous)
            average_sells_previous = statistics.mean(sells_previous)
            # print("average_sells_previous",average_sells_previous)
            # print("\n\n")
            order_book_current = exchange.fetch_order_book(ticker, limit=10)
            # print("order_book_current",order_book_current)
            buys_current = [item[0] for item in order_book_current['bids']]
            # print("buys_current",buys_current)
            average_buys_current = statistics.mean(buys_current)
            # print("average_buys_current",average_buys_current)
            sells_current = [item[0] for item in order_book_current['asks']]
            # print("sells_current",sells_current)
            average_sells_current = statistics.mean(sells_current)
            # print("average_sells_current",average_sells_current)

            # Submit order
            if change_position[ticker] > 0:
                # buy order
                # order filled?
                if transaction_id and order_filled(ticker, transaction_id):
                    break

                elif average_buys_current < (average_buys_previous*0.99) or (average_buys_current > average_buys_previous*1.01):
                    if transaction_id:
                        # Value changed by more than 1%
                        exchange.cancelOrder(transaction_id, symbol=ticker, params={})
                    transaction = exchange.createOrder(ticker, 'limit', 'buy', change_position[ticker], average_buys_current)
                    transaction_id = transaction['id']
                else:
                    #  Value is in the limits. wait for the order to complete. do nothing
                    pass
            else:
                if transaction_id and order_filled(ticker, transaction_id):
                    print("Breaking the loop")
                    break
                elif average_sells_current < (average_sells_previous * 0.99) or (average_sells_current > average_sells_previous * 1.01):
                    print("Sell Order")
                    # sell order
                    if transaction_id:
                        print("Cancelling the existing order")
                        exchange.cancelOrder(transaction_id, symbol=ticker, params={})
                    print("Creating new sell order")
                    transaction = exchange.createOrder(ticker, 'limit', 'sell', abs(change_position[ticker]), average_sells_current)
                    transaction_id = transaction['id']
                    print(transaction_id)
                else:
                    #  Value is in the limits. wait for the order to complete. do nothing
                    print("Value is in the limits. wait for the order to complete. do nothing")
                    pass

            order_book_previous = order_book_current
            average_sells_previous = average_sells_current
            average_buys_previous = average_buys_current
            # sleep for 0.5 sec
            time.sleep(wait_time)
            print("--------------------------------------------------------")

        # print(exchange.fetchOrders('BNXUSDT'))
        print("we're out")
import ccxt
import sqlalchemy
import time as t
from datetime import * 
from loguru import logger

exchange_id = 'binanceusdm'
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class({

    'apiKey': 'rSw15gwfuc3USRqLBi4mm5qQ5CP5HfdElUXN9tHC6222j64FpHtZ0T7E8MKncFC9',
    'secret': 'EqIuEQSJOI8CP4Ge5XhxzQInZ4KTRDRVSOmMspl9guUZGyxVISwZ4YsaWfJrD0kA',
    'enableRateLimit': True,
})

logger.add(
    "display.log",
    level="INFO",
    format="{time} {level} {message}",
    rotation="00:00",  # each day at 00:00 we create a new log file
    compression="zip",  # archive old log files to save space
    retention="30 days",  # delete logs after 30 days
    serialize=True,  # json format of logs
)

#connects to CloudSQL postgres
username = 'postgres'  # DB username
password = 'proddb123'  # DB password
host = '34.84.190.250'  # Public IP address for your instance
port = '5432'
database = 'prod'  # Name of database ('postgres' by default)

db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
    username, password, host, port, database)

engine = sqlalchemy.create_engine(db_url)
cloud = engine.connect()
bnb_value = 250

rest_time = 5 #length between updates

def get_fees(fees):
    currency = fees[0]['currency']
    amount = fees[0]['cost']
    if currency == 'USDT':
        return float(amount)
    elif currency == 'BNB': 
        return float(amount)*bnb_value

def check_tables(): #see if viz tables exist, if not create them.
    res = cloud.execute("SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_name = 'equity_data');").one()[0]
    if res == True: 
        pass
    else:
        cloud.execute("CREATE TABLE equity_data (date int, total_equity numeric, unrealized_pl numeric)")

    res = cloud.execute("SELECT EXISTS ( SELECT FROM information_schema.tables WHERE table_name = 'current_positions');").one()[0]
    if res == True: 
        pass
    else:
        cloud.execute("CREATE TABLE current_positions (ticker varchar(256), position numeric, average_cost numeric, current_price numeric, p_l numeric)")

def calculate_total_value(info):
    return float(info['total']['USDT']) + float(info['total']['BUSD'])
    #return float(info['total']['USDT']) + float(info['total']['BUSD']) + float(info['total']['BNB'])*bnb_value
    # values = info['total']
    # res = 0
    # for val in values: 
    #     if val == 'USDT' or val == 'BUSD':
    #         res += values[val]

# def check_data(): #checks current data at predefined intervals 
#     pass

# def update_chart()
#     pass

def write_completed_trades(): #id both are internal, respond with "FAILED"

    try: 
        #get a list of all internal_ids and what trades to attempt to match
        data = []
        res = cloud.execute("SELECT internal_id,ticker FROM trades WHERE status='OPEN' ORDER BY timestamp DESC").all()
        internal_ids = [[int(r[0]), r[1]] for r in res]
        for a in internal_ids:
            if a not in data:
                data.append(a)

        #attempt to match all of the trades
        for d in data: 

            id = d[0]
            ticker = d[1]

            res_open = cloud.execute("SELECT * FROM trades WHERE status='OPEN' AND internal_id=" + str(id) + " AND ticker='" + str(ticker) + "'").all()
            res_close = cloud.execute("SELECT * FROM trades WHERE status='CLOSE' AND internal_id=" + str(id) + " AND ticker='" + str(ticker) + "'").all()

            len_open, len_close = len(res_open), len(res_close)
            same_pair_count_open, same_pair_count_close = 0,0
            open_volume, close_volume, total_fees = 0,0,0
            open_prices, close_prices = [], []
            open_time, close_time = int(res_open[0][3]) , -1
            avg_price_open, avg_price_close = 0, 0

            for res in res_open: 
                if res[2] == 999996 or res[2] == 999996:
                    same_pair_count_open += 1

                pair_number = int(res[0])
                open_type = res[5]

                open_volume += float(res[7])
                total_fees += float(res[8])

                avg_price = float(res[6]) 
                quantity = float(res[7])
                open_prices.append([avg_price, quantity])
                
                #finding earliest open time
                if int(res[3]) < open_time: 
                    open_time = float(res[3]) 
                
            for res in res_close: 
                if res[2] == 999996 or res[2] == 999996:
                    same_pair_count_close += 1

                close_volume += float(res[7])
                total_fees += float(res[8])

                avg_price = float(res[6]) 
                quantity = float(res[7])
                close_prices.append([avg_price, quantity])
                #finding latest closing time

                if int(res[3]) > close_time: 
                    close_time = float(res[3]) #replace if next value is higher

            if close_volume != 0 and open_volume == close_volume:   
                pass
            else:
                continue

            duration = close_time - open_time

            for price in open_prices:
                avg_price_open += price[0]*(price[1]/open_volume)
            
            for price in close_prices:
                avg_price_close += price[0]*(price[1]/close_volume)

            #minor bug here
            if open_type == 'buy': 
                p_l = avg_price_close*close_volume - avg_price_open*open_volume
            elif open_type == 'sell': 
                p_l = avg_price_open*open_volume - avg_price_close*close_volume

            p_l, total_fees, duration, close_time = round(p_l,3), round(total_fees, 5), round(duration, 0), round(close_time, 0)

            if same_pair_count_close == len_close and same_pair_count_open == len_open: 
                cloud.execute("DELETE FROM trades WHERE internal_id =" + str(id))

            cloud.execute(f"UPDATE trades SET status='OPEN-LOGGED' WHERE status='OPEN' AND internal_id={str(id)} AND ticker='{str(ticker)}'")
            cloud.execute(f"UPDATE trades SET status='CLOSED-LOGGED' WHERE status='CLOSE' AND internal_id={str(id)} AND ticker='{str(ticker)}'")
            cloud.execute(f"INSERT INTO completed_trades (pair_number, ticker, p_l, fees, close_time, duration, special_status, internal_id) VALUES ({(pair_number)},'{str(ticker)}',{str(p_l)},{str(total_fees)},{str(close_time)},{str(duration)},'None',{str(id)})")

            logger.info(f"UPDATE trades SET status='OPEN-LOGGED' WHERE status='OPEN' AND internal_id={str(id)} AND ticker='{str(ticker)}'")
            logger.info(f"UPDATE trades SET status='CLOSED-LOGGED' WHERE status='CLOSE' AND internal_id={str(id)} AND ticker='{str(ticker)}'")
            logger.info(f"INSERT INTO completed_trades (pair_number, ticker, p_l, fees, close_time, duration, special_status, internal_id) VALUES ({(pair_number)},'{str(ticker)}',{str(p_l)},{str(total_fees)},{str(close_time)},{str(duration)},'None',{str(id)})")

    except Exception as e: 

        logger.debug(e)
        logger.debug('There was an error writing to completed trades.')

def update_positions():
    tickers, q, p_l = [], [], []
    pos = exchange.fetch_balance()['info']['positions']

    for p in pos:
        if float(p['positionAmt']) != 0:
            tickers.append(p['symbol'])
            q.append(p['positionAmt'])
            p_l.append(p['unrealizedProfit'])

    cloud.execute("DELETE FROM current_positions")
    for i in range(len(tickers)): 
        cloud.execute("INSERT INTO current_positions (ticker, position, p_l) VALUES ( '" + str(tickers[i]) + "'," + str(q[i]) + "," + str(p_l[i]) + ")")

# def check_table_time(table, bar_period):
#     current_time = t.time()*1000
#     latest_chart_time = cloud.execute("SELECT * FROM " + str(table) + " ORDER BY timestamp DESC").fetchone()[0]
#     current_chart_time = None 
#     if bar_period == '1m':
#         seconds = 60
#     elif bar_period == '5m':
#         seconds = 300
#     elif bar_period == '15m':
#         seconds = 900
#     elif bar_period == '30m':
#         seconds = 1800
#     elif bar_period == '60m': 
#         seconds = 3600

#     if (current_chart_time + (seconds*1000)) > current_time:
#         return latest_chart_time

#     else: 
#         return None

def get_latest_bar(pair):
    time_period = pair[0]
    ticker1 = pair[0]
    table = 'ohlcv_' + ticker1 + '_' + time_period

    res = cloud.execute("SELECT * FROM " + table + " ORDER BY timestamp DESC").fetchone()
    latest_time = res[0]
    return latest_time

# def write_to_equity_charts():
#     pairs = cloud.execute("SELECT * FROM current_data").all()
#     active_pairs = []
#     for pair in pairs:
#         if pair[14]+300000 > t.time()*1000: 
#             active_pairs.append(pair)

#     for pair in active_pairs: 
#         table = 'pair_data_' + str(pair[0])
#         bar_period = pair[3]
#         new_row_time = check_table_time(table, bar_period)

#         z = pair[5]
#         upper= pair[6]
#         lower= pair[7]
#         ma= pair[8]
#         spread= pair[16]
#         pricea = pair[15]
#         priceb = pair[16]
#         equity = 0 #unsure where to get this for now 

#         if new_row_time:
#             timestamp = get_latest_bar(pair)#we need to ge the next timestamp to insert, will likely not be easy to get!
#             if timestamp == new_row_time: 
#                 return
#             cloud.execute("INSERT INTO " + str(table) + "(timestamp, zscore, upper_band, lower_band, moving_average, spread, prices_a, prices_b,equity) VALUES (" + str(timestamp) + "," + str(z) + "," + str(upper) + "," + str(lower) + "," + str(ma) + "," + str(spread) + "," + str(pricea) + "," + str(priceb) + "," + str(equity) + ")")
#         else: 
#             print('Updating')
#             curr_time = int(cloud.execute("SELECT * FROM " + str(table) + " ORDER BY timestamp DESC").fetchone()[0])
#             cloud.execute("UPDATE " + table + " SET zscore=" + str(z) + ",upper_band=" + str(upper) +  ",lower_band=" + str(lower) + ",moving_average= " + str(ma)+ " ,spread=" + str(spread) + " ,prices_a=" + str(pricea) + ",prices_b= " + str(priceb) +  ",equity= " + + str(equity) + " WHERE timestamp=" + str(curr_time))

def update_pl_and_fees():
    pairs = cloud.execute("SELECT * FROM current_data").all()
    for pair in pairs:
        pair_number = int(pair[0])
        pl = cloud.execute("SELECT SUM(p_l) FROM completed_trades WHERE pair_number=" + str(pair_number)).fetchall()[0][0]
        fees = cloud.execute("SELECT SUM(fees) FROM completed_trades WHERE pair_number=" + str(pair_number)).fetchall()[0][0]

        if pl == None: 
            pl = 0
        else: 
            pl = float(pl)

        if fees == None: 
            fees = 0
        else: 
            fees = float(fees)

        total_pl = pl - fees

        pl = round(pl,2)
        fees = round(fees, 2)
        total_pl = round(total_pl,2)

        cloud.execute("UPDATE current_data SET raw_pl=" + str(pl) + ",fees=" + str(fees) + ",total_pl=" + str(total_pl) + " WHERE pair_number=" + str(pair_number)) 

def update_status_table():
    cloud.execute("UPDATE statuses SET display=" + str(int(t.time()*1000)))

#updates every 5 seconds or so in the status table 
def update_equity_value():
    info = exchange.fetch_balance()
    equity = calculate_total_value(info)
    unrealized = float(info['info']['totalUnrealizedProfit'])
    cloud.execute(f"UPDATE statuses SET latest_equity={equity}, unrealized={unrealized}")

def log_equity():
    #should log approximately every 5 minutes
    info = exchange.fetch_balance()
    equity = float(calculate_total_value(info))
    unrealized = float(info['info']['totalUnrealizedProfit'])
    curr = datetime.timestamp(datetime.now())
    cloud.execute("INSERT INTO equity_data (date, total_equity, unrealized_pl) VALUES (" + str(curr) + "," + str(equity) + "," + str(unrealized) + ")")

def write_pair_trades():
    res = cloud.execute("SELECT * FROM completed_trades WHERE special_status IS NULL").fetchall()
    ids = list(set([r[7] for r in res]))
    for id in ids: 
        try: 
            res = cloud.execute(f"SELECT * FROM completed_trades WHERE internal_id={id}")
            if len(res) == 2: 
                pair_number, latest_close_time = res[0], res[4]
                p_l, fees = 0,0
                for r in res:
                    p_l += float(r[2])
                    fees += float(r[3])
                total_pl = p_l + fees
                cloud.execute(f'INSERT INTO pair_trades (pair_number, internal_id, pl, fees, total_pl, latest_close_time) VALUES ({pair_number}, {id}, {p_l}, {fees}. {total_pl}, {latest_close_time})')
            else: 
                logger.info('Error with completed trades')
                continue
        except: 


def main(): 
    check_tables()
    i = 50
    j = 300
    while True: 
        
        update_positions()
        update_status_table()
        update_equity_value()
        t.sleep(rest_time)

        if i >= 50: 
            #write_to_equity_charts() 
            write_completed_trades()
            update_pl_and_fees()
            #write_pair_trades()

            i = 0 
        if j >= 300:
            log_equity()

            j = 0

        i += 1
        j += 1
    

if __name__ == '__main__':
    main()
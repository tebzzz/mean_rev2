import ccxt.pro as ccxtpro
import ccxt 
import sqlalchemy
import time
import pandas as pd 
import random

exchange = ccxt.binanceusdm()

username = 'postgres'  
password = 'proddb123'
host = '34.84.190.250' 
port = '5432'
database = 'prod' 

db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
    username, password, host, port, database)

engine = sqlalchemy.create_engine(db_url)
cloud = engine.connect()

pair_number = 1
ohlcv = True

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
                print("Table 1 price not a float")
        except: 
            print("Table 1 has not been created yet!")
        
        try: 
            price = cloud.execute("SELECT close FROM " + str(table2) + " ORDER BY timestamp DESC").all()[0][0]
            if type(price) == type(float(1.0)):
                price_ticker2 = price
            else:
                print("Table 2 price not a float")
        except: 
            print("Table 2 has not been created yet!")

def prepare_table(ticker, tables, base_table, bar_size, heartbeat):
    id = pair_number + random.randint(0,1000000)
    table = (str(ticker) + '_' +  str(bar_size)).lower()
    limit = 500

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

def check_websockets(ticker1, ticker2, bar_size, ohlcv=False): #needs a little bit of editings
    heartbeat = exchange.milliseconds()
    price_ticker1, price_ticker2 = None, None

    if ohlcv == True: 
        base_table = 'ohlcv_websockets'
    else: 
        base_table = 'price_websockets'

    #get list of all tables in postgres, check if tables exist 
    tables = pd.DataFrame(cloud.execute("SELECT table_name FROM information_schema.tables"))
    tables = tables['table_name'].values.tolist()

    prepare_table(ticker1, tables, base_table, bar_size, heartbeat)
    prepare_table(ticker2, tables, base_table, bar_size, heartbeat)

    #waiting for the tables to be populated 
    print(1)
    wait_for_tables(ticker1, ticker2, bar_size)
    print('Both price tables are currently present, script starting!')


def update_websockets(ticker1, ticker2, bar_size, ohlcv=False):
    if ohlcv == True: 
        table_type = 'ohlcv_websockets'
    else: 
        table_type = 'price_websockets'
    table1 = str(ticker1).lower() + '_' + str(bar_size)
    table2 = str(ticker1).lower() + '_' + str(bar_size)
    current_time = exchange.milliseconds()

    cloud.execute("UPDATE " + str(table_type) + " SET heartbeat="+ str(current_time) + " WHERE ticker_name='" + str(ticker1) + "' AND bar_size='" + str(bar_size) + "'")
    cloud.execute("UPDATE " + str(table_type) + " SET heartbeat="+ str(current_time) + " WHERE ticker_name='" + str(ticker2) + "' AND bar_size='" + str(bar_size) + "'")

if __name__ == '__main__':
    check_websockets('XRPUSDT', 'APEUSDT', '5m', ohlcv=True)
    while True: 
        print(exchange.milliseconds())
        update_websockets('XRPUSDT', 'APEUSDT', '5m', ohlcv=True)
        time.sleep(30)
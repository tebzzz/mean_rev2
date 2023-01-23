import sqlalchemy
import secret as s

from sqlalchemy import Table, Column, BigInteger, Integer, String, MetaData, select, desc


username = s.username  # DB username
password = s.password  # DB password
host = s.host  # Public IP address for your instance  
port = s.port
database = s.database  # Name of database ('postgres' by default)
db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
    username, password, host, port, database)

engine = sqlalchemy.create_engine(db_url, echo=True)
cloud = engine.connect()
metadata_obj = MetaData()


def create_ohlcv_table(table_name):
    table = Table(table_name, metadata_obj,
        Column('id', Integer, primary_key=True, nullable=False),
        Column('ticker_name', String(128), nullable=False),
        Column('bar_size', String(128), nullable=False),
        Column('limits', Integer, nullable=False),
        Column('heartbeat', BigInteger, nullable=False),
    )
    table.create(cloud, checkfirst=True)
    return table

def create_table(table_name):
    table = Table(table_name, metadata_obj,
        Column('id', Integer, primary_key=True, nullable=False),
        Column('ticker_name', String(128), nullable=False),
        Column('bar_size', String(128), nullable=False),
        Column('heartbeat', BigInteger, nullable=False),
    )
    table.create(cloud, checkfirst=True)
    return table

def create_row(table, data):
    cloud.execute(table.insert().values(ticker_name = data["ticker_name"], bar_size = data["bar_size"], heartbeat = data["heartbeat"]))

def create_ohlcv_row(table, data):
    cloud.execute(table.insert().values(ticker_name = data["ticker_name"], bar_size = data["bar_size"], limits = data["limits"], heartbeat = data["heartbeat"]))

def delete_row(table, row):
    cloud.execute(table.delete().where(table.c.id == row))

if __name__ == '__main__':
    price = create_table("price_websockets")
    ohlcv = create_ohlcv_table("ohlcv_websockets")

    # price_data = [
    #     {
    #         "ticker_name": "BTCUSDT",
    #         "bar_size": "1m",
    #         "heartbeat": 1665048003*1000
    #     },
    #     {
    #         "ticker_name": "BTCUSDT",
    #         "bar_size": "10s",
    #         "heartbeat": 1665048003*1000
    #     }
    # ]

    # ohlcv_data = [
    #     {
    #         "ticker_name": "BTCUSDT",
    #         "bar_size": "1m",
    #         "limits": 100,
    #         "heartbeat": 1665048003*1000
    #     },
    #     {
    #         "ticker_name": "BTCUSDT",
    #         "bar_size": "10s",
    #         "limits": 100,
    #         "heartbeat": 1665048003*1000
    #     }
    # ]

    # create_row(price, price_data[0])
    # create_row(price, price_data[1])
    # create_ohlcv_row(ohlcv, ohlcv_data[0])
    # create_ohlcv_row(ohlcv, ohlcv_data[1])
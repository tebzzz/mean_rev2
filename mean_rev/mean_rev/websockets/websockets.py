import ccxt.pro as ccxtpro
import ccxt
import asyncio
import sqlalchemy
import math
import time
import loguru

import secret as s

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import Table, Column, BigInteger, Float, MetaData, select, desc, text

logger.add(
    "orders.log",
    level="INFO",
    format="{time} {level} {message}",
    rotation="00:00",  # each day at 00:00 we create a new log file
    compression="zip",  # archive old log files to save space
    retention="3 days",  # delete logs after 3 days
    serialize=True,  # json format of logs
)

username = s.username  # DB username
password = s.password  # DB password
host = s.host  # Public IP address for your instance
port = s.port
database = s.database  # Name of database ('postgres' by default)

db_url = 'postgresql+asyncpg://{}:{}@{}:{}/{}'.format(
  username, password, host, port, database)

sync_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
  username, password, host, port, database)

engine = create_async_engine(db_url, echo=False)

engine_sync = sqlalchemy.create_engine(sync_url, echo=False)
cloud_sync = engine_sync.connect()

metadata_obj = MetaData()

watched_price_data = Table(
  'price_websockets', metadata_obj, autoload_with=engine_sync)
watched_ohlcv_data = Table(
  'ohlcv_websockets', metadata_obj, autoload_with=engine_sync)

exchange = ccxtpro.binanceusdm()

existing_tables = set()
heartbeats = dict()


def delete_locks(table):
  # get all of the locks and select the ones we want to delete
  locks = cloud_sync.execute(
    "SELECT t.relname, l.locktype, page, virtualtransaction, pid, mode, granted FROM pg_locks l, pg_stat_all_tables t WHERE l.relation = t.relid ORDER BY relation asc")

  # delete them
  for lock in locks:
    if lock[0] == table:
      cloud_sync.execute("SELECT pg_terminate_backend(" + str(lock[4]) + ")")


def get_table_name(ticker, candle_duration, ohlcv=False):
  if ohlcv:
    return f"ohlcv_{ticker.lower()}_{str(candle_duration)}"
  else:
    return f"prices_{ticker.lower()}_{str(candle_duration)}"


def create_price_timestamp_table(table_name):
  drop_if_exists(table_name)
  table = Table(table_name, metadata_obj,
                Column('timestamp', BigInteger,
                        primary_key=True, nullable=False),
                Column('price', Float, nullable=False),
                keep_existing=True,
                )
  table.create(cloud_sync, checkfirst=False)
  return table


def create_ohlcv_table(table_name):
  drop_if_exists(table_name)
  table = Table(table_name, metadata_obj,
                Column('timestamp', BigInteger,
                        primary_key=True, nullable=False),
                Column('open', Float, nullable=False),
                Column('high', Float, nullable=False),
                Column('low', Float, nullable=False),
                Column('close', Float, nullable=False),
                Column('volume', Float, nullable=False),
                keep_existing=True,
                )
  table.create(cloud_sync, checkfirst=False)
  return table


def drop_if_exists(table_name):
  delete_locks(table_name)
  engine_sync.execute(text(f'DROP TABLE IF EXISTS {table_name};'))


async def get_table_last_pk(table):
  s = select(table.c.timestamp).limit(1).order_by(desc('timestamp'))
  async with engine.begin() as cloud:
    pk = await cloud.execute(s)
    pk = pk.fetchone()
  return pk[0] if pk else None


async def get_table_last_price(table):
  s = select(table.c.price).limit(1).order_by(desc('timestamp'))
  async with engine.begin() as cloud:
    price = await cloud.execute(s)
    price = price.fetchone()
  return price[0] if price else None


async def create_row(table, ts, price):
  async with engine.begin() as cloud:
    await cloud.execute(table.insert().values(timestamp=ts, price=price))


async def update_row(table, pk, ts, price):
  async with engine.begin() as cloud:
    await cloud.execute(table.update().where(table.c.timestamp == pk).values(timestamp=ts, price=price))


async def update_timestamp(table, pk, ts):
  async with engine.begin() as cloud:
      await cloud.execute(table.update().where(table.c.timestamp == pk).values(timestamp=ts))


async def create_ohclv_row(table, data):
  async with engine.begin() as cloud:
    await cloud.execute(table.insert().values(
      timestamp=data[0],
      open=data[1],
      high=data[2],
      low=data[3],
      close=data[4],
      volume=data[5]
    ))


async def update_ohclv_row(table, data):
  async with engine.begin() as cloud:
    
    await cloud.execute(table.update().where(table.c.timestamp == data[0]).values(
      high=data[2],
      low=data[3],
      close=data[4],
      volume=data[5]
    ))


async def update_ohlcv_table(symbol, candle_duration, table, limit=1000):
  #stop_time = stop_time + 30*60*1000
  ts = exchange.parse_timeframe(candle_duration) * 1000
  try:
    print(symbol)
    print(candle_duration)
    print(limit)
    ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=candle_duration, limit=limit)
  except ccxtpro.RequestTimeout as e:
    print('fetch_ohlcv failed due to RequestTimeout error:', str(e))

  ohlcv = ohlcv[::-1]
  ohlcv_dict = [{"timestamp": e[0], "open": e[1], "high": e[2],
                  "low": e[3], "close": e[4], "volume": e[5]} for e in ohlcv]

  pk = ohlcv_dict[0]['timestamp']

  async with engine.begin() as cloud:
    await cloud.execute(table.insert(), ohlcv_dict)

  while True:
    stop_time = heartbeats[table.name]
    current_time = int(time.time()*1000)

    if (current_time >= stop_time):
      existing_tables.remove(table.name)
      del heartbeats[table.name]
      tasks = asyncio.all_tasks()
      delete_locks(str(table))
      # changed engine_sync to engine...see if this solves the problem
      table.drop(engine, checkfirst=False)
      current_task, = [
          task for task in tasks if task.get_name() == table.name]
      current_task.cancel()
      await asyncio.sleep(0)

    else:
      if exchange.has['watchTrades']:
        try:
          trades = await exchange.watch_trades(symbol)
          built_ohlcvc = exchange.build_ohlcvc(trades, timeframe=candle_duration, since=current_time-ts)
         
          if len(trades) > 0:
            current_minute = int(current_time / ts)
            ohlcvc = ([candle for candle in built_ohlcvc if int( candle[0] / ts ) == current_minute])[0]

            if ohlcvc[0] != pk:
              pk = ohlcvc[0]
              await create_ohclv_row(table, ohlcvc)
            else:
              await update_ohclv_row(table, ohlcvc)

        except Exception as e:
          print(str(e))


async def update_price_timestamp_table(ticker, candle_duration, table, limit=1000):
  ts = exchange.parse_timeframe(candle_duration) * 1000

  try:
    ohlcv = await exchange.fetch_ohlcv(ticker, timeframe=candle_duration, limit=limit)
  except ccxtpro.RequestTimeout as e:
    print('fetch_ohlcv failed due to RequestTimeout error:', str(e))

  ohlcv = ohlcv[::-1]
  orders_dict = [{"timestamp": e[0], "price": e[2]} for e in ohlcv]
  pk = orders_dict[0]['timestamp']
  new_row_creation_time = pk + ts

  async with engine.begin() as cloud:
    await cloud.execute(table.insert(), orders_dict)

  while True:
    stop_time = heartbeats[table.name]
    price = None
    current_time = int(time.time()*1000)

    if (current_time >= stop_time):
      existing_tables.remove(table.name)
      del heartbeats[table.name]
      tasks = asyncio.all_tasks()
      delete_locks(str(table))
      # changed engine_sync to the async engine
      table.drop(engine, checkfirst=False)
      current_task, = [
        task for task in tasks if task.get_name() == table.name]
      current_task.cancel()
      await asyncio.sleep(0)

    else:
      if exchange.has['watchOrderBook']:
        try:
          orderbook = await exchange.watch_order_book(ticker, limit=5)
          price = orderbook['asks'][0][0] if len(
              orderbook['asks']) > 0 else None
        except Exception as e:
            print(e)

        if price:
          if (current_time >= new_row_creation_time):
            new_row_creation_time += ts
            timestamp = int(math.floor(current_time / ts) * ts)
            await create_row(table, current_time, price)
            await update_timestamp(table, pk, timestamp)

          else:
            await update_row(table, pk, current_time, price)

          pk = current_time

        elif (current_time >= new_row_creation_time):
          new_row_creation_time += ts
          price = get_table_last_price(table)
          if not price:
            price = 0

          await create_row(table, current_time, price)

          pk = current_time


async def check_price_presence(price_data):
  for row in price_data:
    table_name = get_table_name(
      row._mapping['ticker_name'], row._mapping['bar_size'])
    hearbeat = row._mapping['heartbeat']
    current_time = int(time.time()*1000)
    hearbeat_after_30min = hearbeat + 30*60*1000

    if hearbeat_after_30min > current_time and table_name not in existing_tables:
      existing_tables.add(table_name)
      heartbeats[table_name] = hearbeat_after_30min
      table = create_price_timestamp_table(table_name)
      asyncio.create_task(update_price_timestamp_table(
        row._mapping['ticker_name'],
        row._mapping['bar_size'],
        table,
        row._mapping['limits']),
        name=table_name)

    elif table_name in existing_tables:
      heartbeats[table_name] = hearbeat_after_30min


async def check_ohlcv_presence(ohlcv_data):
  for row in ohlcv_data:
    table_name = get_table_name(
      row._mapping['ticker_name'], row._mapping['bar_size'], ohlcv=True)
    hearbeat = row._mapping['heartbeat']
    current_time = int(time.time()*1000)
    hearbeat_after_30min = hearbeat + 30*60*1000

    if hearbeat_after_30min > current_time and table_name not in existing_tables:
      existing_tables.add(table_name)
      heartbeats[table_name] = hearbeat_after_30min
      table = create_ohlcv_table(table_name)
      asyncio.create_task(update_ohlcv_table(
        row._mapping['ticker_name'],
        row._mapping['bar_size'],
        table,
        row._mapping['limits']),
        name=table_name)

    elif hearbeat_after_30min > current_time and table_name in existing_tables:
      heartbeats[table_name] = hearbeat_after_30min


def update_heartbeat():
  cloud_sync.execute("UPDATE statuses SET websockets=" +
                      str(int(time.time()*1000)))


async def get_tables():
  while True:
    update_heartbeat()
    async with engine.begin() as cloud:
      price_data = await cloud.execute(watched_price_data.select())
      ohlcv_data = await cloud.execute(watched_ohlcv_data.select())

    await check_price_presence(price_data)
    await check_ohlcv_presence(ohlcv_data)

    await asyncio.sleep(10)


async def run_websocket():
  await get_tables()
  await asyncio.gather(*asyncio.all_tasks())

if __name__ == '__main__':
  loop = asyncio.run(run_websocket())
  exchange.close()
import ccxt
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import pandas as pd
import yfinance as yf
from datetime import datetime
import matplotlib.pyplot as plt
import random
import math

exchange = ccxt.binanceusdm()

vol = ['FIL-USD', 'DOGE-USD', 'SC-USD', 'ENJ-USD', 'JASMY-USD', 'SAND-USD', 'EGLD-USD', 'ZEC-USD',  'COMP-USD', 'NEAR-USD', 'HNT-USD', 'CRV-USD', 'HBAR-USD', 'SNX-USD', 'THETA-USD', 'BTC-USD', 'RVN-USD', 'INJ-USD', 'BTS-USD', 'ETC-USD', 'TRB-USD', 'SOL-USD', 'BNB-USD', 'AVAX-USD', 'UNFI-USD', 'TRX-USD',  'DYDX-USD', 'AXS-USD', 'KLAY-USD', 'WOO-USD', 'UNI-USD','PEOPLE-USD', 'RUNE-USD', 'VET-USD', 'BCH-USD', 'COTI-USD', 'TOMO-USD', 'MKR-USD', 'SFP-USD', 'GALA-USD', 'CHZ-USD', 'XRP-USD', 'DASH-USD', 'APE-USD', 'SPELL-USD', 'KSM-USD', 'BAND-USD', 'RSR-USD', 'MATIC-USD', 'EOS-USD', 'BNX-USD', 'MTL-USD','XTZ-USD', 'ATA-USD', 'AR-USD', 'NKN-USD', 'XLM-USD', 'LIT-USD', '1INCH-USD', 'REEF-USD', 'ARPA-USD', 'ZIL-USD', 'DOT-USD', 'LINA-USD', 'ADA-USD', 'REN-USD', 'ALGO-USD', 'KNC-USD', 'BEL-USD', 'BLZ-USD', 'ATOM-USD', 'ETH-USD', 'BAKE-USD', 'GAL-USD', 'ALPHA-USD', 'LTC-USD', '1000SHIB-USD', 'AUDIO-USD', 'XMR-USD', 'AAVE-USD', 'IOTA-USD', 'LINK-USD', 'YFI-USD', 'TLM-USD', 'LDO-USD', 'SUSHI-USD', 'MANA-USD', 'GMT-USD', 'FLM-USD',  'ENS-USD', 'ICP-USD', 'FTM-USD', 'WAVES-USD']
#tickers = ['BTC-USD', 'ETH-USD','SOL-USD', 'XRP-USD', 'LTC-USD', 'DOGE-USD', 'BNB-USD', 'FTM-USD', 'DYDX-USD', 'MATIC-USD', 'APE-USD', 'ETC-USD']
#oil = ['XOM', 'CVX', 'SHEL', 'TTE', 'COP', 'EQNR', 'BP', 'DUK', 'ENB', 'SO', 'EOG', 'SLB', 'PBR', 'CNQ', 'OXY', 'PXD', 'EPD', 'PSX', 'SLO', 'SRE', 'WDS', 'HES', 'KMI', 'WMB', 'TRP']
#tickers = []

source = 'yfinance'

model_start = '2021-6-01'
model_end = '2022-6-01'
bars_back_model = 100
interval_model = '1d'

#backtest parameters
start = model_start#'2022-12-01'
end = datetime.today()
bars_back = 1000
interval = '1d'

windows =  [
    # ['2019-1-01', '2020-1-01', '2020-1-01',datetime.today()],
    # ['2019-3-01', '2020-3-01', '2020-3-01',datetime.today()],
    # ['2019-6-01', '2020-6-01', '2020-6-01',datetime.today()],
    # ['2019-9-01', '2020-9-01', '2020-9-01',datetime.today()],
    # ['2020-1-01', '2021-1-01', '2021-1-01',datetime.today()],
    # ['2020-3-01', '2021-3-01', '2021-3-01',datetime.today()],
    # ['2020-6-01', '2021-6-01', '2021-6-01',datetime.today()],
    # ['2020-9-01', '2021-9-01', '2021-9-01',datetime.today()],
    ['2021-6-01', '2022-6-01', '2022-1-01',datetime.today()],
    # ['2021-3-01', '2022-3-01', '2021-3-01',datetime.today()],
    # ['2021-6-01', '2022-6-01','2021-6-01',datetime.today()],
    # ['2021-9-01', '2022-9-01','2021-9-01',datetime.today()],
    # ['2022-1-01', '2023-1-01', '2022-1-01',datetime.today()]
    ] #[model_start, model_end, start, end] 

def create_windows(start_date, length, start_time,until_current=True):
    pass

def reduce_tickers(tickers): #attempts to redcue all the non-important varaibles 
    pass

def print_model_stats(jres):

    traces = pd.DataFrame()
    traces['Values'] = jres.lr1

    crit_trace = pd.DataFrame(jres.cvt)
    crit_trace.columns = ['90%', '95%', '99%']

    res_traces = pd.concat([crit_trace, traces], axis=1)

    eigen = pd.DataFrame()
    eigen['Values'] = jres.lr2
    
    crit_eigen = pd.DataFrame(jres.cvm)
    crit_eigen.columns = ['90%', '95%', '99%']

    res_eigen = pd.concat([crit_eigen, eigen], axis=1)

    print(res_eigen)
    print(res_traces)

def get_model(tickers, model_start, model_end, bars_back_model, interval_model):
    print(tickers)
    prices = get_prices(tickers, model_start, model_end, bars_back_model, interval_model)
    jres = coint_johansen(prices, det_order=0, k_ar_diff=1)
    coeff = jres.evec[:,0]
    return jres, coeff

def get_prices(tickers, start=None, end=None, bars_back=None, interval=None):
    if source == 'binance':
        prices = {}
        for ticker in tickers: 
            price, timestamps = get_prices_crypto(ticker, bars_back, interval)
            prices[ticker] = price

        prices_df = pd.DataFrame(prices)
        print(prices_df)
        return prices_df

    elif source == 'yfinance':
        return yf.download(tickers,start, end, interval =interval)['Close']

def get_tickers():
    tickers = ['ETH', 'BNB', 'ADA','DOT', 'LTC', 'BTC','MATIC']

    # for i in range(0,4):
    #     tickers.append(random.choice(vol))

    # tickers = list(set(tickers))

    if source == 'binance':
        res = []
        for t in tickers: 
            res.append(t + 'USDT')

    elif source == "yfinance":
        res = []
        for t in tickers:
            res.append(t+'-USD')

    return res

def get_prices_crypto(ticker, bars_back, bar_period):
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
            print(break123)

        now = exchange.milliseconds()
        res = math.floor(bars_back/1000)

        for i in range(1,res+1):
            date_list.append(now-(time_delta*i*1000*1000))

        date_list = date_list[::-1]

        for date in date_list:
            data = data + exchange.fetch_ohlcv(ticker,bar_period,since=date,limit=1000)

        if bars_back%1000 != 0:
            extra = bars_back%1000
            e_date = date_list[0]+extra*time_delta
            data = data + exchange.fetch_ohlcv(ticker,bar_period,since=e_date,limit=extra)

    prices = []
    timestamps = []

    for d in data: 
        prices.append(d[4])

    for d in data:
        timestamps.append(d[0])

    return prices, timestamps 

def get_portfolio(tickers, start, end, bars_back, interval, coeff):
    prices = get_prices(tickers, start, end, bars_back, interval)
    print(prices)
    print(coeff)
    portf_n_assets = (prices * coeff).sum(axis=1)
    return portf_n_assets

def forward_window_test(windows):
    tickers = get_tickers()
    
    for w in windows:
        
        model_start = w[0]
        model_end = w[1]
        start = w[2]
        end = w[3]
        jres, coeff = get_model(tickers, model_start, model_end, bars_back_model, interval_model)
        time_series = get_portfolio(tickers, start, end, bars_back, interval, coeff)

        plt.plot(time_series)

    plt.show()


def create_sparse_portfolio():
    pass

if __name__ == '__main__':

    #creating a model 

    forward_window_test(windows)
    # while True:
    #     try: 
    #         tickers = get_tickers()
    #         jres, coeff = get_model(tickers, model_start, model_end, bars_back_model, interval_model)

    #         #print_model_stats(jres)
    #         time_series = get_portfolio(tickers, start, end, bars_back, interval, coeff)
    #         plt.plot(time_series)
    #         plt.show()

    #     except Exception as e:
    #         print(e) 
    #         print('Failed')

#['XTZ-USD', 'BEL-USD', 'AR-USD', 'MTL-USD', 'THETA-USD', 'NKN-USD', 'HBAR-USD', 'KSM-USD', 'ALGO-USD', 'EOS-USD', 'CRV-USD']
#['RVN-USD', 'MKR-USD', 'REEF-USD', 'YFI-USD', 'SUSHI-USD', 'ARPA-USD', 'LTC-USD', 'VET-USD', 'ZEC-USD', 'RUNE-USD', 'XRP-USD', 'PEOPLE-USD']
#['XLM-USD', 'INJ-USD', 'ICP-USD', 'LINA-USD', 'BNB-USD', 'MATIC-USD', 'WOO-USD', 'RUNE-USD', 'LDO-USD', 'EGLD-USD', 'LINK-USD']

#What to do: 
#The more assets the smoother a curve generally is. (Not including sparse portfolio generation.)
#See if we are able to use normal pairs trading techniques to make it profitable 

#TLT, SKYY

#['BTC-USD', 'ETH-USD', 'BNB-USD']
#'BTC', 'ETH','BNB','ADA'
#['LINK', 'ETH','BNB','SOL']#


#["PSTG", "BOX", "NTAP", "NTNX"] 2017-2019

#['ETH', 'ADA', 'SOL', 'BNB', 'AAVE']

# eth bnb ada dot ltc btc



# sol 
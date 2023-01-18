import numpy as np
import pandas as pdf
from scipy import stats
import scipy
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss
from statsmodels.tsa.stattools import coint
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import math
import random
import sqlite3
import ccxt 
import tickertape
import pandas as pd
import yfinance as yf

from datetime import datetime, timedelta
from ib_insync import *

import warnings
warnings.filterwarnings("ignore")

#test johansen multivariate code 

conn = sqlite3.connect('test.db')
d = conn.cursor()

software_tickers = ['AYX', 'PHR', 'ZI', 'GENI', 'BILL', 'HUBS', 'RSKD', 'INS', 'GTLB', 'IS', 'RNG', 'PUBM', 'LAW', 'CSOD', 'MNTV', 'RPD', 'PANW', 'APPS', 'BASE', 'ESTC', 'TEAM', 'SQ', 'SMRT', 'TRMB', 'BIGC', 'FTNT', 'INST', 'REKR', 'AVLR', 'VERI', 'AMPL', 'GLOB', 'CSU.TO', 'LPSN', 'CXM', 'FIVN', 'MITK', 'PATH', 'CLVT', 'AI', 'ENPH', 'PAYC', 'DOCN', 'PWSC', 'JAMF', 'NET', 'ASAN', 'ORCL', 'SMAR', 'ZS', 'XM', 'LSPD', 'APP', 'ZM', 'PCTY', 'BL', 'MDB', 'SEMR', 'OKTA', 'WKME', 'SSTI', 'OLO', 'FROG', 'YEXT', 'PEGA', 'ZBRA', 'NOW', 'TENB', 'GOOG', 'APPF', 'PLAN', 'KLTR', 'TOST', 'BSY', 'RDVT', 'S', 'NEWR', 'WDAY', 'DAVA', 'DDOG', 'ESMT', 'BLD', 'ZUO', 'DT', 'PCOR', 'WK', 'TWLO', 'EGH', 'MNDY', 'EVCM', 'ZEN', 'VEEV', 'CRWD', 'PD', 'TTD', 'PLTR', 'CRM', 'TYL', 'FORG', 'FRSH', 'DOCU', 'SPT', 'CFLT', 'STEM', 'CDAY', 'CLBT', 'COUP', 'SNOW', 'EVBG', 'HCP', 'IOT', 'BRZE', 'USER', 'EXFY']

sec = None
secs = []
all_data = {}

#Equities

bar_size= '1 day'
duration = '5 Y'
today = datetime.now()
final = []

#Crypto
exchange = ccxt.binanceusdm() 
binance_pairs = ['T/USDT', 'BEAR/USDT', 'BTC/USDT', 'PNT/USDT', 'AMP/USDT', 'NANO/USDT', 'ETHBULL/USDT', 'MFT/USDT', '1INCH/USDT', 'LPT/USDT', 'NEXO/USDT', 'RAD/USDT', 'OCEAN/USDT', 'XTZUP/USDT', 'HC/USDT', 'RAY/USDT', 'FOR/USDT', 'AXS/USDT', 'TCT/USDT', 'KEEP/USDT', 'ALICE/USDT', 'ACA/USDT', 'OG/USDT', 'WTC/USDT', 'KAVA/USDT', 'TLM/USDT', 'ZEC/USDT', 'AVAX/USDT', 'IMX/USDT', 'DAI/USDT', 'FIO/USDT', 'CVP/USDT', 'RNDR/USDT', 'EOSDOWN/USDT', 'ENS/USDT', 'PHA/USDT', 'DOTDOWN/USDT', 'GTC/USDT', 'SAND/USDT', 'FUN/USDT', 'ORN/USDT', 'TORN/USDT', 'BTCST/USDT', 'BTCUP/USDT', 'NBS/USDT', 'USDSB/USDT', 'YFIUP/USDT', 'FORTH/USDT', 'MULTI/USDT', 'STORJ/USDT', 'KP3R/USDT', 'CTSI/USDT', 'SFP/USDT', 'BCC/USDT', 'PAX/USDT', 'BAL/USDT', 'KNC/USDT', 'PERL/USDT', 'ONE/USDT', 'IRIS/USDT', '1INCHUP/USDT', 'MANA/USDT', 'SOL/USDT', 'NPXS/USDT', 'FTT/USDT', 'AGLD/USDT', 'PORTO/USDT', 'QI/USDT', 'ONG/USDT', 'REP/USDT', 'XVS/USDT', 'MC/USDT', 'PAXG/USDT', 'XLMDOWN/USDT', 'WAN/USDT', 'ETHDOWN/USDT', 'DEXE/USDT', 'FXS/USDT', 'MBL/USDT', 'PSG/USDT', 'CLV/USDT', 'GAL/USDT', 'DOGE/USDT', 'MOB/USDT', 'NU/USDT', 'WAXP/USDT', 'XEC/USDT', 'DOCK/USDT', 'NBT/USDT', 'DCR/USDT', 'BTG/USDT', 'STEEM/USDT', 'TWT/USDT', 'ANC/USDT', 'VEN/USDT', 'LDO/USDT', 'AUDIO/USDT', 'OGN/USDT', 'RSR/USDT', 'BLZ/USDT', 'HARD/USDT', 'ERD/USDT', 'IOTA/USDT', 'CKB/USDT', 'SYS/USDT', 'BCHUP/USDT', 'BIFI/USDT', 'LTO/USDT', 'SKL/USDT', 'EPX/USDT', 'AR/USDT', 'TRB/USDT', 'BAND/USDT', 'UMA/USDT', 'SUPER/USDT', 'LINA/USDT', 'FLOW/USDT', 'YFIDOWN/USDT', 'AUCTION/USDT', 'BNT/USDT', 'DYDX/USDT', 'NEO/USDT', 'SLP/USDT', 'JOE/USDT', 'ALGO/USDT', 'FET/USDT', 'VOXEL/USDT', 'XTZ/USDT', 'BCH/USDT', 'TRU/USDT', 'API3/USDT', 'BTT/USDT', 'EPS/USDT', 'BNBBULL/USDT', 'DODO/USDT', 'BAKE/USDT', 'GALA/USDT', 'BAR/USDT', 'OXT/USDT', 'APE/USDT', 'PEOPLE/USDT', 'MIR/USDT', 'MITH/USDT', 'CITY/USDT', 'UNI/USDT', 'YFI/USDT', 'LRC/USDT', 'EUR/USDT', 'ALPHA/USDT', 'TRXDOWN/USDT', 'LOKA/USDT', 'BNBDOWN/USDT', 'ANT/USDT', 'ANKR/USDT', 'BETA/USDT', 'REQ/USDT', 'LINKDOWN/USDT', 'UNIUP/USDT', 'WNXM/USDT', 'LEND/USDT', 'POND/USDT', 'MKR/USDT', 'CHZ/USDT', 'ALPACA/USDT', 'RAMP/USDT', 'BNBUP/USDT', 'ATM/USDT', 'ICP/USDT', 'ARPA/USDT', 'MASK/USDT', 'HIVE/USDT', 'BUSD/USDT', 'LAZIO/USDT', 'STRAT/USDT', 'JUV/USDT', 'VITE/USDT', 'ALCX/USDT', 'WIN/USDT', 'SHIB/USDT', 'XRPUP/USDT', 'RLC/USDT', 'LTC/USDT', 'SRM/USDT', 'DATA/USDT', 'YFII/USDT', 'NEAR/USDT', 'ATA/USDT', 'DF/USDT', 'GXS/USDT', 'DEGO/USDT', 'PLA/USDT', 'XNO/USDT', 'ERN/USDT', 'SUSHIDOWN/USDT', 'NMR/USDT', 'SUSHIUP/USDT', 'NKN/USDT', 'BTCDOWN/USDT', 'ADAUP/USDT', 'CVC/USDT', 'BKRW/USDT', 'FILUP/USDT', 'SANTOS/USDT', 'MTL/USDT', 'ELF/USDT', 'ZIL/USDT', 'EOS/USDT', 'TROY/USDT', 'SCRT/USDT', 'CELR/USDT', 'STPT/USDT', 'BURGER/USDT', 'EOSUP/USDT', 'DENT/USDT', 'VGX/USDT', 'KSM/USDT', 'SNX/USDT', 'QUICK/USDT', 'STX/USDT', 'FARM/USDT', 'ADADOWN/USDT', 'XTZDOWN/USDT', 'REEF/USDT', 'DAR/USDT', 'BCHDOWN/USDT', 'ARDR/USDT', 'AAVE/USDT', 'HIGH/USDT', 'CVX/USDT', 'WING/USDT', 'BEAM/USDT', 'VIDT/USDT', 'ETC/USDT', 'LTCUP/USDT', 'GMT/USDT', 'WOO/USDT', 'SXPUP/USDT', 'SPELL/USDT', 'RVN/USDT', 'ASR/USDT', 'CHR/USDT', 'JST/USDT', 'RGT/USDT', 'MBOX/USDT', 'BSW/USDT', 'SC/USDT', 'EOSBULL/USDT', 'LINK/USDT', 'SUSHI/USDT', 'AAVEUP/USDT', 'HBAR/USDT', 'OOKI/USDT', 'XVG/USDT', 'EOSBEAR/USDT', 'FLUX/USDT', 'ACH/USDT', 'GLMR/USDT', 'OM/USDT', 'XRPBULL/USDT', 'AAVEDOWN/USDT', 'CFX/USDT', 'COTI/USDT', 'FLM/USDT', 'INJ/USDT', 'ENJ/USDT', 'MATIC/USDT', 'DASH/USDT', 'CHESS/USDT', 'EGLD/USDT', 'ASTR/USDT', 'SXP/USDT', 'QTUM/USDT', 'ACM/USDT', 'AVA/USDT', 'HNT/USDT', 'ZEN/USDT', 'CELO/USDT', 'MLN/USDT', 'AKRO/USDT', 'POLY/USDT', 'SUSD/USDT', 'TKO/USDT', 'TRX/USDT', 'OMG/USDT', 'GHST/USDT', 'DOTUP/USDT', 'FRONT/USDT', 'TOMO/USDT', 'CTK/USDT', 'UST/USDT', 'CAKE/USDT', 'FILDOWN/USDT', 'SUN/USDT', 'KDA/USDT', 'TFUEL/USDT', 'XEM/USDT', 'TRIBE/USDT', 'XZC/USDT', 'UNFI/USDT', 'RARE/USDT', 'COCOS/USDT', 'RUNE/USDT', 'BOND/USDT', 'AUTO/USDT', 'CTXC/USDT', 'ADX/USDT', 'STORM/USDT', 'CRV/USDT', 'ADA/USDT', 'XLMUP/USDT', 'SXPDOWN/USDT', 'DIA/USDT', '1INCHDOWN/USDT', 'TRXUP/USDT', 'DOT/USDT', 'COMP/USDT', 'ATOM/USDT', 'ILV/USDT', 'BTTC/USDT', 'IOST/USDT', 'JASMY/USDT', 'ETHUP/USDT', 'THETA/USDT', 'HOT/USDT', 'KMD/USDT', 'FIS/USDT', 'MCO/USDT', 'RIF/USDT', 'NULS/USDT', 'ANY/USDT', 'VET/USDT', 'LIT/USDT', 'ETH/USDT', 'BEL/USDT', 'LINKUP/USDT', 'USDS/USDT', 'LUNA/USDT', 'REI/USDT', 'USDC/USDT', 'DGB/USDT', 'FIDA/USDT', 'GBP/USDT', 'XLM/USDT', 'OP/USDT', 'GRT/USDT', 'ONT/USDT', 'BNX/USDT', 'UNIDOWN/USDT', 'BADGER/USDT', 'PUNDIX/USDT', 'FIL/USDT', 'PERP/USDT', 'DREP/USDT', 'ICX/USDT', 'GNO/USDT', 'ROSE/USDT', 'BAT/USDT', 'DNT/USDT', 'ALPINE/USDT', 'REN/USDT', 'BULL/USDT', 'KLAY/USDT', 'BICO/USDT', 'QNT/USDT', 'COS/USDT', 'GTO/USDT', 'FTM/USDT', 'MINA/USDT', 'UTK/USDT', 'YGG/USDT', 'AUD/USDT', 'ETHBEAR/USDT', 'WAVES/USDT', 'IDEX/USDT', 'XRPBEAR/USDT', 'WRX/USDT', 'STRAX/USDT', 'MDX/USDT', 'LTCDOWN/USDT', 'AION/USDT', 'KEY/USDT', 'BNB/USDT', 'C98/USDT', 'TUSD/USDT', 'STMX/USDT', 'ZRX/USDT', 'VTHO/USDT', 'BSV/USDT', 'BZRX/USDT', 'POWR/USDT', 'POLS/USDT', 'FIRO/USDT', 'BNBBEAR/USDT', 'MOVR/USDT', 'IOTX/USDT', 'USDP/USDT', 'DUSK/USDT', 'PYR/USDT', 'LSK/USDT', 'TVK/USDT', 'XMR/USDT', 'BTS/USDT', 'MDT/USDT']
binance_futures_usd = ['DGB/USDT', '1INCH/USDT', 'OP/USDT', 'DEFI/USDT', 'ANKR/USDT', 'KNC/USDT', 'BTCDOM/USDT', 'HOT/USDT', 'ZIL/USDT', 'LTC/USDT', 'BAT/USDT', 'REEF/USDT', 'ONT/USDT', 'TRB/USDT', 'BEL/USDT', 'GMT/USDT', 'ATOM/USDT', 'REN/USDT', 'ADA/USDT', 'LRC/USDT', 'DOGE/USDT', 'SOL/USDT', 'DOT/USDT', 'NKN/USDT', 'STMX/USDT', 'XEM/USDT', 'BNX/USDT', 'DUSK/USDT', 'ICX/USDT', 'SUSHI/USDT', 'DASH/USDT', 'IOTA/USDT', 'IOST/USDT', 'MKR/USDT', 'WAVES/USDT', 'TLM/USDT', 'CRV/USDT', 'EOS/USDT', 'DAR/USDT', 'KLAY/USDT', 'NEAR/USDT', 'ZEN/USDT', 'LINK/USDT', 'WOO/USDT', 'FLOW/USDT', 'UNFI/USDT', 'BAND/USDT', 'ANT/USDT', 'TRX/USDT', 'DENT/USDT', 'RSR/USDT', 'EGLD/USDT', 'GAL/USDT', 'C98/USDT', 'SAND/USDT', 'ALPHA/USDT', 'MANA/USDT', 'GTC/USDT', 'MATIC/USDT', 'XMR/USDT', 'ALICE/USDT', 'OMG/USDT', 'IOTX/USDT', 'ROSE/USDT', 'KAVA/USDT', 'CELO/USDT', 'BTC/USDT', 'AAVE/USDT', 'ONE/USDT', 'ALGO/USDT', 'COMP/USDT', 'YFI/USDT', 'XRP/USDT', 'GRT/USDT', 'BNB/USDT', 'ARPA/USDT', 'RVN/USDT', 'ATA/USDT', 'VET/USDT', 'SXP/USDT', 'CHR/USDT', 'ZRX/USDT', 'IMX/USDT', 'LPT/USDT', 'DYDX/USDT', 'BTS/USDT', 'AR/USDT', 'MASK/USDT', 'LIT/USDT', 'CTSI/USDT', 'SKL/USDT', '1000XEC/USDT', 'MTL/USDT', 'BLZ/USDT', 'HBAR/USDT', 'COTI/USDT', 'KSM/USDT', 'PEOPLE/USDT', 'HNT/USDT', 'API3/USDT', 'QTUM/USDT', 'BAKE/USDT', 'FIL/USDT', 'OCEAN/USDT', 'FTM/USDT', 'NEO/USDT', 'SNX/USDT', 'RLC/USDT', 'AUDIO/USDT', 'THETA/USDT', 'CVC/USDT', 'CELR/USDT', 'STORJ/USDT', 'FLM/USDT', 'APE/USDT', 'ETH/USDT', 'ZEC/USDT', 'SFP/USDT', 'UNI/USDT', 'ENJ/USDT', 'GALA/USDT', 'ETC/USDT', 'CTK/USDT', 'XTZ/USDT', 'FTT/USDT', 'SC/USDT', '1000SHIB/USDT', 'RAY/USDT', 'RUNE/USDT', 'XLM/USDT', 'ICP/USDT', 'ENS/USDT', 'JASMY/USDT', 'SRM/USDT', 'LINA/USDT', 'CHZ/USDT', 'BAL/USDT', 'OGN/USDT', 'AXS/USDT', 'AVAX/USDT', 'TOMO/USDT', 'BCH/USDT']
#busd = ['FIL/BUSD', 'NEAR/BUSD', 'ETC/BUSD', 'LTC/BUSD', 'AUCTION/BUSD', 'FTT/BUSD', 'ICP/BUSD', 'CVX/BUSD', 'AMB/BUSD', 'APE/BUSD', 'BTC/BUSD', 'ANC/BUSD', 'LUNA2/BUSD', 'BNB/BUSD', 'DOGE/BUSD', 'TRX/BUSD', 'PHB/BUSD', 'LDO/BUSD', 'SOL/BUSD', 'SAND/BUSD', 'XRP/BUSD', 'GALA/BUSD', 'AVAX/BUSD', 'DODO/BUSD', 'MATIC/BUSD', 'TLM/BUSD', 'LINK/BUSD', 'GMT/BUSD', '1000LUNC/BUSD', 'FTM/BUSD', 'GAL/BUSD', 'WAVES/BUSD', 'ADA/BUSD', '1000SHIB/BUSD', 'UNI/BUSD', 'DOT/BUSD', 'LEVER/BUSD', 'ETH/BUSD']
vol = ['FIL/USDT', 'BTC/BUSD', 'DOGE/USDT', 'SC/USDT', 'ENJ/USDT', 'JASMY/USDT', 'SAND/USDT', 'EGLD/USDT', 'ZEC/USDT', 'LUNA2/BUSD', 'COMP/USDT', 'NEAR/USDT', 'HNT/USDT', 'CRV/USDT', 'HBAR/USDT', 'SNX/USDT', 'THETA/USDT', 'BTC/USDT', 'RVN/USDT', 'INJ/USDT', 'ADA/BUSD', 'BTS/USDT', 'ETC/USDT', 'TRB/USDT', 'SOL/USDT', 'BNB/USDT', 'AVAX/USDT', 'UNFI/USDT', 'TRX/USDT', 'WAVES/BUSD', 'DYDX/USDT', 'AXS/USDT', 'KLAY/USDT', 'WOO/USDT', 'UNI/USDT', 'BNB/BUSD', 'ETHUSDT_221230', 'PEOPLE/USDT', 'RUNE/USDT', 'VET/USDT', 'BCH/USDT', 'COTI/USDT', 'TOMO/USDT', 'MKR/USDT', 'SFP/USDT', 'GALA/USDT', 'CHZ/USDT', 'XRP/USDT', 'DASH/USDT', 'SOL/BUSD', 'APE/USDT', 'SPELL/USDT', 'KSM/USDT', 'BAND/USDT', 'OP/USDT', 'RSR/USDT', 'MATIC/USDT', 'EOS/USDT', 'BNX/USDT', 'MTL/USDT', 'MATIC/BUSD', 'XTZ/USDT', 'ATA/USDT', 'AR/USDT', 'NKN/USDT', 'XLM/USDT', 'LIT/USDT', '1INCH/USDT', 'DODO/BUSD', 'REEF/USDT', 'ARPA/USDT', 'ZIL/USDT', 'DOT/USDT', 'LINK/BUSD', 'LUNA2/USDT', 'LINA/USDT', 'ADA/USDT', 'REN/USDT', 'ALGO/USDT', 'KNC/USDT', 'BEL/USDT', '1000LUNC/BUSD', 'BLZ/USDT', 'ATOM/USDT', 'ETH/USDT', 'BAKE/USDT', 'GAL/USDT', '1000LUNC/USDT', 'ALPHA/USDT', 'LTC/USDT', 'BTCUSDT_221230', '1000SHIB/USDT', 'AUDIO/USDT', 'XMR/USDT', 'AAVE/USDT', 'IOTA/USDT', 'AUCTION/BUSD', 'LINK/USDT', 'YFI/USDT', 'TLM/USDT', 'ETH/BUSD', 'LDO/USDT', 'SUSHI/USDT', 'MANA/USDT', 'GMT/USDT', 'FLM/USDT', 'XRP/BUSD', 'ENS/USDT', 'ICP/USDT', 'FTM/USDT', 'WAVES/USDT', 'FOOTBALL/USDT']
busd = ['BTC/BUSD', 'ETH/BUSD', '1000LUNC/BUSD', 'XRP/BUSD', 'SOL/BUSD','BNB/BUSD', 'LUNA2/BUSD', 'DOGE/BUSD', 'GALA/BUSD', 'ADA/BUSD','MATIC/BUSD', 'APE/BUSD'] # 10 MILLION+ DAILY VOL , 'LINK/BUSD', 'GMT/BUSD', 'DOT/BUSD','TRX/BUSD','ETC/BUSD', 'AVAX/BUSD', 'ANC/BUSD','LTC/BUSD', 'NEAR/BUSD', 'ETC/BUSD', 'LTC/BUSD', '1000SHIB/BUSD', 'UNI/BUSD'] 5 MILLION+ DAILY VOL
bar_period = '5m'
bars_back = 300
mil100 = ['ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'LTCUSDT', 'MATICUSDT', 'LTCUSDT', 'LINKUSDT', 'SOLUSDT', 'APEUSDT', 'DYDXUSDT', 'RLCUSDT', 'APTUSDT', 'BCHUSDT', 'NEARUSDT', '1000SHIB/USDT', 'NEARUSDT', 'DOTUSDT', 'CRVUSDT', 'CRVUSDT','EOSUSDT', 'OPUSDT', 'AVAXUSDT', 'ADAUSDT', 'ETCUSDT', 'OCEANUSDT', 'AXSUSDT', 'FTMUSDT', 'MASKUSDT','BTCUSDT']
#Using a windows
#from_date = 

start_date = today - timedelta(days=250)

num = round(random.random()*100)

# ib = IB()
# ib.connect(host='127.0.0.1', port=4002, clientId=num)

#supporting functions

def johansen_multi(tickers):
    dict1 = {}
    
    for t in tickers: 
        prices = []
        data = exchange.fetch_ohlcv(t,'1h',limit=1000)
        for d in data: 
            prices.append(d[4])
        dict1[t] = prices

    g = pd.DataFrame(dict1) 

    jres = coint_johansen(g, det_order=0, k_ar_diff=0)

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
    print(jres.evec[:,0])

def johansen_multivariate(tickers, bar_duration=bar_period): #***needs a little bit of tweaking
    data = {}
    for ticker in tickers: 
        try: 
            if len(ticker) > 5:
                d, a = get_prices_crypto(ticker, bar_duration)
            else: 
                d = get_historical_prices(ticker)
            data[ticker] = d
        except:
            print('Issue getting price, skipping!')

    df = pd.DataFrame(data)

    jres = coint_johansen(df, det_order=0, k_ar_diff=0)

    eigen_stats = jres.lr2
    crit_eigen = jres.cvm
    trace_stats = jres.lr1
    crit_trace = jres.cvt

    df_eigen_stats = pd.DataFrame(eigen_stats)
    df_crit_eigen = pd.DataFrame(crit_eigen)
    df_trace_stats = pd.DataFrame(trace_stats)
    df_crit_trace = pd.DataFrame(crit_trace)

    df_crit_eigen.columns, df_crit_trace.columns = ['90%', '95%', '99%'], ['90%', '95%', '99%']
    print(eigen_stats)
    print(crit_eigen)

    print(jres.eig)
    print(pd.DataFrame(jres.evec))

    pass

    for i in range(len(eigen_stats)):
        if eigen_stats[i] >= crit_eigen[i][1]:
            pass
        else:
            return False

    for i in range(len(trace_stats)):
        if trace_stats[i] >= crit_trace[i][1]:
            pass
        else:
            return False

    return True

def get_historical_prices(ticker1, from_date = start_date, bar_size1=bar_size, duration1=duration):
    today = datetime.today()
    data1 = []
    stock = Stock(ticker1, 'SMART','USD')
    ib.qualifyContracts(stock)

    data = ib.reqHistoricalData(stock, from_date, duration1,bar_size1, 'ASK', True, formatDate=1, keepUpToDate=False, chartOptions=[], timeout=60)
    for d in data:
        data1.append(d.close)
    return data1

# def get_prices_crypto(ticker):
#     data = exchange.fetch_ohlcv(ticker,bar_period,limit=bars_back)
#     prices = []

#     for d in data: 
#         prices.append(d[4])

#     return prices

def get_prices_crypto(ticker, bar_duration):
    if bars_back <= 1000:
        data = exchange.fetch_ohlcv(ticker,bar_duration,limit=bars_back)
        
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
            data = data + exchange.fetch_ohlcv(ticker,bar_duration,since=date,limit=1000)

        if bars_back%1000 != 0:
            extra = bars_back%1000
            e_date = date_list[0]+extra*time_delta
            data = data + exchange.fetch_ohlcv(ticker,bar_duration,since=e_date,limit=extra)

    prices = []

    for d in data: 
        prices.append(d[4])

    return prices, data


def find_spread(ticker_a_prices, ticker_b_prices):

    m,b = np.polyfit(ticker_a_prices,ticker_b_prices,1)
    spread = []

    for i in range(len(ticker_a_prices)):
        spread.append(math.log(ticker_a_prices[i]) - m*math.log(ticker_b_prices[i]))    
    
    return spread

#relevent tests

def johansen(series1, series2): #THe dataframe needs to have both time series. Improved version of engle granger. Som eignevvalues are negative, may be stationary
    x = series1
    y = series2
    d = {'col1': x, 'col2': y}
    g = pd.DataFrame(d) 
    try: 
        jres = coint_johansen(g, det_order=0, k_ar_diff=0)
    except:
        print('There was an error with the test, returning false')
        return False

    eigen_stats = jres.lr2
    crit_eigen = jres.cvm
    trace_stats = jres.lr1
    crit_trace = jres.cvt

    df_eigen_stats = pd.DataFrame(eigen_stats)
    df_crit_eigen = pd.DataFrame(crit_eigen)
    df_trace_stats = pd.DataFrame(trace_stats)
    df_crit_trace = pd.DataFrame(crit_trace)

    df_crit_eigen.columns, df_crit_trace.columns = ['90%', '95%', '99%'], ['90%', '95%', '99%']

    if eigen_stats[1] >= crit_eigen[1][1]:
        return False
    if eigen_stats[0] >= crit_eigen[0][1]:
        return True

    return False

def engle_granger(series1, series2): #tests if the two series are cointegrated. ***If p value is below 0.05 then we can assume the series is cointegrated. 
    results = coint(series1, series2)
    t_stat = results[0]
    p_value = results[1]
    crit_value = results[2]
    if p_value <= 0.055: 
        return True
    return False

def kpss_test(timeseries): #tests if the series is trend stationary (does not check for unit root). If the LM statistic is greater than the critical value, then the series is non stationary; should be lower than critical values
    #print ('Results of KPSS Test:')
    kpsstest = kpss(timeseries, regression='c', nlags="auto")
    kpss_output = pd.Series(kpsstest[0:3], index=['Test Statistic','p-value','#Lags Used'])
    for key,value in kpsstest[3].items():
        kpss_output['Critical Value (%s)'%key] = value
    # print(kpss_output)
    return kpss_output

def get_hurst_exponent(time_series, max_lag=5): #classifies trend as mean reverting, trending, or a random walk. < .5 may be stationary, >.5 is known to be trending...so we want as close to zero as possible. 
    """Returns the Hurst Exponent of the time series"""
    
    lags = range(2, max_lag)

    # variances of the lagged differences
    tau = [np.std(np.subtract(time_series[lag:], time_series[:-lag])) for lag in lags]

    # calculate the slope of the log plot -> the Hurst Exponent
    reg = np.polyfit(np.log(lags), np.log(tau), 1)

    return reg[0]

def pearson_correlation(series_a, series_b):
    val = scipy.stats.pearsonr(series_a, series_b)
    if val > 0.8:
        return True
    return False

def get_data(tickers,bar_duration=bar_period):
    data, pairs = {},[]

    for ticker in tickers: 
        try: 
            if len(ticker) > 5:
                d,a = get_prices_crypto(ticker, bar_duration)
            else: 
                d,a = get_historical_prices(ticker)
            data[ticker] = d
        except:
            print(ticker)
            print('Issue getting price, skipping!')

    for ticker in tickers:
        for tick in tickers: 
            if ticker == tick:
                pass
            else:
                pairs.append([ticker,tick])
    return data, pairs

def coint_test(data, pairs):
    j, eg, h, k = [],[],[],[]

    
    for pair in pairs: 
        try:
            series1 = data[pair[0]]
            series2 = data[pair[1]]
        
            if len(series1) != len(series2):
                continue

            # johansen
            if johansen(series1, series2): 
                j.append(pair)

            #engle granger test
            try: 
                res = engle_granger(series1, series2)
                if res == True:
                    eg.append(pair)
            except:
                pass
        
            try: 
                spread = find_spread(series1, series2)
            except: 
                print('Issue finding spread. Skipping...')
                continue

            #kpss and hurst tests
            kpss_output = kpss_test(spread)
            if kpss_output[1] == 0.1 or kpss_output[1] > kpss_output[4]:
                k.append(pair)

            hurst = get_hurst_exponent(spread)
            if hurst < 0.45:
                h.append(pair)
        except:
            pass

        
    return j, eg, h, k


def adf_test(timeseries): #tests if the series is stationary. Do not need to use as it is integrated into the engle granger test. 
    #print ('Results of Dickey-Fuller Test:')
    dftest = adfuller(timeseries, autolag='AIC')
    #dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    # for key,value in dftest[4].items():
    #     dfoutput['Critical Value (%s)'%key] = value
    #print (dfoutput)
    if dftest[0] < dftest[4]['5%']:
        return True

def return_sectors(sec1):
    sectors = ['Communications', 'Electronic Technology', 'Health Technology', 'Non-Energy Minerals', 'Miscellaneous', 'Retail Trade', 
    'Technology Services', 'Industrial Services', 'Process Industries', 'Distribution Services', 'Consumer Non-Durables', 'Transportation', 
    'Consumer Durables', 'Producer Manufacturing', 'Finance', 'Utilities', 'Energy Minerals', 'Consumer Services', 'Commercial Services', 'Health Services']
    d = pd.read_csv('tickers.csv')
    df = d[d.industry==sec1]
    return df.tolist()

def return_industry(ind1):
    industries = ['Auto Parts: OEM', 'Recreational Products', 'Other Consumer Services', 'Electronics/Appliances', 'Financial Publishing/Services', 
                'Construction Materials', 'Textiles', 'Food Distributors', 'Computer Communications', 'Wireless Telecommunications', 'Pharmaceuticals: Other', 
                'Food: Specialty/Candy', 'Environmental Services', 'Aluminum', 'Miscellaneous Commercial Services', 'Trucking', 'Publishing: Books/Magazines', 
                'Water Utilities', 'Specialty Insurance', 'Other Consumer Specialties', 'Coal', 'Office Equipment/Supplies', 'Hotels/Resorts/Cruise lines', 
                'Industrial Specialties', 'Chemicals: Agricultural', 'Marine Shipping', 'Industrial Conglomerates', 'Discount Stores', 'Specialty Stores', 
                'Chemicals: Specialty', 'Consumer Sundries', 'Building Products', 'Movies/Entertainment', 'Alternative Power Generation', 'Electrical Products', 
                'Major Telecommunications', 'Real Estate Investment Trusts', 'Services to the Health Industry', 'Household/Personal Care', 'Drugstore Chains', 
                'Precious Metals', 'Publishing: Newspapers', 'Steel', 'Medical/Nursing Services', 'Metal Fabrication', 'Wholesale Distributors', 'Apparel/Footwear', 
                'Integrated Oil', 'Food: Meat/Fish/Dairy', 'Real Estate Development', 'Commercial Printing/Forms', 'Chemicals: Major Diversified', 'Financial Conglomerates', 
                'Automotive Aftermarket', 'Multi-Line Insurance', 'Electronics Distributors', 'Oil & Gas Production', 'Casinos/Gaming', 'Medical Specialties', 'Oil & Gas Pipelines', 
                'Contract Drilling', 'Pulp & Paper', 'Data Processing Services', 'Home Furnishings', 'Internet Software/Services', 'Pharmaceuticals: Major', 'Medical Distributors', 
                'Investment Banks/Brokers', 'Hospital/Nursing Management', 'Major Banks', 'Media Conglomerates', 'Electronic Equipment/Instruments', 'Gas Distributors', 'Miscellaneous', 
                'Electronic Production Equipment', 'Internet Retail', 'Oil Refining/Marketing', 'Other Metals/Minerals', 'Property/Casualty Insurance', 'Containers/Packaging', 
                'Personnel Services', 'Miscellaneous Manufacturing', 'Advertising/Marketing Services', 'Savings Banks', 'Industrial Machinery', 'Oilfield Services/Equipment', 
                'Computer Processing Hardware', 'Information Technology Services', 'Engineering & Construction', 'Trucks/Construction/Farm Machinery', 'Broadcasting', 'Computer Peripherals', 
                'Catalog/Specialty Distribution', 'Specialty Telecommunications', 'Investment Managers', 'Forest Products', 'Railroads', 'Other Transportation', 'Electronics/Appliance Stores', 
                'Homebuilding', 'Electronic Components', 'Aerospace & Defense', 'Tools & Hardware', 'Agricultural Commodities/Milling', 'Telecommunications Equipment', 'Regional Banks', 
                'Packaged Software', 'Air Freight/Couriers', 'Beverages: Non-Alcoholic', 'Finance/Rental/Leasing', 'Beverages: Alcoholic', 'Tobacco', 'Managed Health Care', 'Department Stores', 
                'Restaurants', 'Airlines', 'Semiconductors', 'Biotechnology', 'Apparel/Footwear Retail', 'Food: Major Diversified', 'Cable/Satellite TV', 'Life/Health Insurance', 'Electric Utilities',
                'Motor Vehicles', 'Insurance Brokers/Services', 'Food Retail', 'Home Improvement Chains', 'Investment Trusts/Mutual Funds']
    d = pd.read_csv('tickers.csv')
    df = d[d.industry==ind1]
    return df.tolist()

def print_res(j,eg,h,k):
    print(j)
    print(eg)
    print(h)
    print(k)
    all = []
    jh, egh,jk, egk, jegh, jegk = [], [], [], [],[],[]
    l = j 
    for t in eg: 
        if t not in l:
            l.append(t)

    for pair in l: 
        if pair in j:
            if pair in eg:
                if pair in h:
                    if pair in k:
                        all.append(pair)
                        continue
                    else:
                        jegh.append(pair)
                        continue
                elif pair in k:
                    jegk.append(pair)
                    continue
            if pair in h:
                jh.append(pair)
                continue
            if pair in k:
                jk.append(pair)
                continue
        if pair in eg: 
            if pair in h:
                egh.append(pair)
                continue
            if pair in k:
                egk.append(pair)
                continue


    print('All: ' + str(all))
    print()
    print('Johansen + EG + Hurst: ' + str(jegh))
    print()
    print('Johansen + EG + KPSS: ' + str(jegk))
    print()
    print('Johansen + Hurst: ' + str(jh))
    print()
    print('Engle-Granger + Hurst: ' + str(egh))
    print()
    print('Johansen + KPSS: ' + str(jk))
    print()
    print('Engle-Granger + KPSS: ' + str(egk))

def write_to_sql(j,eg,h,k):
    df = pd.DataFrame({'johansen': [str(j)],'engle_granger':[str(eg)], 'hurst': [str(h)], 'kpss':[str(k)]})
    d.execute("DROP TABLE discovery")
    df.to_sql(name='discovery', con=conn)

def test_pairs(pairs, bar_duration=bar_period): #***test this
    res = {}
    for pair in pairs:
        if len(pair[0]) > 4:
            prices_a,a = get_prices_crypto(pair[0], bar_duration)
            prices_b,a = get_prices_crypto(pair[1], bar_duration)
        else:
            print('Need to implement pricing for equities')
            print(break123)
        equity_curve, equity = tickertape.tickertape(prices_a, prices_b, moving_avg_period=100)
        #***can enter a filer
        res[pair] = equity

    return res

def get_correlation_coef(prices_a, prices_b):
    res = stats.pearsonr(prices_a, prices_b)
    return res[0]

def multiple_test(bars):
    #initial setup to get the first list 
    prior_list = mil100
    for i in range(len(bars)): 

        curr_list = []
        if i == 0: 
            data, pairs = get_data(prior_list, bar_duration=bars[i]) #need to get data for all the individual tickers in the pairs

        else: 
            ticker_list = []
            for pair in prior_list: 
                ticker_list.append(pair[0])
                ticker_list.append(pair[1])
            ticker_list = list(set(ticker_list))
            data, pairs = get_data(ticker_list, bar_duration=bars[i])
        j, eg, h, k = coint_test(data,pairs)

        for pair in pairs: 
            num = 0
            if pair in j:
                num += 1
            if pair in eg: 
                num += 1 
            if pair in h: 
                num += 1
            if pair in k: 
                num += 1
            if num >= 2:
                curr_list.append(pair)
        prior_list = curr_list
    return prior_list

if __name__ == '__main__': #set up look foward period, test from years 1-3, and then check year 1 on same time periods

    res1 = multiple_test(['12h', '4h', '1h'])
    print(res1)

    # data, pairs = get_data(mil100)
    # print('Got all of the data needed!')

    # j, eg, h, k = coint_test(data,pairs)
    # print(j)
    # print(eg)
    # print(h)
    # print(k)
    # print_res(j,eg,h,k)
    # write_to_sql(j,eg,h,k)

#covariance matrix, where the covariance matrix will go in the future or assume its constant, corrlations collapse at lower timeframes, covariance matrix produces constant mean?
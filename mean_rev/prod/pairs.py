# template.run_crypto(8,'IOST/USDT', 'ZEC/USDT','5m',50,2.7,avg_type_input='ema',bol_band_type='ema',band_vol_exp=1.4,stop_loss=-0.015) #possible issue with bol band type and band vol exp -- monitoring length or curr data
# template.run_crypto(18,'ANKR/USDT', 'SKL/USDT','15m',75,2.5, avg_type_input='ema',bol_band_type='ema',stop_loss=-0.03,band_vol_exp=1.5,stoploss_break=False)
# template.run_crypto(14,'BLZ/USDT', 'SKL/USDT','15m',100,2.5, avg_type_input='ema',bol_band_type='sma',stop_loss=-0.02,band_vol_exp=1,stoploss_break=False)
# template.run_crypto(29,'TLM/BUSD', 'ICP/BUSD','15m',50,3.5,avg_type_input='sma',stop_loss=-0.01, bol_band_type='sma',band_vol_exp=0,stoploss_break=False)

#Exp Bollinger Bands
template.run_crypto(11,'ETH/BUSD', 'ADA/BUSD','5m',75,3,avg_type_input='ema',stop_loss=-0.02, bol_band_type='ema',band_vol_exp=1.5,stoploss_break=False)
template.run_crypto(16,'GALA/BUSD', 'ADA/BUSD','5m',100,3,avg_type_input='ema',stop_loss=-0.02, bol_band_type='ema',band_vol_exp=1.5,stoploss_break=False)
template.run_crypto(17,'ETH/BUSD', 'APE/BUSD','5m',100,3,avg_type_input='ema',stop_loss=-0.02, bol_band_type='ema',band_vol_exp=1.5,stoploss_break=False)
template.run_crypto(18,'DOGE/BUSD', 'ADA/BUSD','5m',100,3,avg_type_input='ema',stop_loss=-0.02, bol_band_type='ema',band_vol_exp=1.5,stoploss_break=False)
template.run_crypto(19, 'ARPA/USDT', 'PEOPLE/USDT', '5m', 85, 2.9,avg_type_input='ema',stop_loss=-0.01, bol_band_type='ema',band_vol_exp=1.75,stoploss_break=False)

#Kalman Pairs (portfolio of 20 pairs, 5 minute bars)
template.run_crypto(30,'VET/USDT', 'ZIL/USDT','5m',50,2.5, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)
template.run_crypto(31,'BLZ/USDT', 'DOGE/USDT','5m',50,2.5, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)
template.run_crypto(32,'FLM/USDT', 'FTM/USDT','5m',50,2.5, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)
template.run_crypto(33,'OGN/USDT', 'ONT/USDT','5m',50,2.5, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)
template.run_crypto(35,'ETH/BUSD', 'GALA/BUSD','5m',50,3,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input = 20)
template.run_crypto(36,'AUCTION/BUSD', 'UNI/BUSD','15m',50,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input = 20)
template.run_crypto(37,'STMX/USDT', 'DGB/USDT','5m',50,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input = 20)

template.run_crypto(38,'APE/BUSD', 'SOL/BUSD','5m',100,3,avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, bol_band_type='ema', order_val_input=20)
template.run_crypto(39,'VET/USDT', 'XEM/USDT','5m',100,2,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=20)
template.run_crypto(40,'WOO/USDT','OGN/USDT','5m',50,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=20)
template.run_crypto(42,'IOTA/USDT', 'SXP/USDT','15m',25,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)
template.run_crypto(43,'DOT/USDT', 'LINK/USDT','5m',50,3,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)

template.run_crypto(45,'OMG/USDT', 'BAND/USDT','5m',50,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)
template.run_crypto(46,'TLM/BUSD', 'ICP/BUSD','5m',100,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)

template.run_crypto(47,'1000LUNC/BUSD', 'GALA/BUSD','5m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)
template.run_crypto(48,'HNT/USDT', 'ICP/USDT','5m',50,2.50,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)
template.run_crypto(49,'RVN/USDT', 'ARPA/USDT','5m',50,2.50,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)
template.run_crypto(50,'BTC/USDT', '1000SHIB/USDT','5m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)


template.run_crypto(51,'ARPA/USDT', 'PEOPLE/USDT','5m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True)


#Kalman pairs (4 pairs, 15 minute bars)

template.run_crypto(46,'THETA/USDT', 'SAND/USDT','5m',35,3,avg_type_input='kalman',stop_loss=-0.03, stoploss_break=True)


template.run_crypto(100,'WOO/USDT','OGN/USDT','5m',50,2.25,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=20)
template.run_crypto(101,'WOO/USDT','OGN/USDT','5m',50,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=20)
template.run_crypto(102,'WOO/USDT','OGN/USDT','5m',100,2.25,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=20)
template.run_crypto(103,'WOO/USDT','OGN/USDT','5m',100,2.5,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=20)

template.run_crypto(104,'OGN/USDT', 'ONT/USDT','5m',50,2.25, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)
template.run_crypto(105,'OGN/USDT', 'ONT/USDT','5m',50,2.5, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)
template.run_crypto(106,'OGN/USDT', 'ONT/USDT','5m',100,2.25, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)
template.run_crypto(107,'OGN/USDT', 'ONT/USDT','5m',100,2.5, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=20)

template.run_crypto(44,'BTC/USDT', 'BTC/USDT','15m',200,17,stop_loss=-0.01, stoploss_break=True)


, 5m, 100, #2.25, 2.5, 2.75

template.run_crypto(108,'ARPA/USDT', 'TRX/USDT','5m',100,2.25, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=10)
template.run_crypto(109,'ARPA/USDT', 'TRX/USDT','5m',100,2.5, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=10)
template.run_crypto(110,'ARPA/USDT', 'TRX/USDT','5m',100,2.75, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=10)


template_threaded.run_crypto(121,'WOO/USDT','OGN/USDT','5m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=100)
template_threaded.run_crypto(122,'OGN/USDT', 'ONT/USDT','5m',50,2.75, avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=100)
template.run_crypto(123,'SFP/USDT', 'ONT/USDT','15m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=20)
template.run_crypto(125,'APE/BUSD', 'SOL/BUSD','5m',100,2.75,avg_type_input='kalman',stop_loss=-0.01,stoploss_break=True, order_val_input=50)
template.run_crypto(128,'DOGE/USDT', 'SOL/USDT','5m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input = 20)
template.run_crypto(129,'BTC/USDT', 'XRP/USDT','15m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input = 25)
template.run_crypto(130,'BNB/USDT', 'ETH/USDT','5m',75,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input = 25)
template.run_crypto(131,'LINK/USDT', 'NEAR/USDT','5m',125,3,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input = 25)

t.run_crypto(121,['BTC/USDT','ETH/USDT','BNB/USDT'],'5m',50,2.75,avg_type_input='kalman',stop_loss=-0.01, stoploss_break=True, order_val_input=100)
import math


def build_ohlcvc(trades, ohlcvc, timeframe=60000):
        ohlcvc_dict = ohlcvc
        num_trades = len(trades)
        oldest = (num_trades - 1)
        for i in range(0, oldest):
            trade = trades[i]

            # here I have to check if I'm dealing with the old trade
            # or if it's time to move to a new one
            original_opening_time = ohlcvc_dict['timestamp']
            if trade['timestamp']:
                opening_time = int(math.floor(trade['timestamp'] / timeframe) * timeframe)  # Shift the edge of the m/h/d (but not M)
  
            # if it's new timeframe we have to update
            # the info about previous candle and later
            # create new candle only if the time has come
            if opening_time and opening_time >= (original_opening_time + timeframe):
                ohlcvc_dict['timestamp'] = opening_time
                ohlcvc_dict['open'] = trade['price']
                ohlcvc_dict['high'] = trade['price']
                ohlcvc_dict['low'] = trade['price']
                ohlcvc_dict['close'] = trade['price']
                ohlcvc_dict['volume'] = trade['amount']
                ohlcvc_dict['count'] = 1
                ohlcvc_dict['ids'].clear()
                ohlcvc_dict['ids'].add(trade['id'])
            else:
                # still processing the same timeframe
                # bot only if this is not the same trade we saw previously
                if trade['id'] not in ohlcvc_dict['ids']:
                    ohlcvc_dict['ids'].add(trade['id'])
                    ohlcvc_dict['high'] = max(ohlcvc_dict['high'], trade['price'])
                    ohlcvc_dict['low'] = min(ohlcvc_dict['low'], trade['price'])
                    ohlcvc_dict['close'] = trade['price']
                    ohlcvc_dict['volume'] += trade['amount']
                    ohlcvc_dict['count'] += 1

        return ohlcvc_dict


def get_table_name(ticker, candle_duration):
    return f"{ticker.lower()}_{str(candle_duration)}"
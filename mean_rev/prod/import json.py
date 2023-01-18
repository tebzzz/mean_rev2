import json
import concurrent.futures

def read_arguments_from_file(file_name):
    with open(file_name, 'r') as json_arguments:
        arguments = json.load(json_arguments)  # read all the arguments from file (from config.json for example)
        return arguments

# send arguments to the function and start it multiple times using threads
def create_threads(arguments, function_name):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(arguments)) as executor:
        executor.map(lambda f: function_name(**f), arguments)

def run_crypto(pair_number1, ticker1, ticker2, bar_size, lookback_input,sigma,order_val_input=total_order_value, avg_type_input=average_type,stop_loss=max_loss,stoploss_break=break_on_loss, band_vol_exp=bollinger_vol_exp, bol_band_type=bollinger_type):
    data = {}
    
    data['pair_number'] = pair_number1 # pair_number = pair_number1
    data['duration'] = lookback_input*5 #duration_lookback = lookback_input*5
    data['bar_size'] = bar_size #bar_size = bar_size
    data['lookback'] = lookback_input #lookback = lookback_input
    data['entry_sigma '] = sigma #entry_sigma = sigma
    data['total_order_value'] = order_val_input #total_order_value = order_val_input
    data['average_type'] = avg_type_input #average_type = avg_type_input
    data['max_loss'] = stop_loss #max_loss = stop_loss
    data['break_on_loss'] = stoploss_break #break_on_loss = stoploss_break
    data['bollinger_vol_exp'] = band_vol_exp # bollinger_vol_exp = band_vol_exp
    data['bollinger_type'] = bol_band_type #bollinger_type = bol_band_type
    data['global_data_max_size'] = int(float(lookback)*2.5) #global_data_max_size = int(float(lookback)*2.5) #max size of global_data
    
    #run function goes here
    print('success')

if __name__ == "__main__":
    create_threads(read_arguments_from_file('config.json'), run_crypto)
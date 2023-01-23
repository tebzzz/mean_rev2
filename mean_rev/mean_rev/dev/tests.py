import numpy as np
import pandas as pd
import scipy
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss
from statsmodels.tsa.stattools import coint
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import math
import random
import sqlite3

from datetime import datetime, timedelta
from ib_insync import *


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
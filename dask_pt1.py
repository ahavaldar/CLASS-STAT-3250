#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DASK DATAFRAMES

Created on Sat Apr  2 15:05:25 2022

@author: taylor
"""
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask
import psutil
import sys
import os

# we'll play around with this later but for now just run it
dask.config.set(scheduler='threads')

# inspect your machine
psutil.cpu_count()
psutil.virtual_memory()[0] // (2.0**30)
# 2^3 bits = byte
# 2^10 bytes = 1KB
# 2^20 bytes = 1MB
# 2^30 bytes = 1GB

# very similar read in!
aa_p = pd.read_csv("AA.csv")
aa_d = dd.read_csv("AA.csv")

# take a gander
aa_p
aa_d

# compare sizes in memory
sys.getsizeof(aa_p) # in bytes
sys.getsizeof(aa_d) # in bytes

# lazy execution! 
result = aa_p['Close'].apply(np.log).diff().mean()
result
result = aa_d['Close'].apply(np.log).diff().mean()
result
result.compute()

# get rid of warnings
aa_d['Close'].apply(np.log, meta=('Close', 'float64')).diff().mean().compute()

## store many files in a single dask df
## even if collectively larger than available memory!
stocks_d = dd.read_csv("stocks/*.csv")
stocks_d

# or do something similar to a previous assignment
stocks_p = pd.DataFrame()
for csv_file in os.listdir('stocks/'):
    file_path = 'stocks/' + csv_file
    ticker = file_path[7:-4]
    tmp_df = pd.read_csv(file_path)
    tmp_df['ticker'] = ticker
    stocks_p = pd.concat([stocks_p,tmp_df], axis=0)

stocks_p

sys.getsizeof(stocks_d)
sys.getsizeof(stocks_p)

np.mean(stocks_d['High'] - stocks_d['Low']).compute()
np.mean(stocks_p['High'] - stocks_p['Low'])


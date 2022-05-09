import numpy as np
import dask.array as da
import dask
import sys
import matplotlib.pyplot as plt
import time


# we'll play around with this later but for now just run it
dask.config.set(scheduler='threads')

# compare sizes
sys.getsizeof(da.ones(100))
sys.getsizeof(np.ones(100))

# need .compute() at the end, just like with dask dataframes!
da.cumsum(da.random.normal(size=100)).compute()
np.cumsum(np.random.normal(size=100))


###################
# option pricing  #
###################

std_dev = 0.03
ave_day = .0001
num_days_into_future = 100
current_price = 100

returns = np.random.normal(loc = ave_day,scale=std_dev, 
                           size = num_days_into_future)
random_path = current_price * np.cumprod(1+returns)
plt.plot(random_path)

num_paths = 500000
#plt.plot(random_paths[0,:])
#plt.plot(random_paths[:,0])
returns = np.random.normal(loc=ave_day, scale = std_dev, 
                           size = (num_paths, num_days_into_future))
random_paths = (current_price * np.cumprod(1 + returns, axis = 1))

# if you own a call option, you have the 
# "call" options have intrinsic value if current_price > strike_price
# this is because you can buy a stock worth 110, say, for 100
# in other words, if the price is above the strike, you can immediately buy low and then sell high
# if the stock price is below the strike price, then you don't need to "exercise" your option
# and the option is intrinsically worthless
strike = 200
returns = np.random.normal(loc=ave_day, scale = std_dev, size = (num_paths, num_days_into_future))
random_paths = (current_price * np.cumprod(1 + returns, axis = 1))
best_case_scenarios = np.max(np.maximum(0, random_paths - strike), axis=1)
plt.hist(best_case_scenarios)

# time the numpy version
num_paths = 500000#0 # with one more zero, it crashes my spyder!
start = time.time()
returns = np.random.normal(loc=ave_day, scale = std_dev, 
                           size = (num_paths, num_days_into_future))
random_paths = (current_price * np.cumprod(1 + returns, axis = 1))
best_case_scenarios = np.max(np.maximum(0, random_paths - strike), axis=1)
print('finished in ', time.time() - start, " seconds")

# time the dask version
start = time.time()
returns = da.random.normal(loc=ave_day, scale = std_dev, 
                           size = (num_paths, num_days_into_future))
random_paths = (current_price * da.cumprod(1 + returns, axis = 1))
best_case_scenarios = da.max(da.maximum(0, random_paths - strike), axis=1)
best_case_scenarios.compute()
print('finished in ', time.time() - start, " seconds")
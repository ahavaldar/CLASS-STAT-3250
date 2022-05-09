##
## File: assignment11.py (STAT 3250)
## Topic: Assignment 11 
##

##  The file Stocks.zip is a zip file containing nearly 100 sets of price 
##  records for various stocks.  A sample of the type of files contained
##  in Stocks.zip is ABT.csv, which we have seen previously and is posted
##  in recent course materials. Each file includes daily data for a specific
##  stock, with stock ticker symbol given in the file name. Each line of
##  a file includes the following:
##
##   Date = date for recorded information
##   Open = opening stock price 
##   High = high stock price 
##   Low = low stock price 
##   Close = closing stock price 
##   Volume = number of shares traded
##   Adj Close = closing price adjusted for stock splits (ignored for this assignment)

##   The time interval covered varies from stock to stock. For many files
##   there are dates when the market was open but the data is not provided, so
##   those records are missing. Note that some dates are not present because the 
##   market is closed on weekends and holidays.  Those are not missing records.  

##  The Gradescope autograder will be evaluating your code on a subset 
##  of the set of files in the folder Stocks.  Your code needs to automatically 
##  handle all assignments to the variables q1, q2, ... to accommodate the 
##  reduced set, so do not copy/paste things from the console window, and
##  take care with hard-coding values. 

##  The autograder will contain a folder Stocks containing the stock data sets.
##  This folder will be in the working directory so your code should be written
##  assuming that is the case.


import pandas as pd # load pandas
import numpy as np # load numpy
import glob 
pd.set_option('display.max_columns', 10) # Display 10 columns in console


## 1.  Find the mean for the Open, High, Low, and Close entries for all 
##     records for all stocks.  Give your results as a Series with index
##     Open, High, Low, Close (in that order) and the corresponding means
##     as values.
filelist = glob.glob('Stocks/*.csv')            # list of files
df=pd.DataFrame()               # initialize empty df
for file in filelist:
    newdf = pd.read_csv(file)               # read each df
    df = pd.concat([df,newdf])              # concat to make one big df
cols = ['Open', 'High','Low', 'Close']      # specifying cols
q1ser = df[cols].mean()                     # getting mean


q1 = q1ser  # Series of means of Open, High, Low, and Close


## 2.  Find all stocks with an average Close price less than 30.  Give your
##     results as a Series with ticker symbol as index and average Close price. 
##     price as value.  Sort the Series from lowest to highest average Close
##     price.  (Note: 'MSFT' is the ticker symbol for Microsoft.  'MSFT.csv',
##     'Stocks/MSFT.csv' and 'MSFT ' are not ticker symbols.)
lst = []                # empty list
for file in filelist:
    newdf = pd.read_csv(file)       # reading in files
    tick = file[7:-4]               # storing the ticker name
    mean = newdf['Close'].mean()        # storing the close mean
    lst.append([tick,mean])             # appending to list
df2 = pd.DataFrame(lst, columns=['ticker', 'mean'])         # creating DF with list values

q2ser = df2.set_index('ticker').squeeze().sort_values(ascending=True)       # converting to series and sorting
q2ser = q2ser[q2ser<30]             # values <30

q2 =  q2ser  # Series of stocks with average close less than 30


## 3.  Find the top-10 stocks in terms of the day-to-day volatility of the
##     price, which we define to be the mean of the daily differences 
##     High - Low for each stock. Give your results as a Series with the
##     ticker symbol as index and average day-to-day volatility as value. 
##     Sort the Series from highest to lowest average volatility.

lst2 = []                   # intialize list
for file in filelist:
    newdf = pd.read_csv(file)                   # read in file
    vol = (newdf['High']-newdf['Low'])          # volatility for each day
    vol2 = np.mean(vol)                         # mean volatilty across all days
    tick = file[7:-4]                           # ticker
    lst2.append([tick,vol2])                    # appending ticker and volatility to list
df3 = pd.DataFrame(lst2, columns=['ticker', 'volatility'])          # convert to df
q3ser = df3.set_index('ticker').squeeze().sort_values(ascending=False).nlargest(10, keep='all')     # convert to series and sort


q3 = q3ser  # Series of top-10 mean volatility


## 4.  Repeat the previous problem, this time using the relative volatility, 
##     which we define to be the mean of
## 
##                       (High âˆ’ Low)/(0.5(Open + Close))
##
##     for each day. Provide your results as a Series with the same specifications
##     as in the previous problem.

lst3 = []                   # intialize list
for file in filelist:
    newdf = pd.read_csv(file)                   # read in file
    vol = (newdf['High']-newdf['Low'])/(0.5*(newdf['Open'] + newdf['Close']))          # volatility for each day
    vol2 = np.mean(vol)                         # mean volatilty across all days
    tick = file[7:-4]                           # ticker
    lst3.append([tick,vol2])                    # appending ticker and volatility to list
df4 = pd.DataFrame(lst3, columns=['ticker', 'volatility'])          # convert to df
q4ser = df4.set_index('ticker').squeeze().sort_values(ascending=False).nlargest(10, keep='all')     # convert to series and sort


q4 = q4ser  # Series of top-10 mean relative volatility


## 5.  For each day the market was open in October 2008, find the average 
##     daily Open, High, Low, Close, and Volume for all stocks that have
##     records for October 2008.  (Note: The market is open on a given
##     date if there is a record for that date in any of the files.)
##     Give your results as a DataFrame with dates as index and columns of
##     means Open, High, Low, Close, Volume (in that order).  The dates should 
##     be sorted from oldest to most recent, with dates formatted (for example)
##     2008-10-01, the same form as in the files.   
df5=pd.DataFrame()               # initialize empty df
for file in filelist:
    newdf = pd.read_csv(file)               # read each df
    df5 = pd.concat([df5,newdf])
df5 = df5[df5['Date'].str.contains('2008-10')]          # subsetting 10/2008
q5gb = df5[['Open', 'High', 'Low', 'Close', 'Volume']].groupby(df5['Date'])  # getting variables
q5mean = q5gb.mean()            # calculating mean

q5 = q5mean  # DataFrame of means for each open day of Oct '08.


## 6. For 2011, find the date with the maximum average relative volatility 
##    for all stocks and the date with the minimum average relative 
##    volatility for all stocks. Give your results as a Series with 
##    the dates as index and corresponding average relative volatility
##    as values, with the maximum first and the minimum second.
df6 = pd.DataFrame()
for file in filelist:
    newdf= pd.read_csv(file)
    df6 = pd.concat([df,newdf])

df6['vola'] = (df6['High']-df6['Low'])/(0.5*(df6['Open'] + df6['Close']))
df6 = df6[df6['Date'].str.contains('2011')]
q6gb = df6['vola'].groupby(df6['Date']).mean().sort_values(ascending=False)
q6ser = q6gb.iloc[[0,-1]]


q6 = q6ser  # Series of average relative volatilities


## 7. For 2010-2012, find the average relative volatility for all stocks on
##    Monday, Tuesday, ..., Friday.  Give your results as a Series with index
##    'Mon','Tue','Wed','Thu','Fri' (in that order) and corresponding
##    average relative volatility as values. 
df7 = pd.DataFrame()
for file in filelist:
    newdf= pd.read_csv(file)
    df7 = pd.concat([df,newdf])                         # appending dfs
df7 = df7[df7['Date'].str.contains('2010|2011|2012')]       # filtering out the correct years
df7['vola'] = (df7['High']-df7['Low'])/(0.5*(df7['Open'] + df7['Close']))   # calculating volatility
df7['Date'] = pd.to_datetime(df7['Date'])       # converting to datetime
df7['dotw'] = df7['Date'].dt.dayofweek          # getting day of the week

q7gb = df7['vola'].groupby(df7['dotw']).mean()          # group vola mean by dotw
index = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']     
q7gb.index = index                                            # setting index

q7 = q7gb  # Series of average relative volatility by day of week


## 8.  For each month of 2009, determine which stock had the maximum average 
##     relative volatility. Give your results as a Series with MultiIndex
##     that includes the month (month number is fine) and corresponding stock 
##     ticker symbol (in that order), and the average relative volatility
##     as values.  Sort the Series by month number 1, 2, ..., 12.
df8 = pd.DataFrame()                # empty list
for file in filelist:
    newdf = pd.read_csv(file)                                           # reading in files
    newdf = newdf[newdf['Date'].str.contains('2009')]
    newdf['Date'] = pd.to_datetime(newdf['Date'])
    newdf['Month'] = newdf['Date'].dt.month
    newdf['vola'] = (newdf['High']-newdf['Low'])/(0.5*(newdf['Open'] + newdf['Close']))        # storing the close mean
    newgb = newdf['vola'].groupby(newdf['Month'])
    newmean = newgb.mean()
    tempdf = pd.DataFrame(newmean)
    tempdf = tempdf.reset_index()
    tempdf['ticker'] = file[7:-4]                                                         # storing the ticker name
    df8 = pd.concat([df8, tempdf], axis=0)
q8gb = df8['vola'].groupby([df8['Month'], df8['ticker']])
q8mean = q8gb.mean()
q8ans = q8mean.groupby('Month', group_keys=False).nlargest(1).sort_index(ascending=True)        # getting largest value of rate for each airline

q8 = q8ans  # Series of maximum relative volatilities by month


## 9.  The â€œPython Indexâ€ is designed to capture the collective movement of 
##     all of our stocks. For each date, this is defined as the average price 
##     for all stocks for which we have data on that day, weighted by the 
##     volume of shares traded for each stock.  That is, for stock values 
##     S_1, S_2, ... with corresponding volumes V_1, V_2, ..., the average
##     weighted volume is
##
##           (S_1*V_1 + S_2*V_2 + ...)/(V_1 + V_2 + ...)
##
##     Find the Open, High, Low, and Close for the Python Index for each date
##     the market was open in January 2013. 
##     Give your results as a DataFrame with dates as index and columns of
##     means Open, High, Low, Close (in that order).  The dates should 
##     be sorted from oldest to most recent, with dates formatted (for example)
##     2013-01-31, the same form as in the files.  
 
df9=pd.DataFrame()               # initialize empty df
for file in filelist:
    newdf = pd.read_csv(file)               # read each df
    df9 = pd.concat([df9,newdf])
df9 = df9[df9['Date'].str.contains('2013-01')]      # subsetting by year

temp1 = df9[['Open', 'High', 'Low', 'Close']].multiply(df9['Volume'], axis='index')     # multiply by volume
temp1['Date'] = df9['Date']
temp1['Volume'] = df9['Volume']
temp2 = temp1[['Open', 'High', 'Low', 'Close', 'Volume']].groupby(temp1['Date']).sum()      # grouping by date and summing
temp3 = temp2[['Open', 'High', 'Low', 'Close']].divide(temp2['Volume'], axis='index')           # dividing by volume to get python index


q9 = temp3  # DataFrame of Python Index values for each open day of Jan 2013. 


## 10. For the years 2007-2012 determine the top-8 month-year pairs in terms 
##     of average relative volatility of the Python Index. Give your results
##     as a Series with MultiIndex that includes the month (month number is 
##     fine) and year (in that order), and the average relative volatility
##     as values.  Sort the Series by average relative volatility from
##     largest to smallest.
df10=pd.DataFrame()               # initialize empty df
for file in filelist:
    newdf = pd.read_csv(file)               # read each df
    df10 = pd.concat([df10, newdf])
df10 = df10[df10['Date'].str.contains('2007|2008|2009|2010|2011|2012')]             # subsetting out the years
df10['Date'] = pd.to_datetime(df10['Date'])         # converting to datetime

        
temp5 = df10[['Open', 'High', 'Low', 'Close']].multiply(df10['Volume'], axis='index')     # multiply by volume
temp5['Date'] = df10['Date']
temp5['Volume'] = df10['Volume']
temp6 = temp5[['Open', 'High', 'Low', 'Close', 'Volume']].groupby(temp5['Date']).sum()      # grouping by date and summing
temp7 = temp6[['Open', 'High', 'Low', 'Close']].divide(temp6['Volume'], axis='index')           # dividing by volume to get python index
temp7 = temp7.reset_index()
temp7['month'] = temp7['Date'].dt.month               # getting the month
temp7['year'] = temp7['Date'].dt.year                 # getting the year
temp7['avg']= (temp7['High']-temp7['Low'])/(0.5*(temp7['Open'] + temp7['Close']))       # calculating average relative volatility
temp7gb = temp7['avg'].groupby([temp7['month'], temp7['year']]).mean().sort_values(ascending=False).iloc[0:8]              # getting mean of relative volatility  for each year-month pair and sorting


q10 = temp7gb  # Series of month-year pairs and average rel. volatilities


## 11. Each stock in the data set contains records starting at some date and 
##     ending at another date.  In between the start and end dates there may be 
##     dates when the market was open but there is no record -- these are the
##     missing records for the stock.  For each stock, determine the percentage
##     of records that are missing out of the total records that would be
##     present if no records were missing. Give a Series of those stocks
##     with less than 1.3% of records missing, with the stock ticker as index 
##     and the corresponding percentage as values, sorted from lowest to 
##     highest percentage.
df11 = pd.DataFrame()
for file in filelist:
    newdf = pd.read_csv(file)               # read each df
    newdf['ticker'] = file[7:-4]  
    df11 = pd.concat([df11,newdf]) 

lst11 = []                  # empty list
for file in filelist:
    newdf = pd.read_csv(file)
    tick = file[7:-4]
    mx = max(newdf['Date'])                 # getting max date
    mn = min(newdf['Date'])                 # getting min date
    temp9 = df11[(df11['Date'] >= mn) & (df11['Date'] <= mx)]     # subset overall df on min and max of dates
    tot = len(df11[df11['ticker']==tick])       
    uni = len(temp9['Date'].unique())
    perc = ((uni/tot) * 100) - 100                  # calculating percentage
    lst11.append([tick, perc])                      # appending to list
    
df12 = pd.DataFrame(lst11, columns=['ticker', 'perc'])          # convert to df
q11ser = df12.set_index('ticker').squeeze().sort_values(ascending=True)     # convert to series and sort
q11ser = q11ser[q11ser < 1.3]

q11 = q11ser  # Series of stocks and percent missing




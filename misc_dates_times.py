##
## Topic: Misc + Dates and Times
##

#### A few misc items of use in future assignments

import pandas as pd  

## The 'sum' function

# One use of 'sum' is to combine a bunch of lists into a single
# combined list of all elements

listoflists = [['A','B'], ['B', 'C', 'D', 'E'], ['F', 'E', 'G']]
listoflists  # this is a list of lists, for demo purposes

sum(listoflists, []) # this creates one big list, with nothing removed.

# The same approach will work on a series of lists
seriesoflists = pd.Series(listoflists)
seriesoflists

# Here's the sum
sum(seriesoflists,[])

## Reminder on changing data types

# When reading in lines of text data, everything comes in as a string,
# including the numbers.  Below is a quick reminder of how to convert 
# them into numerical values

stringlist = ['Name A; Red; 28', 'Name B; Blue; 74', 'Name C; Green; 55']
seriesofstrings = pd.Series(stringlist)  # a series of strings

# Now let's extract the digits from each string:
nums = seriesofstrings.str.split(';').str[2] # split strings, extract 3rd element

# If we try to compute the mean, we get an error
nums.mean()

# Below we convert them to type 'int' (integers)
nums = nums.astype(int)
nums
nums.mean()  # now the mean works

# We could also use type 'float'
nums = nums.astype(float)
nums
nums.mean()  # the mean still works

## Masking for items matching 'False'

# There are sometimes cases where we want to extract values from a 
# series or dataframe when a condition is *not* met -- that is, when
# something is false.  A reminder that '~' will work in that situation.

testseries = pd.Series([27, 3, 9, 17, 3])  # A test set
testdata = pd.Series(['Dog', 'Cat', 'Mouse', 'Rabbit', 'Turtle'])

# Here's how we can extract the pet types corresponding to values of 3 in
# 'testseries':
testdata[testseries == 3]

# Suppose we want all the other pet types?  It's easiest to negate the test
# 'testseries == 3' with a '~' than to figure out how to negate it.
testdata[~(testseries == 3)]  # don't forget the () around the test!


#### Dates and Times

# pandas provides a function that allows for parsing dates and times.  The 
# argument 'unit' specifies the time units -- here it is seconds.  The default
# is UTC.
ts = pd.to_datetime(1481250949, unit='s') # this is seconds since Jan 1, 1970
print(ts)

# We can get a different format using 'strftime'
format = "%a %b %d %H:%M:%S %Y"
print(ts.strftime(format))

# Or just put the format in strftime
print(ts.strftime("%B %d %Y"))

# We can extract parts of the datetime object by specifying
# the part directly or through 'strftime'
ts
ts.month  # month number
ts.hour   # hour 
ts.strftime('%Y') # full year
ts.strftime('%b') # Abbreviated month

# Web site with formating codes:
# https://pandas.pydata.org/docs/reference/api/pandas.Period.strftime.html

## Series of timestamps

# Suppose that we have a Series of timestamps to process.

timestamps = pd.Series([881250949,891717742,878887116,880606923,886397596])
timestamps

# The pandas 'to_datetime' will take a Series as input and produce a Series
# of numpy datetime objects as output.
dts = pd.to_datetime(timestamps, unit='s')
print(dts)


#### More on Dates

# Here we use data from the file 'AA.csv' that contains stock prices from 
# a large airline.

stocks = pd.read_csv('AA.csv') # File of stock prices
stocks

# Here's the data types.
stocks.dtypes # Default data types for the columns
stocks.loc[0,'Date'] - stocks.loc[1,'Date'] # Not a date!

# Here's one way to convert the column so that Pandas recognizes
# the entries as dates.
stocks['Date'] = pd.to_datetime(stocks['Date']) # Convert to dates
stocks.dtypes

# We can identify the month, year, and day of the week:
stocks['Date'].dt.month
stocks['Date'].dt.year  # file dates in reverse chronological order
stocks['Date'].dt.dayofweek # 0 = monday, ... 6 = sunday

# It's possible to use inequalities on dates
stocks['Date'] < '2014-12-10'
(stocks['Date'] < '2014-12-31') & (stocks['Date'] > '2014-12-28')

# The above combines well with masking
stocks[stocks['Date'].dt.year == 2012]
stocks[(stocks['Date'] < '2014-12-31') & (stocks['Date'] > '2014-12-28')]
stocks.loc[stocks['Date'].dt.dayofweek == 0, 'Open']

# And we can do arithmetic
a = stocks.loc[0,'Date'] - stocks.loc[7,'Date']
a
b = stocks.loc[0,'Date'] - stocks.loc[5,'Date']
b

# We can do arithmetic with the 'Timedelta's
a/b 
(a-b)
(a+b)*4
a.days  # This extracts the value without the Timedelta

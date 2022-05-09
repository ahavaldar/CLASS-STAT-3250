##
## Topic: Brief Introduction to Graphics -- Part 1
##

import pandas as pd

## Line plots (common in time series data)

# Read in stock files 'AA.csv'
AA = pd.read_csv('AA.csv')
AA = AA.loc[::-1] # Reverse order so time goes forward
print(AA['Date'])

AA['Open'].plot() # Very basic plot, x-axis uses index by default
AA.plot(x='Date',y='Open') # Put 'Date' on x-axis

AA.plot(x='Date',y=['Open','Close']) # Two sets of values

# Let's compare with the 'MS' prices

MS = pd.read_csv('MS.csv').loc[::-1] # Reverse to fix date order
MS.plot(x='Date',y='Open') # 'Date' again on x-axis

# We can put the AA and MS plots on the same axes by merging
# them into a dataframe on the 'Date' variable.
# (how='outer' uses all dates from both AA and MS)
df = pd.merge(AA,MS, on='Date', how='outer')
print(df)

df.plot(x='Date',y=['Open_x','Open_y'])

# Merge doesn't preserve the order of 'Date' so we fix that
df = df.sort_values(by='Date',ascending=True)

# Rename the 'Open' columns to specify stocks
df = df.rename(columns={'Open_x': 'AAOpen', 'Open_y': 'MSOpen'})

# And the plot
df.plot(x='Date',y=['AAOpen','MSOpen']) 

# There are lots of possible modifications that can be applied.
df.plot(x='Date',
        y=['AAOpen','MSOpen'],
        legend=False) # This gets rid of the legend
        
df.plot(x='Date',
        y=['AAOpen','MSOpen'],
        style=['g','r']) # This sets the plot colors to green, red
        
df.plot(x='Date',
        subplots=True,  # This separates the two plots
        y=['AAOpen','MSOpen'],
        style=['g','r']) 
       
## Histograms

# Import the 'grades' data
grades = pd.read_csv('samplegrades.csv')

# An unhelpful plot
grades.plot(y='CourseAve')

# A histogram is more interesting
grades.plot(y='CourseAve', kind='hist')
grades.plot.hist(y='CourseAve')

# Several histograms together
grades.plot.hist(y=['Math','Read','Write']) # Not great

grades.plot.hist(y=['Math','Read','Write'], subplots=True) # Better!










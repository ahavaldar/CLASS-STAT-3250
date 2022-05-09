##
## File: week06b.py (STAT 3250)
## Topic: Misc. Commands; Merging data frames; Warnings
## 

import numpy as np # load numpy as np
import pandas as pd # load pandas as pd

#### Extracting a substring; changing the data type

s = pd.Series(['A345EF076T89  24Mar1988',
              'X76TYS54ZL   08Jul2013',
              'TY76007GHEQ  13Dec2009',
              'HJ119TKJU8    02May1998',
              'GGTE45T1038  21Oct1974'])

s.str.split()  # Split each list item on spaces

# Suppose we just want the year?  Splitting on spaces does not work, but
# we can count back characters from the end of each string.
years = s.str[-4:] # years always in last 4 positions 
years

# Here is the mean of the years
np.mean(years)  #That does not look right

# The entries in years are strings, we need them to be floating point
# or integers to get the mean we want
years = years.astype(int)
np.mean(years)

#### Concatenating lists

# Start by creating a Series of lists
d = s.str.split()
d

# The "sum" command will apply "+" to the lists, which concatenates them.
sum(d,[])

# alternatively
d.sum() # returns a list
d.explode() # returns a Series

#### Renumbering the index on a dataframe

sub = s[[0,1,3,4]] # extract a subSeries
sub

# We can reset the index to 0, 1, 2,...
sub.index = range(len(sub)) 
sub

#### Identifying digits

testseries = pd.Series(['a345','752','xyz','23.5']) # define test strings
testseries.str.isdigit() # determine which are digits.

#### Merging data sets

# Let's start with two simple data frames, to use as an illustration.
df1 = pd.DataFrame({'student':['Jim','Jane','Bob','Ann'],
                    'major':['English','Math','Math','CompSci']})
df1

df2 = pd.DataFrame({'student':['Ann','Bob','Jim','Jane'],
                    'year':[2,3,4,3]})
df2

# Both data frames have a column 'student' with the same student's
# names in each.  The function 'pd.merge' use the common column
# and creates a new data frame that combines df1 and df2.
df3 = pd.merge(df1,df2, on='student')
df3

# Now suppose we have another data frame 'df4' that includes the
# name of majors and the building.
df4 = pd.DataFrame({'building':['Bryan','Kerchof','Rice'],
                    'major':['English','Math','CompSci']})
df4

# If we apply 'pd.merge' to df3 and df4, the two data frames
# are merged together based on the common column 'major'.
df5 = pd.merge(df3,df4, on='major')
df5

# Let's add one more data frame, this one with the student's
# favorite classes.
df6 = pd.DataFrame({'student':['Ann','Bob','Bob','Jim','Jane','Jane','Jane'],
                    'fav':['STAT 3250','MATH 4140','MATH 3350','SOC 2001',
                           'MATH 4140','MATH 3100','MATH 3310']})
df6

# Here's what we get if we merge df5 and df6:
df7 = pd.merge(df5,df6, on='student')
df7

# One more data frame:
df8 = pd.DataFrame({'school':['College','College','College','College','SEAS'],
                    'major':['English','Math','Math','CompSci','CompSci']})
df8

# There is a duplicate row in df8.  We can remove this using 
# 'drop_duplicates()'
df8 = df8.drop_duplicates() 
df8

# Here's what we get when we merge df7 and df8:
df9 = pd.merge(df7,df8, on='major')
df9

# This new data frame might not be what we want -- Anne's school
# of enrollment is not clear.  The moral: Be careful with merging, and
# make sure it's doing what you want! (Test on small data sets.)

#### Subsets of data frames and warnings

# define our customary practice data frame
df = pd.DataFrame({'C1':['a','a','b','b','c','c','a','c'],
                   'C2':['a','y','b','x','x','b','x','a'],
                   'C3':[1,3,-2,-4,5,7,0,2],
                   'C4':[8,0,5,-3,4,1,3,1],
                   'C5':[3,-1,0,-2,4,3,8,0]})
df

subdf = df[df['C1'] == 'a']  # subset of rows with 'a' in column 'C1'
subdf

subdf['C6'] = 20  # create new column 'C6' in subdf, assign 20; get warning
subdf  # changes as expected
df   # might change or might not -- that's why we get the warning

# new version of data frame
df1 = pd.DataFrame({'C1':['a','a','b','b','c','c','a','c'],
                   'C2':['a','y','b','x','x','b','x','a'],
                   'C3':[1,3,-2,-4,5,7,0,2],
                   'C4':[8,0,5,-3,4,1,3,1],
                   'C5':[3,-1,0,-2,4,3,8,0]})
df1

subdf1 = df1[df1['C1'] == 'a'].copy() # this forces a copy to be made when subsetting

subdf1['C6'] = 20  # no warning this time
subdf1  # changes
df1  # no changes

subdf1['C4'][1] = 100   # warning again, because of "chaining"; subdf1['C4']  
                        # is a subset of df1

# use .loc instead, then there is no subset
subdf1.loc[1,'C4'] = 100
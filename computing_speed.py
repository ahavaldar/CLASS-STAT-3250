##
## Topic: Computational speeds
## 

import numpy as np # load numpy as np
import pandas as pd # load pandas as pd
import time  # load time to measure how quickly code runs


#### Comparing computational speeds

# Below we look at various ways to add data to a data frame.  We start with
# defining a sample frame.  The initial data for two columns.
x = np.random.normal(size=100000) # random standard normal values
y = np.zeros(100000)

# Define a dictionary and then the corresponding data frame
di = {'C1':x,
      'C2':y}
df = pd.DataFrame(di)

# Suppose that, for each row of df, we want to set the entry in C2 based on 
# the entry in C1 as follows: If the C1 entry is less than -1, then we set the
# C2 entry to 2; if the C1 entry is greater than 1, then we set the C2 entry
# to -3; otherwise, we set the C2 entry to 1.

# We could do this with a simple loop and if/elif/else statement

t0 = time.time() # time when code starts
for i in range(100000):
    if df.loc[i,'C1'] < -1: # When entry is < -1, enter 2
        df.loc[i,'C2'] = 2
    elif df.loc[i,'C1'] > 1: # When entry is > 1, enter -3
        df.loc[i,'C2'] = -3
    else:                   # When entry between -1 and 1, enter 1
        df.loc[i,'C2'] = 1
t1 = time.time() # time when code stops
print(df.mean()) # Mean of columns of df, for comparison
print(t1-t0) # time to run code (seconds)

# There are several ways to do this that include a function to convert the
# C1 entry into the corresponding C2 entry.  The function:

def myfun1(x):
    if x < -1:
        return(2)  # When entry is < -1, return 2
    elif x > 1:
        return(-3) # When entry is > 1, return -3
    else:
        return(1) # When entry between -1 and 1, return 1

# Now let's use the function to place entries into C2 one at a time.

df['C2'] = np.zeros(100000)  # reset the column C2 to 0's for consistency

t0 = time.time() # time when code starts
for i in range(100000):
    df.loc[i,'C2'] = myfun1(df.loc[i,'C1']) # compute new value, plug into C2
t1 = time.time() # time when code stops
print(df.mean()) # Mean of columns of df, for comparison
print(t1-t0) # time to run code (seconds)

# The above code runs somewhat slowly.  A significant reason seems to be because
# of writing the individual values to the data frame using df.loc.  

# For the next computational trial, we set up a temporary numpy array to
# hold values, then transfer them all to C2 at the same time

df['C2'] = np.zeros(100000)  # reset the column C2 to 0's for consistency

t0 = time.time()
temp1 = np.zeros(100000) # temporary array for eventual C2 values
for i in range(100000):
    temp1[i] = myfun1(df.loc[i,'C1']) # Plug C1 entry into myfun1, then to temp1
df['C2'] = temp1 # transfer temp1 values to C2 column
t1 = time.time()
print(df.mean()) # Mean of columns of df, for comparison
print(t1-t0) # time to run code (seconds)

# Would it help if the values from C1 were also in an array?  Let's try it.

df['C2'] = np.zeros(100000)  # reset the column C2 to 0's for consistency

t0 = time.time()
temp1 = np.zeros(100000) # temporary array for eventual C2 values
temp2 = np.array(df['C1']) # copy C1 column into an array
for i in range(100000):
    temp1[i] = myfun1(temp2[i]) # Plug temp2 entry into myfun1, then to temp1
df['C2'] = temp1 # transfer temp1 values to C2 column
t1 = time.time()
print(df.mean()) # Mean of columns of df, for comparison
print(t1-t0) # time to run code (seconds)

# The moral: If you're going to use a loop, it's faster to use arrays to 
# transfer individual values, then use vectorization to copy them into
# the data frame.

# Next: What about 'apply'?  It turns out to be somewhat faster than any of
# the above approaches.  (The speed improvement of apply depends on the 
# application.)

df['C2'] = np.zeros(100000)  # reset the column C2 to 0's for consistency

t0 = time.time()
df['C2'] = df['C1'].apply(myfun1) 
t1 = time.time()
print(df.mean()) # Mean of columns of df, for comparison
print(t1-t0) # time to run code (seconds)

# Although apply is pretty fast, the fastest approach is still vectorization
# whenever it is possible.  (Pandas dataframe columns are designed for this
# type of computation.)  Below we eliminate the need for the function myfun1
# and the use of apply.

df['C2'] = np.zeros(100000)  # reset the column C2 to 0's for consistency

t0 = time.time()
df['C2'] = np.ones(100000) # initialize C2 to be all 1's
df.loc[df['C1'] < -1,'C2'] = 2 # set C2 = 2 for each row with C1 < -1
df.loc[df['C1'] > 1,'C2'] = -3 # set C2 = -3 for each row with C1 > 1
t1 = time.time()
print(df.mean()) # Mean of columns of df, for comparison
print(t1-t0) # time to run code (seconds)

# This takes roughly 1/5000 as long as the original loop.
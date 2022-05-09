##
## Topic: Functions and Apply
##

#### Defining Functions

import pandas as pd # load pandas as pd
import numpy as np # load numpy as np
   
def myabs(x):   # Define a function named 'myabs'
    if x < 0:       
        return(-x)  # Return specifies the value "returned" to the call
    else:
        return(x)

myabs(5.3)  # Plug 5.3 into 'myabs'
myabs(-7)   # Plug -7 into 'myabs'
y = myabs(-4) # Set y = 4
y
sizes = [12, 16, 12, 20, 18, 20]
myabs(sizes) # Doesn't work with lists as defined.

# Functions can take different types of objects
# as input.
def mycountval(val,inputlist):  # input a value 'val' and list 'inputlist'
    ct = 0   # set counter to 0
    for x in inputlist:  # Go through the list one entry at a time
        if x == val:
            ct += 1  # increment counter if x equals val
    return(ct)   # return counter when loop is complete.

v = [3,4,2,-1,0,2,5,6,2,0,3,2,1]
z = mycountval(2, v)
z
ct  # Variables inside function stay inside; not 'visible' outside
    
# Functions can return arrays and other things
def myarraymult(n, inputarray):  # input an integer 'n' and array 'inputarray'
    newarray = np.array(n*list(inputarray))  # n copies of input array
    return(newarray)
    
myarraymult(3,np.array([8,9,10])) # 3 copies of array[8,9,10]

# A sample data frame
df = pd.DataFrame({'C1':['a','a','b','b','c','c','a','c'],
                   'C2':['a','y','b','x','x','b','x','a'],
                   'C3':[1,3,-2,-4,5,7,0,2],
                   'C4':[8,0,5,-3,4,1,3,1],
                   'C5':[3,-1,0,-2,4,3,8,0]})
df

df['C6'] = "" # add empty column 
df

# We can use a loop and 'myabs' to populate column C6 with the absolute
# value of the entries in C5
for i in range(len(df)):
    df.loc[i,'C6'] = myabs(df.loc[i,'C5']) # use .loc!!
df

#### Apply and ApplyMap

# Let's start with a sample function

def mysgn(x): # Returns the "sign" of x
    if x > 0:
        return(1) # 1 when x positive
    elif x < 0:
        return(-1) # -1 when x negative
    else:
        return(0) # 0 when x is 0

mysgn(-3) # Test values
mysgn(0) 
mysgn(2) 
 
# 'apply' works on a data frame column, and will apply the 
# called function to each column entry.  

df['C5'].apply(mysgn)  # data column on the left, function name in parens

# We can add a new column of populated values to the data frame
# without using an explicit loop.
df['C7'] = df['C5'].apply(mysgn)
df

# 'apply' won't work on more than one column
df[['C4','C3']].apply(mysgn)

# But a variant 'applymap' will work on multiple columns
# of a DataFrame
df[['C4','C3']].applymap(mysgn)

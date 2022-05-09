##
## Topic: Writing data to files
##

import pandas as pd # load pandas as pd

#### Writing data to a file: Method 1

# We need some data, let's read in the transactions.txt file
lines = open('transactions.txt').read().splitlines()

# Sometimes we want to write data out to a file.
# We start by opening a path to the file.
fh = open('newfile1.txt', 'w') # 'w' = 'write'; 'fh' stands for file handle

# Use 'write' to write out to the file.
fh.write("This is my new file.\n")

# We can add our date strings to the file.
for oneline in lines:
    if oneline.count(' 2014 ') > 0:
        sub = oneline.split('2014')[0]+'2014\n'
        fh.write(sub)

# We have to close the file before viewing it.
fh.close()


#### Writing data to a file: Method 2

# We start by opening a file with file handle 'f':
fh = open('newfile2.txt', 'w') # 'w' = 'write'

# If we add 'file=fh' as an argument to the 'print' function
# then the output that would print to the console is instead
# written to the file. 

for oneline in lines:
    if oneline.count(' 2014 ') > 0:
        sub = oneline.split('2014')[0]+'2014' # Note: No '\n'
        print(sub, file=fh)  # This method will automatically break lines
 
# We have to close the file before viewing it.
fh.close()


#### Writing a pandas DataFrame to a file

# We need a dataframe.  Here's one from a few weeks ago.
df = pd.DataFrame({'C1':['a','a','b','b','c','c','a','c'],
                   'C2':['x','y','x','x','x','y','x','y'],
                   'C3':[1,3,-2,-4,5,7,0,2],
                   'C4':[8,0,5,-3,4,1,3,1],
                   'C5':[3,-1,0,-2,4,3,8,0]})
df

# Write the DataFrame to a file using 'to_csv' (even if not CSV)
df.to_csv('newfile3.txt', header=None, index=None, sep=' ')
df.to_csv('newfile3.txt', sep=',')  # Previous output overwritten
df.to_csv('newfile3.txt', sep=';', mode='a')  # mode = 'a' gives append to file
df.to_csv('newfile3.txt', sep=';', mode='a', index=None)  # drop index


#### Writing a pandas Series to a file

# Start by defining a Series:
ser = pd.Series(df['C3'].values, index=df['C1'])
ser

# We can use 'to_csv' to write a Series to a file.
ser.to_csv('newfile4.txt')

# The same options for 'to_csv' apply for a Series.

#### One more string vectorization

testSeries = pd.Series(['Test', ' Test', ' Test ', '   Test Test '])
print(testSeries)
testSeries[0] == testSeries[1]
testSeries[3] == testSeries[2]
newtestSeries = testSeries.str.strip()
print(newtestSeries)
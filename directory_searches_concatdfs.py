##
## Topic: Directory Searches and Concatenating Data Frames
##

#### Before We Start

# 1) In your computer's folder for STAT 3250, create a new folder stocks".
# 2) Place in the stocks folder the files 'AA.csv', 'HSIC.csv', and 'WFM.csv'
# 3) Set your working directory to your computer's folder for STAT 3250.

#### Concatenating Data Frames

import pandas as pd

# Define df1 and df2
df1 = pd.DataFrame({'J': [3,5,6,7],
                    'K': [8,3,2,5],
                    'L': [6,8,6,3]})
df1
df2 = pd.DataFrame({'J': [3,4,6,7,8],
                    'K': [4,7,0,11,3],
                    'L': [5,5,6,2,8]})
df2

# Concatenate df1 and df2 by rows
newdf1 = pd.concat([df1,df2])
newdf1

newdf1.index = range(len(newdf1))
newdf1

# We can concatenate by columns, but we get odd results
newdf2 = pd.concat([df1,df2], axis=1)
newdf2
newdf2['J']

#### Searching directories

import glob # 'glob' searches for files

# Reminder: Change to directory containing the folder 'stocks'

# '*.csv' selects files ending in '.csv'
filelist = glob.glob('Stocks/*.csv') # 'glob.glob' is the directory search
filelist

# The above list allows us to iterate through the files to read
# them in, one at a time.  This is just to see what we are getting
# for input.
for file in filelist:
    df = pd.read_csv(file)  # read in file as dataframe
    print(file,"; Length =",len(df),"\n")  # print file name and length

# We can concatenate the dataframes into one large dataframe
df = pd.DataFrame()  # empty dataframe
for file in filelist:
    newdf = pd.read_csv(file)  # read in the file
    df = pd.concat([df,newdf])  # concatenate to existing dataframe
print(len(df)) # print length of combined dataframe
print(df)  # the combined dataframe; note that file name not included





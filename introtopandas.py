##
## Topic: Introduction to Pandas and DataFrames
## 

import numpy as np # load "numpy"
import pandas as pd # load pandas as pd

## Creating a DataFrame for demonstration

data = {'Course':['STAT 3A','STAT 3A','STAT 3A','STAT 3B','STAT 3B','STAT 4A','STAT 4A'],
        'Time':['MW200','TR930','TR330','MW1000','MWF900','TR330','MWF100'],
        'Sect':[1,2,4,1,2,1,3],
        'Enrolled':[41,46,39,78,84,51,33],
        'Capacity':[40,50,40,80,80,60,40]}  # this is a 'dict' (dictionary)
classdf = pd.DataFrame(data, columns=['Course','Sect','Time','Enrolled','Capacity'])
classdf

# The index is set by default to 0, 1, 2, ...
# We can change it to ['A','B','C','D','E','F','G'] for demonstration
classdf.index = ['A','B','C','A','E','B','A']
classdf

#### Extracting subsets of a DataFrame

## Columns
classdf['Enrolled']  # The column 'Enrolled'
classdf[['Course','Time']] # The columns Course, Time

# The above approach to extracting subsets has some limitations.
# Another method is to use the .loc add-on.  For example:

classdf.loc[:,'Enrolled'] # The column 'Enrolled', but with .loc which allows 
                          # easier access and better supports slicing

classdf.loc[:,'Sect':'Enrolled'] # The slice Sect-Enrolled, all rows
classdf.loc[:,['Sect','Enrolled','Time']] # A list of columns
classdf['Sect':'Enrolled'] # This doesn't work
classdf[:,'Sect':'Enrolled'] # Neither does this

## Rows
classdf.loc['B'] # Rows indexed by B
classdf.loc['C'] # Row indexed by C, but mildly annoying format
classdf.loc[['C'],:]  # Row C as dataframe
classdf.loc[['B','A'],:] # Rows indexed by B and A, all columns

classdf.loc['A':'C', :] # This doesn't work because index is not sorted and 'A' 
                        # is not a unique index label; see below

# classdf.loc refers to the "explicit" index of classdf.  The "implicit" index 
# is still 0, 1, 2, ....  This can be referred to using 'iloc'.  (We won't 
# use iloc that often, but it is occasionally handy.)
classdf
classdf.iloc[2:4,:] # the 3rd and 4th rows of df

## Columns and Rows
classdf.loc[['E','B'],'Time'] # Rows E, B, and column Time (type = Series)
classdf.loc[['B','A','E'],['Enrolled','Sect']] # Rows B,A,E; Columns Enrolled, Sect
classdf.loc[['C','B'],'Time':'Capacity'] # Slicing works
classdf.loc['C':'E','Sect':'Enrolled'] # Slicing works here too, 
                                       # but careful with index

## Examples of masking
classdf[classdf['Enrolled'] < 50]  # Slightly surprising that this produces rows
classdf.loc[classdf['Enrolled'] < 50]   # These give the same thing
classdf.loc[classdf['Enrolled'] < 50,:] # This might be the most clear

classdf.loc[classdf['Sect'] == 2, ['Time','Enrolled']] # Time, Enrolled for Sect = 2

# The Time for STAT 3A and Capacity < 50
classdf.loc[(classdf['Course']=='STAT 3A') & (classdf['Capacity']<50), 'Time']

np.mean(classdf) # Computes mean of all columns where it makes sense
np.mean(classdf)['Enrolled']  # Just the 'Enrolled' column

# Sorting
sorteddf1 = classdf.sort_values(by = 'Enrolled') # Sort by enrollment
sorteddf1

sorteddf2 = classdf.sort_values(by = 'Enrolled', ascending=False) # Reverse direction
sorteddf2

sorteddf3 = classdf.sort_values(by = ['Sect','Enrolled']) # Sort by Sect, then Enrolled
sorteddf3

#### Reading in a .csv file

# Note: The "working directory" must be set to the location of the file
# "samplegrades.csv" in order for the code below to work correctly.
gradesdf = pd.read_csv('samplegrades.csv')
gradesdf  # Note some values are NaN (missing).  In Spyder, clicking on the
          # name of the dataframe in "Variable explorer" brings up the
          # dataframe in table format.

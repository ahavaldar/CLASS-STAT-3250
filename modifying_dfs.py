##
## Topic: Modifying Data Frames; Demo of 'isin' and 'value_counts';
##        Finding hashtags and @'s; Misc others

import pandas as pd # load pandas as pd
import numpy as np  # load numpy as np

#### Adding and removing dataframe columns and rows

# A sample data frame
df = pd.DataFrame({'C1':['a','a','b','b','c','c','a','c'],
                   'C2':['a','y','b','x','x','b','x','a'],
                   'C3':[1,3,-2,-4,5,7,0,2],
                   'C4':[8,0,5,-3,4,1,3,1],
                   'C5':[3,-1,0,-2,4,3,8,0]})
df

# We can create an empty DataFrame column 
df['C6'] = ""
df

# Or we can add values to the column
df['C6'] = 13
df

# We can perform arithmetic on columns to create new columns
df['C7'] = df['C4']+df['C5']  # Vectorized -- faster than loops!
df

# Or modify old columns
df['C6'] = df['C5'] - 2*df['C4']
df

# And it's possible to divide columns
df['C3']/df['C4']


# We can drop columns with 'drop':
df = df.drop('C5',axis=1) # 'axis=1' specifies columns; 'axis=0' is rows
df

# Drop one more column
df = df.drop('C3',axis=1)
df

# Just for fun, let's remove a row too
df = df.drop(5, axis=0)  # Removing a row from the middle is fine
df

####  Changing specific data frame entries

# We can change individual entries with '.loc':
df.loc[3,'C6'] = 100  # Here '3' is in both the explicit and impicit index
df

# We can add lots of entries with a loop.  (Only do this if vectorization
# is not a good alternative.)
for i in range(len(df)):
    df.loc[i,'C7'] = (i+3)**2 # If column does not exist it is created.
df  # Note odd output in C7!

# Use '.loc'!! -- leaving it off does not work as expected!!
df[3,'C7'] = 100  # Don't do this!
df

# Let's remove the unwanted column
df.columns  # Here's the column list
df = df.drop((3,'C7'), axis=1) # 'axis=1' specifies columns; 'axis=0' is rows
df

#### Chained assignments

df['C4'][1] = 500  # This works, but produces a warning ('chained assignment')
df

df.loc[1,'C4'] = 1000  # This is the preferred approach
df

#### 'isin' and 'value_counts()'

# 'isin' allows one to automatically check if each element of one set is
# also an element of another set.  This can save from having to create
# inefficient/complicated loops.

# Test if each of 'a', 'b', 'c' is in df['C2'].  Note the use of 'pd.Series'
# because the first set cannot be a list, but a Series or DataFrame will work.
pd.Series(['a', 'b', 'c']).isin(df['C2'])

# Columns of data frames are automatically Series
df['C1'].isin(df['C2']) # Checks which entries of df['C1'] are in df['C2']

# Count the number of values in a Series (or data frame column)
df['C1'].value_counts()


#### Hunting for hashtags

# Start by defining a series of tweets:
ser1 = pd.Series(["Let's go #Hoos!",              # One: #Hoos
                 "I hope this works #STAT#3250",  # One: #STAT#3250
                 "I heart #UVAHoops,#Wahoowa",    # Two: #UVAHoops #Wahoowa
                 "DoesThis#Work?",                # This won't work
                 "What # about ?#This?",          # One: #This
                 "#This!#Here"])                  # Two: #This #Here
ser1 

# To look for "legal" hashtags it helps to change everything to lower
# case (hashtags are not case sensitive) and to replace all punctuation
# except "#" with spaces.  The code below does this.
ser2 = ser1.str.lower().str.replace(r'[^\w\s#]+', ' ')        
ser2

# In ser2 any "word" that begins with a "#" and contains another character
# is a legal hashtag.  Anything else is not.
# (Note: This is not completely in agreement with Twitter, but it's close and  
# matches the definition we will use on Assignment 4.)


#### How to find "Top-X and ties" automatically

# Example: Find the top-5 (plus ties) of Final scores from 'samplegrades.csv'
grades = pd.read_csv("samplegrades.csv")

# Start by sorting the final exam scores, largest to smallest
sortedfinals = grades['Final'].sort_values(ascending=False)
sortedfinals[:15]

# Use '.iloc' to find the score in 5th position:
cutval =  sortedfinals.iloc[5]
cutval
# Now identify all scores that are greater than or equal to that value, 
# then extract all of those values.
sortedfinals[sortedfinals >= cutval]

#### Removing "NaN"'s

grades['Math'] # The Math SAT scores

len(grades['Math'])  # The total number of entries, including NaN
len(grades['Math'].dropna())  # Just the actual entries, minus NaNs
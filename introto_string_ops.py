##
## Topic: String operations 
##

#### Introduction to String operations

import pandas as pd # load pandas as pd

teststr = "There are two ways of constructing a software design." 
teststr

# Extract substrings in specific positions
teststr[10:30] # Characters from positions indexed by 10-29
teststr[:20] # The first 20 characters
teststr[50:] # From the position indexed by 50 to the end
teststr[:-20] # All but the last 20 characters

teststr[3:8]+teststr[30:35] # Concatenate strings

teststr.upper() # upper case
teststr.lower() # lower case (Good for comparing when case in doubt)

teststr.index("o") # Index for first "o".
teststr.count("o") # Number of "o"s in str.
teststr.find("o") # Same as index
teststr.replace("are", "were") # Replace a substring

teststr.startswith("Hello") # Test if 'teststr' starts with given string
teststr.startswith("The")
teststr.endswith("ign") # Test if 'teststr' ends with given string
teststr.endswith("ign.")

teststr.split(" ") # split 'teststr' on " " (space)
teststr.split("s") # We can split on things other than spaces
teststr.split() # The default is spaces; extra spaces discarded
"ABCD".join(teststr.split("s")) # Splice strings together, with "ABCD"
"".join(teststr.split("s")) # Splice strings together, with no space
"s".join(teststr.split("s")) # Splice strings together, with s back in place


#### Vectorized string operations

# Many of the string operations seen previously can be automatically
# performed across a Series of strings without the use of a loop.
# (A column from a DataFrame is also a Series.)

# A practice DataFrame
df = pd.DataFrame({'C1':['a','a','b','b'],
                   'C2':['Wed May 21 15:47:57 2014 27 ExpFcn-2	lane Library/FortLewis/Algebra/10-1-Exponential-Functions/MCH1-10-1-6.pg',
                         'Wed May 21 15:52:15 2014 21 ExpFcn-2	lane Library/LoyolaChicago/Precalc/Chap4Sec2/Q18.pg',
                         'Wed May 21 16:02:01 2014 32 ExpFcn-2	lane Library/FortLewis/Algebra/10-3-TheExponent/MCH-10-3-6.pg',
                         'Wed May 21 16:18:55 2014 44 ExpFcn-2	lane Library/Union/setFunctionExponential/srw4_1_11.pg']})
df
# Below are some examples.  Let's isolate the second column of df.
recs = df['C2']
recs

# We can find the length of each string in 'recs'
recs.str.len()

# We can the start of the word 'Library' in each string
recs.str.find('Library')

# We can count the number of appearances of 'Library' in each string
recs.str.count('Library')

# Or we can check for 'FortLewis' in each string
recs.str.contains('FortLewis')

# We can mask on the result of 'str.contains' to extract the strings 
# that contain the word 'FortLewis'
newrecs = recs[recs.str.contains('FortLewis')]
newrecs

# We can pull out the substrings in specific positions from each string
newrecs.str[0:24]

# It's also possible to split each string
newrecs.str.split()  # Produces a Series of lists

# Then we can pull out just the time
newrecs.str.split().str[3]

# We can split again to get just the minutes
newrecs.str.split().str[3].str.split(':').str[1]


## Cleaning and searching strings

# Example: Find the number of times the word "her" is in each string in ser1,
#  regardless of case.
ser1 = pd.Series(["Her dog is brown.", 
                 "There is his cat.",
                 "Give her bird to her!"])

# It's usually a good idea to start by converting everything to lower
# case when we don't care about case.
ser2 = ser1.str.lower()  # Convert all the strings to lower case
ser2

# Now look for "her":
ser2.str.count('her')  # Check each string for "her" -- this is incorrect.

# We can pad the search for "her" with spaces to avoid things like "there":
ser2.str.count(' her ') # Still not right; problems at the ends and with punctuation

# We can add " " to the beginning and end of each string
ser3 = " " + ser2 + " " # adds a space to the beginning and end of each string 
ser3 # It's not easy to see any difference

# Try to count again
ser3.str.count(' her ') # Better but we still have problems with punctuation
                        # in the 3rd string

# The code below is an example of a "regular expression" (more on those
# later).  It will replace the punctuation with spaces.
ser4 = ser3.str.replace(r'[^\w\s]+', ' ') 
ser4

# One more pass at the count
ser4.str.count(' her ') # That works!

# We can pack all of the above together into a single line
ser5 = (" " + ser1.str.lower() + " ").str.replace(r'[^\w\s]+', ' ')

ser5.str.count(' her ') # Test the count again
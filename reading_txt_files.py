##
## Topic: Reading Text Files; Parsing Text Examples
## 

import numpy as np # load numpy as np
import pandas as pd # load pandas as pd

#### Reading text files

# The code below will read a text file, and put
# each line from the text file into a list
# element.
lines = open('transactions.txt').read().splitlines()

# It's difficult to work with a text file without looking at it
# to get a sense of the organization.  One approach is to print 
# out the first few lines.
lines[0:20]  

# It's also a good idea to open the file in a text editor.  Spyder is a
# good choice as it does not add in line breaks.  
# (Open 'transactions.txt' in Spyder now)


## Question 1: 
#   What schools have problems referenced in the file?
#   How many times is each school referenced?
#   (Schools are listed second in the 'Library/...' string.)

## Sample solution
    
# We have the opportunity to use vectorizable string operations
    
lines.str.contains('Library') # Doesn't work because 'lines' is a list
lines = pd.Series(lines)  # convert 'lines' to a series

lines.str.contains('Library') # We see 'True' for the entries in 'lines'
                                # containing 'Library'
       
# Next we mask to pull out the 'lines' entries that contain 'Library'                         
newlines = lines[lines.str.contains('Library')]
print(newlines)

# Next we grab the first line from 'newlines' to use for practice.
temp = newlines.iloc[0]
print(temp)

# Since the school name appears between the 1st and 2nd occurance of '/'
# we can split on '/' and pull out the second entry.  Let's try it on
# the temp string. (There is only one string, so we don't vectorize)
temp.split('/') # Just the split, to see what happens
temp.split('/')[1] # The second entry

# Looks like it works, so let's go through the entire list by vectorizing
schools = newlines.str.split('/').str[1]
print(schools)

np.unique(schools)  # This gives the unique schools
schools.value_counts()  # The number of times each school appears

## Question 2:
#   Obtain a list of the different unique problem file names.  These are the 
#   names that end in '.pg'.

## Sample solution

# The '.pg' names come at the end of the substring that starts with 'Library'.
# Here's our practice line again: 
print(temp)

# Let's split on ' ' (spaces):
temp.split(' ')

# It turns out that some of the spaces are actually tabs, indicated by '\t'
# Let's try just 'split()' instead
temp.split()

# This is more promising.  The substring with the '.pg' in it is 9th in the
# split list:
temp.split()[8]

# The substring can be split again, this time on '/':
temp.split()[8].split('/')

# We want the last item in this new list:
temp.split()[8].split('/')[-1]

# Now that this seems to be working, let's run through the full set of lines
# by vectorizing each step
problemfiles = newlines.str.split().str[8].str.split('/').str[-1]
print(problemfiles)

print(np.unique(problemfiles)) # The unique problem names
print(problemfiles.value_counts()) # Counts for each problem
##
## Topic: Regular Expressions - Part 2
##

import re   # The regular expression library

#### Brackets and ranges
    
string5 = "One more string with a 7."

## [a-d]
## Matches if any letter among a, b, c, d is in string
if re.search("[a-d]", string5):
    print("Yes")
   
## [a-zA-Z]
## Matches if any letter (upper or lower case) is in string
if re.search("[a-zA-Z]", string5):
    print("Yes")

## [a-z0-9]
## Matches if any lower case letter or a digit is in string
if re.search("[a-z0-9]", string5):
    print("Yes")

## [A-L0-5+?$]
## Matches if A-L, 0-5, +, ?, or $ is in string
if re.search("[A-L0-5+?$]", string5):
    print("Yes")

## [^A-L0-5+?$]
## Matches if A-L, +, ?, or $ is not in string
if re.search("[^A-L0-5+?$]", string5):
    print("Yes")
    

## Example: Check if the sentence starts with a capital letter.
    
string6 = "this is a tyPo filled sentence."

if re.search("^[A-Z]", string6):
    print("Yes")
    
## Example: Check if the sentence has a capital letter not at the start of a word.
    
if re.search("[a-z][A-Z]", string6):
    print("Yes")

## Example: Check if the sentence has a capitalized word in the middle.
    
if re.search(" [A-Z]", string6):
    print("Yes")
    

## Pandas and Regular Expressions
    
import pandas as pd  # usual pandas

# Note: pandas does not need the re library in order to interpret
# regular expressions.

# A few candidates for months
months = pd.Series(["16", "05", "10", "58"])    

# The regular expression to check the form to determine if it is
# a month
months.str.contains("0[1-9]|1[012]", regex=True) # allows "0X" or 10,11,12 

## Dates

# Now for some candidates for dates
dates = pd.Series(["12-28", "15-19", "10-31", "03-47"])    

# This is a sloppy check, it will allow things like 10-78,
# Note: "\d" is any digit
dates.str.contains("0[1-9]|1[012]-\d\d", regex=True)    

# This is a more careful check, but it does still allow 02-30
dates.str.contains("(?:0[1-9]|1[012])-(?:[0-2][0-9]|3[01])", regex=True)

## Dates With Years   

dateswithyears = pd.Series(["12-28-2019", "07-39-1996", "06-22-1968", "03-17-1872"])
    
# This checks for years in the 1900's or 2000's only
dateswithyears.str.contains("(?:0[1-9]|1[012])-(?:[0-2]\d|3[01])-(?:19|20)\d\d", regex=True) 

# Regular expressions also work with 'replace'
dateswithyears.str.replace("\d\d\d\d", "XXXXX", regex=True)

# Note: See the handy "Python Regular Expressions Cheat Sheet" for
#       more useful information.   
    

    
    
    
    






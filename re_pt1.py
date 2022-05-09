##
## Topic: Regular Expressions - Part 1
##

import re   # The regular expression library

#### Below are examples of the re.search function

#### Starters

string1 = "That is the start, this is the end!"

## start
## Matches "start" anywhere in the string
if re.search("start", string1):
    print("Yes")

## ^That
## Matches "That" if at the start of the string
if re.search("^That", string1):
    print("Yes")

## end$
## Matches "end" if at the end of the string
if re.search("end$", string1):
    print("Yes")  # not quite the end!
    
## end!$
if re.search("end!$", string1):  # The correct end of the string
    print("Yes")

#### Quantifiers

string2 = "Are those geese on the Mississippi River?"

## ge*s
## Matches "gs", "ges", "gees", "geees", etc (zero or more "e")
if re.search("ge*s", string2):
    print("Yes")

## ge+s
## Matches "ges", "gees", "geees", etc (one or more "e")
if re.search("ge+s", string2):
    print("Yes")

## ge?s
## Matches "gs" and "ges" only (zero or one "e")
if re.search("ge?s", string2):
    print("Yes")

## ge{2}s
## Matches "gees" only (exactly two "e")
if re.search("ge{2}s", string2):
    print("Yes")

## ge{2,4}s
## Matches "gees", "geees", and "geeees" (between 2 and 4 "e")
if re.search("ge{2,4}s", string2):
    print("Yes")
    
## M(iss)*
## Matches "M", "Miss", "Mississ", ... (any number of "iss")
if re.search("M(iss)*", string2):
    print("Yes")

## M(iss){3,}
## Matches "Missississ", ... (3 or more "iss")
if re.search("M(iss){3,}", string2):
    print("Yes")
    
## M(iss){2,}
## Matches "Mississ", "Missississ", ... (2 or more "iss")
if re.search("M(iss){2,}", string2):
    print("Yes")

#### OR operator

string3 = "This is the first bell"

## b(a|e|i)ll
## Matches "ball" or "bell" or "bill"
if re.search("b(a|e|i)ll", string3):
    print("Yes")
    
## b[aei]ll
## Matches "ball" or "bell" or "bill" (same as previous)
if re.search("b[aei]ll", string3):
    print("Yes")
    
#### Character classes

string4 = "Time to go be 4 I'm late"
   
## \d
## Matches if at least one digit in the string
if re.search("\d", string4):
    print("Yes")

## \D
## Matches if at least one nondigit in the string
if re.search("\D", string4):
    print("Yes")
        
# Note: See the handy "Python Regular Expressions Cheat Sheet" for
#       more useful information.   
    



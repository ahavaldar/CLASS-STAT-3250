##
## File: assignment02.py (STAT 3250)
## Topic: Assignment 2
##

## Two *very* important rules that must be followed in order for your 
## assignment to be graded correctly:
##
## a) The file name must be exactly "assignment02.py" (without the quotes)
## b) The variable names followed by "= None" must not be changed and these 
##    names variable names should not be used anywhere else in your file.  Do   
##    not delete these variables, if you don't know how to find a value just  
##    leave it as is. (If a variable is missing the autograder will not grade  
##    any of your assignment.)


## Questions 1-7: For the questions in this part, use the following
##  lists as needed:
    
list01 = [5, -9, -1, 8, 0, -1, -2, -7, -1, 0, -1, 6, 7, -2, -1, -5]
list02 = [-2, -5, -2, 8, 7, -7, -11, 1, -1, 6, 6, -7, -9, 1, 5, -11]
list03 = [9, 0, -8, 3, 2, 9, 3, -4, 5, -9, -7, -3, -11, -6, -5, 1]
list04 = [-4, -6, 8, 8, -5, -5, -11, -3, -1, 7, 0, 2, -5, -2, 0, -5]
list05 = [-11, -3, 8, -9, 2, -8, -7, -12, 7, 3, 2, 0, 6, 4, -11, 6]
biglist = list01 + list02 + list03 + list04 + list05

## Questions 1-7: Use for loops to answer each of the following applied  
##  to the lists defined above.
 
## 1.  Add up the squares of the entries of biglist.

squares = 0     # variable to store sqaures in the loop
for i in biglist:
    s = i **2       # squares each entry in biglist and saves it to s
    squares += s    # adds the resulting square to the squares variable

q1 = squares   # sum of squares of entries of biglist


## 2.  Create "newlist01", which has 14 entries, each the sum of the 
##      corresponding entry from list01 added to the corresponding entry
##      from list02.  That is,
##     
##         newlist01[i] = list01[i] + list02[i] 
##
##      for each 0 <= i <= 13.

newlist01 = 14*[0]      # creates a list with 14 0's as placeholders
for i in range(14):     # range set to 14 elements
    newlist01[i] = list01[i]+list02[i]      # adds each corresponding element 
                                            # in list01 and list02 together 
                                            # and replaces the result in 
                                            # newlist01

q2 = newlist01   # newlist01


## 3.  Determine the number of entries in biglist that are less than 6.

count = 0       # initializes counter to 0
for i in biglist:   # goes through each element in biglist
    if i < 6:       # check the condition if an element is less than 6
        count += 1  # if it is less than 6, it adds 1 to the counter


q3 = count   # number of entries in biglist less than 6


## 4.  Create a new list called "newlist02" that contains the elements of
##      biglist that are greater than 5, given in the same order as the
##      elements appear in biglist.

newlist02 = []      # intializes an empty newlist02
for i in biglist:
    if i > 5:       # checks if the element in biglist is greater than 5
        newlist02.append(i)   # if it is greater, it appends it in 
                              # order at the end of newlist02

q4 = newlist02   # newlist02


## 5.  Find the sum of the positive entries of biglist.

positives = 0       # initializes the counter
for i in biglist:
    if i > 0:       # checks positive condition
        positives += i      # if positive, adds to the counter


q5 = positives   # sum of the positive entries of biglist


## 6.  Make a list of the first 19 negative entries of biglist, given in
##      the order that the values appear in biglist.


neglist = []           # initializes an empty list
c = 0                   # initializes counter to 0
for i in biglist:
    if i < 0 and c < 19:  # runs as long as element is neg and c < 19 , which 
                          # means there are less than 19 elements in neglist
        neglist.append(i) # appends the neg value to neglist
        c += 1            # if negative, it adds to counter            
            
q6 = neglist   # list of first 19 negative entries of biglist
  
      
##  7. Identify all elements of biglist that have a smaller element that 
##      immediately preceeds it.  Make a list of these elements given in
##      the same order that the elements appear in biglist.

newlist03 = []
for i in range(1, len(biglist)):    # starts from index 1 since index 0 has 
                                    # no preceding value
    if biglist[i] > biglist[i-1]:   # compares the index to the preceding one
        newlist03.append(biglist[i])    # if it is bigger, it appends

q7 = newlist03   # list of elements preceded by smaller element


## Questions 8-9: These questions use simulation to estimate probabilities
##  and expected values.  

##  8. Consider the following game: You flip a fair coin.  If it comes up
##      tails, then you win $1.  If it comes up heads, then you get to 
##      simultaneously flip four more fair coins.  In this case you win $1 
##      for each head that appears on all flips, plus you get an extra $7 if 
##      all five flips are heads.
##
##      Use 100,000 simulations to estimate the average amount of money won 
##      when playing this game.
    
import numpy as np

ctarray01 = np.zeros(100000)                                                     # holds estimated earnings for each trial
for i in range(100000):
    flip01 = np.random.choice([1,0], size=1, p=[0.50, 0.50])  # flips coin once
    ct = 0                                                    # counter starts at 0
    if flip01 == 1:                                           # heads = 1
        flip02 = np.random.choice([1,0], size=4, p=[0.50, 0.50])    # flip 4 coins
        flip02 = np.append(flip02,[1])                              # adds the previous head to the flip list
        for j in flip02:
            if j == 1:                                        # if heads
                ct += 1                                       # it adds 1 to the counter
    if ct == 0:                                               # If the coin flip is 0
        ctarray01[i] = 1                                      # adds $1 to the array 
    if ct == 5:                                           # all flips are heads
        ctarray01[i] = 12                                 # money earned is $12
    if 0 < ct < 5:                                            # some flips are not heads
        ctarray01[i] = ct                                 # amount of money earned is the number of heads

money = np.mean(ctarray01)
q8 = money  # mean winnings from 100,000 times playing the game


##  9. Jay is taking a 15 question true/false quiz online.  The
##      quiz is configured to tell him whether he gets a question
##      correct before proceeding to the next question.  The 
##      responses influence Jay's confidence level and hence his 
##      exam performance.  In this problem we will use simulation
##      to estimate Jay's average score based on a simple model.
##      We make the following assumptions:
##    
##      * At the start of the quiz there is a 81% chance that 
##        Jay will answer the first question correctly.
##      * For all questions after the first one, if Jay got 
##        the previous question correct, then there is a
##        90% chance that he will get the next question
##        correct.  (And a 10% chance he gets it wrong.)
##      * For all questions after the first one, if Jay got
##        the previous question wrong, then there is a
##        72% chance that he will get the next question
##        correct.  (And a 28% chance he gets it wrong.)
##      * Each correct answer is worth 5 points, incorrect = 0.
##
##      Use 100,000 simulated quizzes to estimate Jay's average 
##      score.

avgarray = np.zeros(100000)
for i in range(100000):
    anslist = []                                                    # empty list to keep answer of previous question
    score = 0                                                       # initializing score to 0
    ans = int(np.random.choice([2,1], size = 1, p=[0.81, 0.19]))    # 2 is right, 1 is wrong
    anslist.append(ans)                                             # appending the answer to the answer list
    for j in range(1, 15):                                          # only until 15 answers
        if anslist[j-1] == 2:                                       # checks if previous answer is right
            ans2 = int(np.random.choice([2,1], size = 1, p=[0.90,0.10]))
            anslist.append(ans2)
        if anslist[j-1] == 1:                                       # checks if previous answer is wrong
            ans3 = int(np.random.choice([2,1], size = 1, p=[0.72,0.28]))
            anslist.append(ans3)
    for k in anslist:                                               # looping through ans list to calculate the score
        if k == 2:
            score += 5
    avgarray[i] = score
                    
winnings = np.mean(avgarray)

q9 = winnings  # mean score from 100,000 simulated quizzes



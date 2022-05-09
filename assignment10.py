##
## File: assignment10.py (STAT 3250)
## Topic: Assignment 10 
##

##  For this assignment you will be working with Twitter data related
##  to the season opening of Game of Thrones on April 14, 2019.  You will use 
##  a set of over 10,000 tweets for this purpose.  The data is in the file 
##  'GoTtweets.txt'.  

##  Note: On this assignment it makes sense to use loops to extract 
##  information from the tweets. Go wild.

##  The Gradescope autograder will be evaluating your code on a reduced 
##  version of the GoTtweets.txt data that includes only a fraction of the
##  records.  Your code needs to automatically handle all assignments
##  to the variables q1, q2, ... to accommodate the reduced data set,
##  so do not copy/paste things from the console window, and take care
##  with hard-coding values.  
import json
import pandas as pd
import numpy as np 
import re


tweetlist = []
for line in open('GoTtweets.txt', 'r'):            # loading in the tweet json file
    tweetlist.append(json.loads(line))


## 1.  The tweets were downloaded in several groups at about the same time.
##     Are there any that appear in the file more than once?  Give a Series 
##     with the tweet ID for any repeated tweets as the index and the number 
##     of times each ID appears in the file as values.  Sort by the index from
##     smallest to largest.

temp1 = []                  # creating empty list
for tweet in tweetlist:                 # appending the ID to the empty list
    temp1.append(tweet['id_str'])

temp1 = pd.Series(temp1)                # converting list to series
temp2 = temp1.groupby(temp1).count()            # counting the tweets by ID
temp2 = temp2[temp2>1].sort_index(ascending=True)       # getting tweets that appear more than once


q1 = temp2 # Series of tweet IDs that appear > 1 time


## Note: For the remaining questions in this assignment, do not worry about 
##       any duplicate tweets.  Just answer the questions based on the 
##       existing data set.
    

## 2.  Determine the number of tweets that include 'Daenerys' (any combination
##     of upper and lower case; part of another work OK) in the text of the 
##     tweet.  Then do the same for 'Snow'.

daenerys_ct = 0
for tweet in tweetlist:
    if re.search('[dD][aA][eE][nN][eE][rR][yY][sS]', tweet['text']):
        daenerys_ct += 1

snow_ct = 0
for tweet in tweetlist:
    if re.search('[sS][nN][oO][wW]', tweet['text']):
        snow_ct += 1

q2a = daenerys_ct  # number of tweets including 'daenerys'
q2b = snow_ct  # number of tweets including 'snow'


## 3.  Find the average number of hashtags included in the tweets. (You may get 
##     the wrong answer if you use the text of the tweets instead of the
##     hashtag lists.)
total_tweets = len(tweetlist)       # total number of tweets
    
hash_ct = 0                     # initialize counter
for tweet in tweetlist:         
    hash_ct += len(tweet['entities']['hashtags'])           # adding the number of hashtags per tweet to the counter

avg_hash = hash_ct / total_tweets

q3 = avg_hash  # average number of hashtags per tweet

 
## 4.  Determine the tweets that have 0 hashtags, 1 hashtag, 2 hashtags,
##     and so on.  Give your answer as a Series with the number of hashtags
##     as index (sorted smallest to largest) and the corresponding number of
##     tweets as values. Include in your Series index only number of hashtags  
##     that occur for at least one tweet. (Note: See warning in #3)
temp3 = []                  # empty list
for tweet in tweetlist:
    temp3.append(len(tweet['entities']['hashtags']))        # appending the number of hashtags to the empty list

temp4 = pd.Series(temp3)                # converting to series
temp4 = temp4.groupby(temp4).count().sort_index(ascending=True)     # getting count of hashtags and sorting


q4 = temp4  # Series of number of hashtags and counts


## 5.  Determine the number of tweets that include the hashtag '#GoT', then
##     repeat for '#GameofThrones'.  (You may get the wrong answer if you
##     use the text of the tweets instead of the hashtag lists.)
##     Note: Hashtags are not case sensitive, so any of '#GOT', '#got', 'GOt' 
##     etc are all considered matches.

got_ct = []                      # initialize list
for tweet in tweetlist:
    q = tweet['entities']['hashtags']           # setting q to be the dictionary of hashtags
    for i in q:                                 # loop through all the hashtags if multiple exists
        if re.search('\A([gG][oO][tT])\Z', i['text']):    
            got_ct.append(q)                         # appending tweet to list

got_ct2 = len(got_ct)                       # count

gameofthrones_ct = []
for tweet in tweetlist:
    q = tweet['entities']['hashtags']           # setting q to be the dictionary of hashtags
    for i in q:                                 # loop through all the hashtags if multiple exists
        if re.search('\A([gG][aA][mM][eE][oO][fF][tT][hH][rR][oO][nN][eE][sS])\Z', i['text']):    
            gameofthrones_ct.append(q)                         # appending tweet to list

gameofthrones_ct2 = len(gameofthrones_ct)       # count


q5a = got_ct2  # number of tweets with '#GoT' hashtag and upper/lower variants               
q5b = gameofthrones_ct2 # number of tweets with '#GameofThrones' hashtags and upper/lower variants             


## 6.  Some tweeters like to tweet a lot.  Find the screen name for all 
##     tweeters with at least 3 tweets in this data.  Give a Series with 
##     the screen name (in lower case) as index and the number of tweets as 
##     value, sorting by the index in alphbetical order.  
namelist = []               # initialize empty list
for tweet in tweetlist:
    namelist.append(tweet['user']['screen_name'])          # append user name to list

nameser = pd.Series(namelist)               # converting to series
nameser = nameser.str.lower()                   # converting to lowercase
namect = nameser.groupby(nameser).count()             # grouping by screen name
namect = namect[namect>=3].sort_index(ascending=True)           # getting user with 3+ tweets and sorting


q6 = namect  # Series of screen name and counts

    
## 7.  Among the screen names with 3 or more tweets, find the average
##     'followers_count' for each and then give a table with the screen  
##     and average number of followers.  (Note that the number of
##     followers might change from tweet to tweet.)  Give a Series with
##     screen name (in lower case) as index and the average number of followers  
##     as value, sorting by the index in alphbetical order.  
temp5 = pd.DataFrame(columns=('name','followers'))      # initialize DF  
tweetser = pd.Series(tweetlist)             # convert list to series

temp5['name'] = tweetser.str['user'].str['screen_name'].str.lower()         # adding all screen names to DF
temp5['followers'] = tweetser.str['user'].str['followers_count']        # adding all followers count to DF

temp5['ct'] = temp5.groupby(['name'])['followers'].transform('count')       # was looking to make a column with the counts of the usernames (code was used from https://stackoverflow.com/questions/17709270/create-column-of-value-counts-in-pandas-dataframe)
temp5 = temp5[temp5['ct']>=3]       # subsetting users with 3+ tweets
temp11 = temp5['followers'].groupby(temp5['name'])      # grouping followers by name
temp12 = temp11.mean().sort_index(ascending=True)           # getting mean followers and sorting


q7 = temp12  # Series of screen names and mean follower counts  

                                                                
## 8.  Determine the hashtags that appeared in at least 50 tweets.  Give
##     a Series with the hashtags (lower case) as index and the corresponding 
##     number of tweets as values, sorted alphabetically by hashtag.
temp6 = []              # initialize empty list
for tweet in tweetlist:
    q = tweet['entities']['hashtags']       # setting q equal to the dictionary of hashtags
    for i in q:
        temp6.append(i['text'])             # appending each hashtag to a list
        
temp6 = pd.Series(temp6)                    # converting to series
temp6 = temp6.str.lower()                   # converting to lowercase
temp6ct = temp6.groupby(temp6).count()      # counting the number of occurences for each hashtag
temp6ct = temp6ct[temp6ct >= 50].sort_index(ascending=True)         # sorting

q8 = temp6ct  # Series of hashtags and counts
        

##  9.  Some of the tweets include the location of the tweeter.  Give a Series
##      of the names of countries with at least three tweets, with country 
##      name as index and corresponding tweet count as values.  Sort the
##      Series alphabetically by country name.
countrylst = []         # initialize empty list
temp7 = []
for tweet in tweetlist:
    if tweet['place']!= None:
        temp7.append(tweet['place'])            # appends entries that include the place to another list

for i in temp7:
    countrylst.append(i['country'])         # appending country to country list

countryser = pd.Series(countrylst)              # converting to series
countryct = countryser.groupby(countryser).count()          # group by country and count
countryct = countryct[countryct >=3].sort_index(ascending=True)     # subsetting >=3 and sorting


q9 = countryct   # Series of countries with at least three tweets


## Questions 10-11: The remaining questions should be done using regular 
##                  expressions as described in the class lectures.

## 10.  Determine the percentage of tweets (if any) with a sequence of 3 or more
##      consecutive digits.  (No spaces between the digits!)  For such tweets,
##      apply 'split()' to create a list of substrings.  Among all the 
##      substrings with a sequence of at least three consecutive digits,
##      determine the percentage where the substring starts with a '@' at the 
##      beginning of the substring.
digitlst = []       # empty list
for tweet in tweetlist: 
    if re.search('\d{3,}', tweet['text']):      # searching and appending tweets with 3+ consecutive digits
        digitlst.append(tweet['text'])
dig_len = len(digitlst)
dig_perc = (dig_len / total_tweets) * 100       # getting percentage

digitser = pd.Series(digitlst)      # converting to series
tempser = digitser.str.split()      # splitting
at_ct = 0               # initalize counter
for i in tempser:
    for j in i:
        if re.search("^@", j):      # adds one to counter if @ appears in the start of the string
            at_ct += 1
at_perc = (at_ct / dig_len) *100    # calculating percentage


q10a = dig_perc  # percentage of tweets with three consecutive digits
q10b = at_perc  # percentage starting with @ among substrings with 3 consec digits


## 11.  Determine if there are any cases of a tweet with a 'hashtag' that is
##      actually not a hashtag because there is a character (letter or digit)
##      immediately before the "#".  An example would be 'nota#hashtag'.
##      Count the number of tweets with such an incorrect 'hashtag'.

incorrect = 0
for tweet in tweetlist:
    if re.search('(\w)#', tweet['text']):
        incorrect += 1

q11 = incorrect  # count of tweets with bad hashtag




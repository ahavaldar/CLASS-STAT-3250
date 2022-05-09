##
## File: assignment04.py (STAT 3250)
## Topic: Assignment 4
##

##  This assignment requires the data file 'airline_tweets.csv'.  This file
##  contains records of over 14000 tweets and associated information related
##  to a number of airlines.  You should be able to read this file in using
##  the usual pandas methods.

##  Gradescope will review your code using a version of airline_tweets.csv
##  that has had about 50% of the records removed.  You will need to write
##  your code in such a way that your file will automatically produce the
##  correct answers on the new set of data.  

import pandas as pd # load pandas as pd
import numpy as np  # load numpy as np

air = pd.read_csv('airline_tweets.csv')  # Read in the data set
temp = air.loc[0:10,:]
## Questions 1-8: These questions should be done without the use of loops.

## 1.  Determine the number of tweets for each airline, indicated by the
##      name in the 'airline' column of the data set.  Give the airline 
##      name and corresponding number of tweets as a Series with airline
##      name as the index, sorted by tweet count from most to least.
unique1 = air['airline'].groupby(air['airline'])        # group by airline
sum_airline = unique1.count().sort_values(ascending=False)        # count the number of tweets per airline
sum_airline = sum_airline.rename('Tweet Count')         # renaming the column for style

q1 = sum_airline  # Series of airlines and number of tweets


## 2.  For each airline's tweets, determine the percentage that are positive,
##      based on the classification in 'airline_sentiment'.  Give the airline 
##      name and corresponding percentage as a Series with airline
##      name as the index, sorted by percentage from largest to smallest
unique2 = air.loc[air['airline_sentiment'] == 'positive',:]                    # getting all the positive tweets               
unique3 = unique2['airline_sentiment'].groupby(unique2['airline'])                       # groups the positives by airline
unique3 = unique3.count()                                                     # counts the number of positive tweets by airline
pos_percentage = unique3.divide(sum_airline).sort_values(ascending=False)       # divides the positive tweets by total number of tweets to get the proportion
pos_percentage = pos_percentage.multiply(100)                                       # multiplies by 100 to get the percentage

q2 = pos_percentage  # Series of airlines and percentage of positive tweets


## 3.  Find all user names (in the 'name' column) with at least 25 tweets
##      along with the number of tweets for each.  Give the user names and
##      corresponding counts as a Series with user name as index, sorted
##      by count from largest to smallest
unique4 = air['name'].groupby(air['name'])                                  # group by name
unique5 = unique4.count().sort_values(ascending=False)                      # counting the number of tweets for each user then sorting high to low
unique5 = unique5.rename('Count')                                           # renaming to count for style
unique5 = unique5[unique5 >= 25]                                            # getting the values greater than 25
        
q3 = unique5  # Series of users with at least 25 tweets


## 4.  Determine the percentage of tweets from users who have more than five
##      tweets in this data set. (Note that this is not the same as the
##      percentage of users with more than five tweets.)
unique6 = air['name'].groupby(air['name'])                                  # group by name
unique7 = unique6.count().sort_values(ascending=False)                      # counting the number of tweets for each user then sorting high to low
total_len = len(unique7.index)                                                    # total number of users who tweeted
unique8 = unique7[unique7 > 5]                                            # getting the values greater than 5
greater5_len = len(unique8.index)                                 # total number of users who have more than 5 tweets
perc_greater5 = (greater5_len/total_len)*100                        # percentage calculation


q4 = perc_greater5  # Percentage of tweets from users with more than 5 tweets                            
                               

## 5.  Among the negative tweets, determine the four reasons are the most common.
##      Give the percentage among all negative tweets for each as a Series 
##      with reason as index, sorted by percentage from most to least
unique9 = air.loc[air['airline_sentiment'] == 'negative',:]             # subsets only the negative tweets
number_negtweets = len(unique9.index)                        # gets the number of negative tweets
unique10 = unique9['negativereason'].groupby(unique9['negativereason'])  # group by negative reason
reasons = unique10.count().sort_values(ascending=False)                       # reasons for the negative comment sorted high to low
reasons_top4 = reasons.nlargest(n=4)                                    # gets the top 4 most common reasons
prop_top4 = reasons_top4.divide(number_negtweets)                       # gets proportion of top4 reasons among all neg tweets
perc_top4 = prop_top4.multiply(100)                                 # percentage of top 4

q5 = perc_top4  # Series of reasons and percentages


## 6.  How many tweets include a link to a web site? (Indicated by the 
##      presence of "http" anywhere in the tweet.)
text = air['text']                          # extracting only the text column
http_count = text.str.count('http')         # counts the appearance of "http" in each tweet
http_series = http_count[http_count != 0]   # adds only the tweets that contains http to a new series
len_http = len(http_series.index)           # counts number of tweets that contains http 

q6 = len_http  # Number of tweets that include a link


## 7.  How many tweets include the word "air" (upper or lower case,
##      not part of another word)?
new_text = text.str.lower().str.replace(r'[^\w\s]+', ' ')           # replaces all punctuation with an empty space and makes it lowercase
air_count = new_text.str.count(' air ')                 # finds all tweets with "air" in it
air_series = air_count[air_count != 0]              # adds only the tweets that contains air to a new series
len_air = len(air_series.index)                     # counts number of tweets that contains air 

q7 = len_air  # Number of tweets that include 'air'


## 8.  How many times total does the word "help" appear in a tweet, either in
##      upper or lower case and not part of another word.
help_count = new_text.str.count(' help ')                 # finds all tweets with "help" in it
total_help = help_count.sum()                             # sums up all the times "help" appears in all tweet  

q8 = total_help  # Number of times that 'help' is included


## Questions 9-13: Some of these questions can be done without the use of 
##  loops, while others cannot.  It is preferable to minimize the use of
##  loops where possible, so grading will reflect this.
##
##  Some of these questions involve hashtags and @'s.  These are special 
##  Twitter objects subject to special rules.  For these problems we assume
##  that a "legal" hashtag:
##
##  (a) Starts with the "#" (pound) symbol, followed by letter and/or numbers 
##       until either a space or punctuation mark (other than "#") is encountered.
##   
##      Example: "#It'sTheBest" produces the hashtag "#It"
##
##  (b) The "#" symbol can be immediately preceded by punctuation, which is 
##       ignored. If "#" is immediately preceded by a letter or number then
##       it is not a hashtag.
##
##      Examples: "The,#dog,is brown"  produces the hashtag "#dog"
##                "The#dog,is brown" does not produce a hashtag
##                "#dog1,#dog2" produces hashtags "#dog1" and "#dog2"
##                "#dog1#dog2" produces the hashtag "#dog1#dog2"
##
##  (c) Hashtags do not care about case, so "#DOG" is the same as "#dog"
##       which is the same as "#Dog".
##
##  (d) The symbol "#" by itself is not a hashtag
##
##  The same rules apply to Twitter handles (user names) that begin with the
##   "@" symbol.         

## 9.  How many of the tweets have at least two Twitter handles?
handle_count = text.str.count('@')              # counts how many times "@" appears in a tweet
handle_count = handle_count[handle_count >=2]       # gets tweets that have at least two twitter handles
handle_count = handle_count.rename('number')        # rename for clarity
num_handle_count = len(handle_count.index)          # counts number of tweets that have at least two twitter handles

q9 = num_handle_count # number of tweets with @ directed at a user besides the target airline


## 10. Suppose that a score of 3 is assigned to each positive tweet, 1 to
##      each neutral tweet, and -2 to each negative tweet.  Determine the
##      mean score for each airline and give the results as a Series with
##      airline name as the index, sorted by mean score from highest to lowest.
sentiment = air[['airline_sentiment']]              # extracting the airline sentiment column as a dataframe
sentiment = sentiment['airline_sentiment'].replace({"positive": 3, "neutral":1, 
                                                    "negative":-2})            # assigning the new values for the sentiments
airline_col = air['airline']                        # extracting the airline column as a series
new_sentiment = pd.concat([sentiment, airline_col], axis=1)                     # joining the airline column series to the new sentiment series using pd.concat to form a dataframe
sentiment_group = new_sentiment['airline_sentiment'].groupby(new_sentiment['airline'])      # groups the sentiment number by airline
mean_score = sentiment_group.mean().sort_values(ascending=False)                 # calculates the mean score per airline from highest to lowest

q10 = mean_score  # Series of airlines and mean scores 


## 11. What is the total number of hashtags in tweets associated with each
##      airline?  Give a Series with the airline name as index and the
##      corresponding totals for each, sorted from most to least.
hashtag_text = text.str.lower().str.replace(r'[^\w\s#]+', ' ')          # puts all text into lowercase, and replaces all punctation except "#" with spaces
hashtag_count = hashtag_text.str.count("#")                             # counting how manu hashtags appear in a tweet 
hashtag_df = pd.concat([hashtag_count, airline_col], axis=1)              # joining the airline column series to the hashtag_text series using pd.concat to form a dataframe
hashtag_group = hashtag_df['text'].groupby(hashtag_df['airline'])       # grouping the count of hashtags by airline
hashtag_sum = hashtag_group.sum().sort_values(ascending=False)             # gives the total number of hashtags in tweets for each airline from most to least


q11 = hashtag_sum  # Series of airlines and hashtag counts


## 12. Among the tweets that "@" a user besides the indicated airline, 
##      find the percentage that include an "@" directed at the other  
##      airlines in this file. 
###------------ Mostly the same code from Q9-----------------------------
num_handle_count                                    # number of tweets with @ directed at a user besides the target airline
handle_text = text.str.lower().str.replace(r'[^a-zA-Z\d\s@]', " ")           # uses a regex to remove all special characters except for spaces and "@"
handle_count = handle_text.str.count('@')              # counts how many times "@" appears in a tweet
handle_count = handle_count[handle_count >=2]       # gets tweets that have at least two twitter handles
handle_count = handle_count.rename('number')        # rename for clarity
###-----------------------------------------------------------------------
handle_df = pd.concat([handle_count, handle_text], axis=1) # adds back in the texts and airline name
handle_df = handle_df.dropna()            # drops the NA's and only leaves the texts with 2+ @s

new_handle_text = handle_df['text']         # subsets just the text 
split_text = new_handle_text.str.split(' ') # splits on empty space
handle_list = ['@virginamerica', '@united', '@southwestair', '@jetblue',
               '@usairways', '@americanair']                # create a list of the twitter handles

counter_list = []                # intialize an empty list
for i in split_text:        # loop through the text
    ct = 0                  # initialize counter
    for j in i:
        if j in handle_list:
            ct += 1         # if the airline twitter handle appears in the list, it adds one to the counter
    counter_list.append(ct)      # append the value of each tweet to the list
  
greater1_list = []          # initalize an empty list
for i in counter_list:
    if i > 1:               # checks for all tweets with 2+ 
        greater1_list.append(i)      # appends it to the list

greater1_count = len(greater1_list)      # gets the number of 2+ tweets

at_perc = (greater1_count/num_handle_count) *100        # calculates the percentage

q12 = at_perc # Percentage of tweets 


## 13. Suppose the same user has two or more tweets in a row, based on how they 
##      appear in the file. For such tweet sequences, determine the percentage
##      for which the most recent tweet (which comes nearest the top of the
##      file) is a positive tweet.

df = air[['name', 'airline_sentiment']]         # subset out name and airline sentiment as a separate df
df = df.dropna()                                # drop any NAs

counter_1 = 0                   # initialize counter
counter_2 = 0                   # initialize counter
for i in range(1,df.shape[0]-1):                # loop through df 
    if df.loc[i,'name'] != df.loc[i-1,'name'] and df.loc[i,'name'] == df.loc[i+1,'name']:       # condition to get only the top name of a user who tweeted 2 or more times consecutively 
        counter_1 += 1                          # adds to counter
        if df.loc[i,'airline_sentiment'] == 'positive':     # check to see if positive appears in the row
            counter_2 += 1                                  # adds to counter

q13_perc = (counter_2 / counter_1) *100         # calculates percentage


q13 = q13_perc  # Percentage of tweets


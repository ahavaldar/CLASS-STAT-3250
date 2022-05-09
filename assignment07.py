##
## File: assignment07.py (STAT 3250)
## Topic: Assignment 7 
##

##  This assignment requires the data file 'movies.txt'.  This file
##  contains records for nearly 3900 movies, including a movie ID number, 
##  the title (with year of release, which is not part of the title), and a 
##  list of movie genre classifications (such as Romance, Comedy, etc).  Note
##  that a given movie can be classified into more than one genre -- for
##  instance Toy Story is classified as "Animation", "Children's", and 
##  "Comedy".

##  Note: Some or all of the questions on this assignment can be done without the 
##  use of loops, either explicitly or implicitly (apply). As usual, scoring 
##  will take this into account.

##  The Gradescope autograder will be evaluating your code on a reduced 
##  version of the movies.txt data that includes only a fraction of the
##  records.  Your code needs to automatically handle all assignments
##  to the variables q1, q2, ... to accommodate the reduced data set,
##  so do not copy/paste things from the console window, and take care
##  with hard-coding values.  

import numpy as np # load numpy as np
import pandas as pd # load pandas as pd

# Read in the movie data as text; leave in encoding = 'utf8'
movielines = open('movies.txt', encoding = 'utf8').read().splitlines()
ser = pd.Series(movielines)


## 1.  Determine the number of movies included in genre "Animation", the number
##     in genre "Horror", and the number in both "Comedy" and "Crime".

anim = len(ser[ser.str.contains('Animation')])      # length of series whose entries contain animation
horr = len(ser[ser.str.contains('Horror')])         # length of series whose entries contain horror
com = len(ser[ser.str.contains('Comedy') & ser.str.contains('Crime')])      # length of series whose entries contain crime and comedy


q1a = anim  # Genre includes Animation
q1b = horr  # Genre includes Horror
q1c = com  # Genre includes both Comedy and Crime


## 2.  Among the movies in the genre "Horror", what percentage have the word
##     "massacre" in the title?  What percentage have 'Texas'? (Upper or lower
##     cases are allowed here.) 
horror = ser[ser.str.contains('Horror')]        # all entries in Horror
horror = horror.str.lower()                         # making all entries lowercase
horr_len = len(horror)                              # number of all horror films

massacre = len(horror[horror.str.contains('massacre')])     # films that contain massacre
mass_perc = (massacre/horr_len) *100                # massacre percentage

texas = len(horror[horror.str.contains('texas')])           # films that contain texas
texas_perc = (texas/horr_len) *100                      # texas percentage


q2a = mass_perc  # percentage in Horror that includes 'massacre'
q2b = texas_perc  # percentage in Horror that includes 'texas'


## 3.  Among the movies with exactly one genre, determine the genres that
##     have at least 50 movies classified with that genre.  Give a Series 
##     with genre as index and counts as values, sorted largest to smallest 
##     by count.
q3df = pd.DataFrame(ser)                # turn into dataframe
q3df['new'] = ser.str.find('|')         # count the characters until the first '|'
one = q3df[q3df['new']==-1]             # one genre movies have a value of -1
one = one.drop(['new'], axis=1)         # drop the character count
one = one.squeeze()                     # turn into series

temp = one.str.split('::').str[-1]      # get the last element in the list which is genre
count = temp.groupby(temp).count()      # count the genres
count = count[count>=50].sort_values(ascending=False)       # get genres with 50+ largest to smallest

q3 = count  # Series of genres for at least 50 movies and counts


## 4.  Determine the number of movies that have 1 genre, 2 genres, 3 genres, 
##     and so on.  Give your results in a Series, with the number of genres
##     as the index and the counts as values, sorted by index values from
##     smallest to largest. 
num = ser.str.split('::').str[-1].str.split('|').str.len()     # how many genres each movie has
num = num.groupby(num).count().sort_index(ascending=True)      # counts the number of movies for each number of genres

q4 = num  # Series of number of genres and counts


## 5.  How many remakes are in the data? We say a movie is a remake if the title is
##     exactly the same as the title of an older movie. For instance, if 'Hamlet'  
##     is in the data set 4 times, then 3 of those should be counted as remakes.
##     (Note that a sequel is not the same as a remake -- "Jaws 2" is completely
##     different from "Jaws".)
remakes = ser.str.split('::').str[1].str[:-7]           # gets only the title of the movie
rem_gb = remakes.groupby(remakes).count()               # counts the number of times a title appears
rem_gb = rem_gb[rem_gb>1]                       # only keep the films with a remake
rem_ct = rem_gb - 1                             # gets rid of the original in the count
rem_ct = rem_ct.sum()                           # total number of remakes

q5 = rem_ct  # number of remakes in data set


## 6.  Determine for each genre the percentage of movies in the data set that
##     are classified as that genre.  Give a Series of all with 8% or more,
##     with genre as index and percentage as values, sorted from highest to 
##     lowest percentage. 

genre = ser.str.split('::').str[-1].str.split('|')      # gets only the genres
new_genre = genre.explode()          # singles out each individual genre
genre_ct = new_genre.groupby(new_genre).count()      # counts the number of movies that belongs to each genre
q6_perc = ((genre_ct / len(genre)) * 100).sort_values(ascending=False)         # gets percentages

q6 = q6_perc  # Series of genres and percentages


## 7.  It is thought that musicals have become less popular over time.  We 
##     judge that assertion here as follows: Compute the median release year 
##     for all movies that have genre "Musical", and then do the same for all
##     other movies.  
mus = ser[ser.str.contains('Musical')]          # all films that are musicals
mus_yr = mus.str.split('::').str[1].str[-5:-1].astype(int)      # year of all films
mus_med = np.median(mus_yr)         # median of all films
    
nonmus = ser[~ser.str.contains('Musical')]          # all films that aren't musicals
nonmus_yr = nonmus.str.split('::').str[1].str[-5:-1].astype(int)        # getting integer year
nonmus_med = np.median(nonmus_yr)       # median of non musical

q7a = mus_med  # median release year for Musical
q7b = nonmus_med  # median release year for non-Musical 


##  8. Determine how many movies came from each decade in the data set.
##     An example of a decade: The years 1980-1989, which we would label as
##     1980.  (Use this convention for all decades in the data set.) 
##     Give your answer as a Series with decade as index and counts as values,
##     sorted by decade 2000, 1990, 1980, ....
yr = ser.str.split('::').str[1].str[-5:-2]  # get the first 3 digits of the year
yr = yr + '0'                                 # adding a 0 on the end to get the decade
yr_ct = yr.groupby(yr).count().sort_index(ascending=False)      # getting the number of movies in each decade

q8 = yr_ct  # Series of decades and counts


##  9. For each decade in the data set, determine the percentage of titles
##     that have exactly one word.  (Note: "Jaws" is one word, "Jaws 2" is not)
##     Give your answer as a Series with decade as index and percentages as values,
##     sorted by decade 2000, 1990, 1980, ....
dec = ser.str.split('::').str[1].str[:-2] + '0'        # get title and decade
temp3 = pd.DataFrame(dec)                               # converting to dataframe
temp3['name'] = temp3.iloc[:, 0]                        # new column of the first column with a column name
temp3 = temp3.drop(columns=temp3.columns[0], axis=1)    # drop old column    
temp3['year'] = temp3.name.str[-4:]                     # make year column
temp3['name'] = temp3.name.str[:-6]                     # simplify name column
temp3['name'] = temp3['name'].str.split(' ')            # split name column on spaces

total_gb = temp3['name'].groupby(temp3['year'])         # group movies by year
total_ct = total_gb.count()                             # get count of movies by year

temp4 = temp3[temp3['name'].str.len()==1]               # getting movies with length one indicating it is a one word title
temp5 = temp4['name'].groupby(temp4['year'])            # group these movies by year
word_ct = temp5.count()                                 # get count of these movies

q9_perc = (word_ct/total_ct)*100                        # getting percentage
q9_perc = q9_perc.sort_index(ascending=False)           # sorting values
q9_perc = q9_perc.fillna(0)

q9 = q9_perc  # Series of percentage 1-word titles by decade

## 10. For each genre, determine the percentage of movies classified in
##     that genre also classified in at least one other genre.  Give your 
##     answer as a Series with genre as index and percentages as values, 
##     sorted largest to smallest percentage.
gen = ser.str.split('::').str[-1].str.split('|')        # getting the genres
gen = pd.DataFrame(gen)             # converting to dataframe
gen['genre'] = gen.iloc[:, 0]           # creating new column with genre column name
gen = gen.drop(columns=gen.columns[0], axis=1)
gen['len'] = gen['genre'].str.len()         # getting the length of each column

temp6 = gen['genre'].explode()              # separating all the genres   
temp6 = pd.DataFrame(temp6)                 # converting to dataframe
temp6['len'] = gen['len']                   # adding the length for each row to the dataframe

temp7 = temp6['genre'].groupby(temp6['genre'])      # group by genre
temp8 = temp7.count()                       # total number of movies for each genre

temp9 = temp6[temp6['len']!=1]              # getting genres that have more than one genre
temp10 = temp9['genre'].groupby(temp9['genre'])         # grouping by genre
temp11 = temp10.count()                 # count of movies that have more than one genre

q10_perc = ((temp11/temp8)*100).sort_values(ascending=False)    # calculating percentage

q10 = q10_perc  # Series of genres, percentages

   

##
## File: assignment08.py (STAT 3250)
## Topic: Assignment 8 
##

##  This assignment requires data from three files: 
##
##      'movies.txt':  A file of nearly 3900 movies
##      'reviewers.txt':  A file of over 6000 reviewers who provided ratings
##      'ratings.txt':  A file of over 1,000,000 movie ratings
##
##  The file 'readme.txt' has more information about these files.
##  You will need to consult the readme.txt file to answer some of the questions.

##  Note: Some or all of the questions on this assignment can be done without the 
##  use of loops, either explicitly or implicitly (apply). As usual, scoring 
##  will take this into account.

##  The Gradescope autograder will be evaluating your code on a reduced 
##  version of the movies.txt data that includes only a fraction of the
##  records.  Your code needs to automatically handle all assignments
##  to the variables q1, q2, ... to accommodate the reduced data set,
##  so do not copy/paste things from the console window, and take care
##  with hard-coding values.  


## It is recommended that you read in the data sets in the manner shown below.
movietext = open('movies.txt', encoding='utf8').read().splitlines()
reviewertext = open('reviewers.txt', encoding='utf8').read().splitlines()
ratingtext = open('ratings.txt', encoding='utf8').read().splitlines()

import pandas as pd
import numpy as np

## 1.  Based on the data in 'reviewers.txt': Determine the percentage of all 
##     reviewers that are female.  Determine the percentage of all reviewers in
##     the 35-44 age group.  Among the 18-24 age group, find the percentage 
##     of reviewers that are male.
rev_ser = pd.Series(reviewertext)           # converting to series
rev_ser = rev_ser.str.split('::')           # splitting
rev_df = pd.DataFrame(rev_ser)              # converting to dataframe
rev_df['col'] = rev_df.iloc[:, 0]                        # new column of the first column with a column name
rev_df = rev_df.drop(columns=rev_df.columns[0], axis=1)     # dropping old column
rev_df = pd.DataFrame(rev_df['col'].to_list(), columns = 
                          ['ReviewerID', 'Gender', 'Age', 'Occupation',
                           'Zip-Code', 'State'])                # making dataframe with each element as a column

rev_tot = len(rev_df)              # total number of reviewers
fem_tot = len(rev_df[rev_df['Gender'] == 'F'])  # number of females
fem_perc = (fem_tot / rev_tot) *100          # female percentage

rev_df['Age'] = rev_df['Age'].astype(int)       # converting age to an int
tot35 = len(rev_df[rev_df['Age']==35])          # how many people aged 35-44
perc35 = (tot35 / rev_tot) *100                 # 35-44 percentage

rev18_df = rev_df[rev_df['Age']==18]            # subset age range 18-24
tot18 = len(rev18_df)
male18 = len(rev18_df[rev18_df['Gender']=='M'])     # subset out the males
perc18 = (male18 / tot18)* 100                  # male 18-24 percentage

q1a = fem_perc  # percentage of female reviewers
q1b = perc35  # percentage age 35-44
q1c = perc18  # percentage of males reviewers in 18-24 age group


## 2.  Give a year-by-year Series of counts for the number of ratings, with
##     the rating year as index and the counts as values, sorted by rating
##     year in ascending order.
ratings_ser = pd.Series(ratingtext)
ratings_ser = ratings_ser.str.split('::')
ratingsdf = pd.DataFrame(ratings_ser)
ratingsdf['col'] = ratingsdf.iloc[:, 0]                        # new column of the first column with a column name
ratingsdf = ratingsdf.drop(columns=ratingsdf.columns[0], axis=1)     # dropping old column
ratingsdf = pd.DataFrame(ratingsdf['col'].to_list(), columns = 
                          ['ReviewerID', 'MovieID', 'Rating', 'Time'])

ratingsdf['dts'] = pd.to_datetime(ratingsdf['Time'], unit='s')      # converting to year format
ratingsdf = ratingsdf.drop(['Time'], axis=1)                        # dropping old column
ratingsdf['year'] = ratingsdf['dts'].dt.year                        # getting only the year
yr_gb = ratingsdf['year'].groupby(ratingsdf['year'])                # group by year
yr_count = yr_gb.count().sort_index(ascending=True)                 # getting the count and sorting


q2 = yr_count  # Series of rating counts by year rated


## 3.  Determine the average rating from female reviewers and the average  
##     rating from male reviewers.

df = pd.merge(rev_df,ratingsdf)     # merging the dataframes
df['Rating'] = df['Rating'].astype(int)     # converting Rating to an int
rating_gb = df['Rating'].groupby(df['Gender'])      # group rating by gender
avg_rat = rating_gb.mean()                          # mean by gender

male = df[df['Gender']=='M']                # subsetting males
male_avg = np.average(male['Rating'])       # average rating

fem = df[df['Gender']=='F']                 # subsetting females
fem_avg = np.average(fem['Rating'])         # average rating

q3a = fem_avg  # average rating for female reviewers
q3b = male_avg  # average rating for male reviewers


## 4.  Determine the number of movies that received an average rating of 
##     less than 1.75.  (Movies and remakes should be considered as
##     different.)
movies_ser = pd.Series(movietext)
movies_ser = movies_ser.str.split('::')
moviesdf = pd.DataFrame(movies_ser)
moviesdf['col'] = moviesdf.iloc[:, 0]                        # new column of the first column with a column name
moviesdf = moviesdf.drop(columns=moviesdf.columns[0], axis=1)     # dropping old column
moviesdf = pd.DataFrame(moviesdf['col'].to_list(), columns = 
                          ['MovieID', 'Title', 'Genre'])

df2 = pd.merge(df, moviesdf)
temp = df2.loc[:, ['Rating', 'MovieID']]                # getting rating and movie ID
temp['MovieID'] = temp['MovieID'].astype(int)           
mov_gb = temp['Rating'].groupby(temp['MovieID'])        # grouping rating by movie ID
mov_avg = mov_gb.mean()
mov_num = len(mov_avg[mov_avg < 1.75])                  # getting averages less than 1.75        

q4 = mov_num  # count of number with average rating less than 1.75


## 5.  Determine the number of movies listed in 'movies.txt' for which there
##     is no rating in 'ratings.txt'.  
temp2 = pd.merge(moviesdf, ratingsdf, how='outer')      # outer join to keep movies without a rating - introduced as NAs
temp3 = temp2[temp2['Rating'].isna()]                   # extracting NA rows
nonrated = len(temp3)                                   

q5 = nonrated  # number of movies that were not rated


## 6.  Among the ratings from male reviewers, determine the average  
##     rating for each occupation classification (including 'other or not 
##     specified'), and give the results in a Series sorted from highest to 
##     lowest average with the occupation title (not the code) as index.

temp4 = df.loc[:,['Gender', 'Occupation', 'Rating']]        # subsetting the gender, occ, rating cols
temp4 = temp4.loc[temp4['Gender']=='M']                     # getting only male reviews
temp4['Occupation'] = temp4['Occupation'].astype(int)       # converting occupation code to int
occ_gb = temp4['Rating'].groupby(temp4['Occupation'])       # group rating by occupation
occ_avg = occ_gb.mean().sort_index(ascending=True)          # sorting the resulting means from lowest to highest index
occs = ["other","academic/educator","artist","clerical/admin",          # list of all the occupations
        "college/grad student","customer service","doctor/health care",
        "executive/managerial","farmer","homemaker","K-12 student","lawyer",
        "programmer","retired","sales/marketing","scientist","self-employed",
        "technician/engineer","tradesman/craftsman","unemployed","writer"]
occ_avg = occ_avg.set_axis(occs)                            # reindexing the series based on the occupation names
occ_avg = occ_avg.sort_values(ascending=False)              # sorting values    

q6 = occ_avg  # Series of average ratings by occupation


## 7.  Determine the average rating for each genre, and give the results in
##     a Series with genre as index and average rating as values, sorted 
##     alphabetically by genre.

temp5 = df2.loc[:,['Rating', 'Genre']]          # getting the rating and genre columns
temp5['Genre'] = temp5['Genre'].str.split('|')      # splitting the genre column
temp6 = temp5['Genre'].explode()                    # exploding the genre column
temp7 = pd.DataFrame(temp6)                         # converting the genre column to a dataframe
temp5 = temp5.drop(['Genre'], axis=1)               # dropping the genre column in the original df
temp8 = pd.concat([temp5, temp7], axis=1)           # concat both DFs
genre_gb = temp8['Rating'].groupby(temp8['Genre'])  # grouping rating by genre
genre_avg = genre_gb.mean()                         # average rating for each genre automatically sorted alphabetically

q7 = genre_avg  # Series of average rating by genre   


## 8.  For the reviewer age category, assume that the reviewer has age at the 
##     midpoint of the given range.  (For instance '35-44' has age (35+44)/2 = 39.5)
##     For 'under 18' assume an age of 16, and for '56+' assume an age of 60.
##     For each possible rating (1-5) determine the average age of the reviewers
##     giving that rating.  Give your answer as a Series with rating as index
##     and average age as values, sorted by rating from 1 to 5.
temp9 = df2.loc[:,['Age', 'Rating']]                # subsetting out Age and Rating
temp9['Age'] = temp9['Age'].astype(int)             # converting to int
temp9['Rating'] = temp9['Rating'].astype(int)       # converting to int
temp9['Age'] = temp9['Age'].replace({1:16, 18:21, 25:29.5, 35:39.5, 45:47,
                                     50:52.5, 56:60})       # replacing ages with mean of age range
age_gb = temp9['Age'].groupby(temp9['Rating'])              # group age by rating
age_avg = age_gb.mean().sort_index(ascending=True)          # finding average age for each rating and sorting

q8 = age_avg  # Series of average age by rating


## 9.  Find the top-5 "states" in terms of average rating.  Give as a Series
##     with the state as index and average rating as values, sorted from 
##     highest to lowest average rating. (Include any ties as usual)
##     Note: "states" includes US territories and military bases. See the 
##     readme.txt file for more information on what constitutes a "state"
##     for this assignment.
temp10 = df2.loc[:,['State', 'Rating']]                 # subsetting state and rating
state_gb = temp10['Rating'].groupby(temp10['State'])        # grouping rating by state
state_avg = state_gb.mean().drop(labels=['Unknown']).nlargest(5, keep='all').sort_values(ascending=False)        # mean of each state, removing unknown since it isn't a state, and getting 5 largest


q9 = state_avg  # top-5 states by average rating


## 10. For each age group, determine the occupation that gave the lowest 
##     average rating.  Give a Series that includes the age group code and 
##     occupation title as a multiindex, and average rating as values.  Sort  
##     the Series by age group code from youngest to oldest. 
temp11 = df2.loc[:, ['Age', 'Occupation', 'Rating']]            # subsetting out age, occupation, and rating
temp11['Occupation']=temp11['Occupation'].astype(int)           # converting to int
temp11['Rating']=temp11['Rating'].astype(int)                   # converting to int
temp11['Occupation'] = temp11['Occupation'].replace({0:"other",1:"academic/educator",
                                                     2:"artist",3:"clerical/admin",
        4:"college/grad student",5:"customer service",6:"doctor/health care",
        7:"executive/managerial",8:"farmer",9:"homemaker",10:"K-12 student",11:"lawyer",
        12:"programmer",13:"retired",14:"sales/marketing",15:"scientist",16:"self-employed",
        17:"technician/engineer",18:"tradesman/craftsman",19:"unemployed",20:"writer"})     # replacing numeric values with occupation name

rat_gb = temp11['Rating'].groupby([temp11['Age'], temp11['Occupation']])        # group rating by age and occupation
means = rat_gb.mean()           # average rating for each occupation within each age range
means2 = means.groupby('Age', group_keys=False).nsmallest(1).sort_index(ascending=True)         # getting lowest value for each occupation in each age range, and sorted

q10 = means2  # Series of average ratings by age code and occupation title




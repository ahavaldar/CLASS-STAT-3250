##
## File: assignment03.py (STAT 3250)
## Topic: Assignment 3 
##

##  The questions in this assignment refer to the data in the
##  file 'absent.csv'.  The data contains 740 records from an
##  employer, with 21 columns of data for each record.  (There
##  are a few missing values indicated by zeros where zeros 
##  are clearly not appropriate.)  The file 'absent.pdf' has
##  a summary of the meanings for the variables.
##
##  All of these questions can be completed without loops.  You 
##  should try to do them this way, "code efficiency" will take 
##  this into account.

import numpy as np  # load numpy as np
import pandas as pd # load pandas as pd

absent = pd.read_csv('absent.csv')  # import the data set as a pandas dataframe

## 1.  Find the mean absent time among all records.
absenteeism_mean = np.mean(absent)['Absenteeism time in hours']     # using np to find mean 

q1 = absenteeism_mean  # mean of "Absenteeism" hours


## 2.  Determine the number of records corresponding to
##     being absent on a Thursday (5).
thurs_absent = absent.loc[absent['Day of the week']==5]     # subsets all the rows which have a 5 (absent on Thurs)
thurs_index = thurs_absent.index                            # gets the index of of the rows
days_absent = len(thurs_index)                              # counts the index of the rows

q2 = days_absent  # days absent on Thursday


## 3.  Find the number of unique employees IDs represented in 
##     this data.  
unique1 = absent['ID'].groupby(absent['ID'])            # groups by unique ID
unique_groups = unique1.ngroups                         # counts number of ID groups

q3 = unique_groups  # number of unique employee IDs


## 4.  Find the average transportation expense for the employee 
##     with ID = 34.
unique2 = absent['Transportation expense'].groupby(absent['ID'])    # groups transportation expense by ID
mean_34 = unique2.mean().loc[34]                                    # gets mean of only ID 34

q4 = mean_34  # Average transportation expense, ID = 34


## 5.  Find the total number of hours absent for the records
##     for employee ID = 11.
unique3 = absent['Absenteeism time in hours'].groupby(absent['ID'])     # groups abseentism by ID
totalhrs_11 = unique3.sum().loc[11]                                     # gets sum of absenteeism of only ID 11


q5 = totalhrs_11  # total hours absent, ID = 11


## 6.  Find (a) the mean number of hours absent for the records of those who 
##     have no pets, then (b) do the same for those who have more than one pet.
unique4 = absent['Absenteeism time in hours'].groupby(absent['Pet'])        # groups absenteeism by Pets
nopets_mean = unique4.mean().iloc[0]                                         # mean of absenteeism for 0 pets
morepets = unique4.mean().iloc[2:,]                                    # mean of groups of pets greater than 1
morepets_mean = morepets.mean()                                        # combined mean of absent hours groups greater than 1  

q6a = nopets_mean # mean hours absent, no pet
q6b = morepets_mean # mean hours absent, more than one pet


## 7.  Among the records for absences that exceeded 8 hours, find (a) the 
##     proportion that involved smokers.  Then (b) do the same for absences 
##     of no more then 4 hours.

#------------------------ 7A ------------------------------------------------
unique5 = absent.loc[absent['Absenteeism time in hours'] > 8,:]                 # everyone who has >8 hours of absence
greater8 = unique5.index                                                        
total_greater8 = len(greater8)                                                  # number of absences >8 hrs               
unique5b = absent.loc[(absent['Absenteeism time in hours'] > 8) &               # number of social smokers who have >8hrs of absence
                      (absent['Social smoker']==1),]
smoker_greater8 = unique5b.index
total_smoker_greater8 = len(smoker_greater8)                                    # counts the number of smokers who have absences greater than 8 hrs

prop_greater8 = total_smoker_greater8/total_greater8                            # calculates proportion

#------------------------ 7B -------------------------------------------------
unique6 = absent.loc[absent['Absenteeism time in hours'] <= 4,:]                # everyone who has less than or equal to 4 hours of absence
less4 = unique6.index                                                        
total_less4 = len(less4)                                                        # number of absences <= 4 hrs               
unique6b = absent.loc[(absent['Absenteeism time in hours'] <=4) &               # number of social smokers who have <= 4hrs of absence
                      (absent['Social smoker']==1),]
smoker_less4 = unique6b.index
total_smoker_less4 = len(smoker_less4)                                          # counts the number of smokers who have absences less than or equal to 4 hrs

prop_less4 = total_smoker_less4/total_less4                                     # calculates proportion

#------------------- ANSWERS --------------------------------------------------
q7a = prop_greater8 # proportion of smokers, absence greater than 8 hours
q7b = prop_less4 # proportion of smokers, absence no more than 4 hours


## 8.  Repeat Question 7, this time for social drinkers in place of smokers.

#------------------------ 8A ------------------------------------------------
unique7 = absent.loc[absent['Absenteeism time in hours'] > 8,:]                 # everyone who has >8 hours of absence
newgreater8 = unique7.index                                                        
newtotal_greater8 = len(newgreater8)                                            # number of absences >8 hrs               
unique7b = absent.loc[(absent['Absenteeism time in hours'] > 8) &               # number of social drinkers who have >8hrs of absence
                      (absent['Social drinker']==1),]
drinker_greater8 = unique7b.index
total_drinker_greater8 = len(drinker_greater8)                                  # counts the number of drinkers who have absences greater than 8 hrs

newprop_greater8 = total_drinker_greater8/newtotal_greater8                     # calculates proportion

#------------------------ 8B -------------------------------------------------
unique8 = absent.loc[absent['Absenteeism time in hours'] <= 4,:]                # everyone who has less than or equal to 4 hours of absence
newless4 = unique8.index                                                        
newtotal_less4 = len(newless4)                                                  # number of absences <= 4 hrs               
unique8b = absent.loc[(absent['Absenteeism time in hours'] <=4) &               # number of social drinkers who have <= 4hrs of absence
                      (absent['Social drinker']==1),]
drinker_less4 = unique8b.index
total_drinker_less4 = len(drinker_less4)                                        # counts the number of drinkers who have absences less than or equal to 4 hrs

newprop_less4 = total_drinker_less4/newtotal_less4                              # calculates proportion

#------------------- ANSWERS --------------------------------------------------
q8a = newprop_greater8 # proportion of social drinkers, absence greater than 8 hours
q8b = newprop_less4    # proportion of social drinkers, absence no more than 4 hours


## 9.  Find the top-5 employee IDs in terms of total hours absent.  Give
##     the IDs and corresponding total hours absent as a Series with ID
##     for the index, sorted by the total hours absent from most to least.

unique9 = absent['Absenteeism time in hours'].groupby(absent['ID'])             # group absentee hours by ID
sorted_sum = unique9.sum().sort_values(ascending = False)                       # sort the sum of absentee hours by ID from most to least
top5_sorted = sorted_sum.nlargest(n=5)                                          # getting the top 5 values


q9 = top5_sorted  # Series of top-5 employee IDs in terms of total hours absent


## 10. Find the average hours absent per record for each day of the week.
##     Give the day number and average as a Series with the day number
##     as the index, sorted by day number from smallest to largest.

unique10 = absent['Absenteeism time in hours'].groupby(absent['Day of the week']) # group absentee hours by day of the week
dotw_mean = unique10.mean()                                                       # finding the mean of absentee hours for each day of the week 

q10 = dotw_mean  # Series of average hours absent by day of week.


## 11. Repeat Question 10 replacing day of the week with month.
##     Give the month number and average as a Series with the month number
##     as the index, sorted by month number from smallest to largest.

unique11 = absent['Absenteeism time in hours'].groupby(absent['Month of absence']) # group absentee hours by month
month_mean = unique11.mean()                                                       # finding the mean of absentee hours for each month 
month_mean = month_mean.drop(0)                                                   # 0 is not a month 

q11 = month_mean  # Series of average hours absent by month of absence.


## 12. Find the top 3 most common reasons for absence for the social smokers.
##      Give the reason code and number of occurances as a Series with the 
##      reason code as the index, sorted by number of occurances from
##      largest to smallest.  (If there is a tie for 3rd place,
##      include all that tied for that position.)

unique12 = absent.loc[absent['Social smoker'] == 1,:]                           # creates new df with only social smokers 
unique13 = absent['Social smoker'].groupby(absent['Reason for absence'])        # groups the social smokers with reason of absence
common_reasons = unique13.sum().sort_values(ascending = False)                  # sorts the number of abscenses in each category from high to low
common_reasons = common_reasons.drop(0)                                          # 0 is not a reason so it is not included
common_reasons_top3 = common_reasons.nlargest(n=3, keep='all')                  # top 3 reasons

q12 = common_reasons_top3  # Series of reason codes and counts


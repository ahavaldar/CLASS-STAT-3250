##
## File: assignment09.py (STAT 3250)
## Topic: Assignment 9 
##

##  This assignment requires the data file 'airline-stats.txt'.  This file 
##  contains thousands of records of aggregated flight information, organized 
##  by airline, airport, and month.  The first record is shown below.  
##
##  The file is quite large (1.8M lines, 31MB) so may be difficult to open in
##  Spyder.  An abbreviated version 'airline-stats-brief.txt' is also 
##  provided that has the same structure as the original data set but is
##  easier to open in Spyder.

##  Note: Some or all of the questions on this assignment can be done without the 
##  use of loops, either explicitly or implicitly (apply). As usual, scoring 
##  will take this into account.

##  The Gradescope autograder will be evaluating your code on a reduced 
##  version of the airline-stats.txt data that includes only a fraction of the
##  records.  Your code needs to automatically handle all assignments
##  to the variables q1, q2, ... to accommodate the reduced data set,
##  so do not copy/paste things from the console window, and take care
##  with hard-coding values.  

# =============================================================================
# airport
#     code: ATL 
#     name: Atlanta GA: Hartsfield-Jackson Atlanta International
# flights 
#     cancelled: 5 
#     on time: 561 
#     total: 752 
#     delayed: 186 
#     diverted: 0
# number of delays 
#     late aircraft: 18 
#     weather: 28 
#     security: 2
#     national aviation system: 105 
#     carrier: 34
# minutes delayed 
#     late aircraft: 1269 
#     weather: 1722 
#     carrier: 1367 
#     security: 139 
#     total: 8314 
#     national aviation system: 3817
# dates
#     label: 2003/6 
#     year: 2003 
#     month: 6
# carrier
#     code: AA 
#     name: American Airlines Inc.
# =============================================================================

import numpy as np # load numpy as np
import pandas as pd # load pandas as pd

# Read in the test data as text one line at a time
airlines = open('airline-stats.txt').read().splitlines()
temp = pd.Series(airlines)          # converting to series
temp = temp.replace('', float("NaN"))       # replaces empty rows with NA
temp = temp.dropna()            # drop empty rows
# cols = ['airport_code', 'airport_name', 'flights_cancelled', 'flights_ontime',
#               'flights_total', 'flights_delayed', 'flights_diverted', 
#               'numdel_late', 'numdel_weather', 'numdel_security', 'numdel_aviation', 
#               'numdel_carrier', 'mindel_late', 'mindel_weather', 'mindel_carrier', 
#               'mindel_security', 'mindel_total', 'mindel_aviation', 'label', 'year',
#               'month', 'carrier_code', 'carrier_name']
df = pd.DataFrame()             # empty dataframe
temp = temp[temp.str.startswith(' ')]       # removing headers from series
temp = temp.str.split(':')                  # splitting entries on :
lst = []                            # empty list
for i in temp:                      
    lst.append((i[-1]))             # appends the last element of each list to a new list
ser = pd.Series(lst)
ser = ser.str[1:]                   # removes empty space at the beginning

# gets every 23rd entry starting at a different point and adds it to a new dataframe
df['airport_code'] = ser.iloc[::23].reset_index(drop=True)
df['airport_name'] = ser.iloc[1::23].reset_index(drop=True)
df['flights_cancelled'] = ser.iloc[2::23].reset_index(drop=True).astype(int)
df['flights_ontime'] = ser.iloc[3::23].reset_index(drop=True).astype(int)
df['flights_total'] = ser.iloc[4::23].reset_index(drop=True).astype(int)
df['flights_delayed'] = ser.iloc[5::23].reset_index(drop=True).astype(int)
df['flights_diverted'] = ser.iloc[6::23].reset_index(drop=True).astype(int)
df['numdel_late'] = ser.iloc[7::23].reset_index(drop=True).astype(int)
df['numdel_weather'] = ser.iloc[8::23].reset_index(drop=True).astype(int)
df['numdel_security'] = ser.iloc[9::23].reset_index(drop=True).astype(int)
df['numdel_aviation'] = ser.iloc[10::23].reset_index(drop=True).astype(int)
df['numdel_carrier'] = ser.iloc[11::23].reset_index(drop=True).astype(int)
df['mindel_late'] = ser.iloc[12::23].reset_index(drop=True).astype(int)
df['mindel_weather'] = ser.iloc[13::23].reset_index(drop=True).astype(int)
df['mindel_carrier'] = ser.iloc[14::23].reset_index(drop=True).astype(int)
df['mindel_security'] = ser.iloc[15::23].reset_index(drop=True).astype(int)
df['mindel_total'] = ser.iloc[16::23].reset_index(drop=True).astype(int)
df['mindel_aviation'] = ser.iloc[17::23].reset_index(drop=True).astype(int)
df['label'] = ser.iloc[18::23].reset_index(drop=True)
df['year'] = ser.iloc[19::23].reset_index(drop=True).astype(int)
df['month'] = ser.iloc[20::23].reset_index(drop=True).astype(int)
df['carrier_code'] = ser.iloc[21::23].reset_index(drop=True)
df['carrier_name'] = ser.iloc[22::23].reset_index(drop=True)


## 1.  Give the total number of hours delayed for all flights in all records,
##     based on the entries in (minutes delayed)/total
hrs_del = np.sum(df['mindel_total'])            # summing min delayed 
hrs_del = hrs_del / 60                  # dividing by 60 to get the hours


q1 = hrs_del  # total number of hours delayed for all flights in all records


## 2.  Which airlines appear in at least 500 records?  Give a Series with airline
##     names as index and record counts for values, in order of record count 
##     from largest to smallest.
q2gb = df['carrier_name'].groupby(df['carrier_name'])       # group by carrier name
q2_ct = q2gb.count()                # count the number of records each airline appears
q2_ct = q2_ct[q2_ct >= 500].sort_values(ascending=False)        # getting values 500+ and sorting


q2 = q2_ct  # Series of airline names and record counts
   

## 3.  The entry under 'flights/delayed' is not always the same as the total
##     of the entries under 'number of delays'.  (The reason for this is not
##     clear.)  Determine the percentage of records for which these two
##     values are different.
df['numdel_total'] = df.iloc[:,7:12].sum(axis=1)        # summing across number of delays
q3df = df[(df['flights_delayed']) != (df['numdel_total'])]      # subsetting rows where the 2 values are not equal
len_df = len(df)
len_del = len(q3df)
q3_perc = (len_del / len_df) *100           # calculating percentage


q3 = q3_perc  # percentage of records with two values different


## 4.  Determine the percentage of records for which the number of delays due to
##     'late aircraft' exceeds the number of delays due to 'carrier'.
q4df = df[(df['numdel_late']) > (df['numdel_carrier'])]     # subsetting rows where late aircraft exceeds carrier
len_q4 = len(q4df)
q4_perc = (len_q4 / len_df) *100                # calculating percentage

q4 = q4_perc  # percentage of records as described above


## 5.  Find the top-8 airports in terms of the total number of minutes delayed.
##     Give a Series with the airport names (not codes) as index and the total 
##     minutes delayed as values, sorted order from largest to smallest total.
##     (Include any ties for 8th position as usual)
q5gb = df['mindel_total'].groupby(df['airport_name'])           # group minutes delayed by airport 
q5_tot = q5gb.sum().sort_values(ascending=False).nlargest(n=8)      # getting total number of min delayed by airline and sorting

q5 = q5_tot  # Series of airport names and total minutes delayed


## 6.  Find the top-12 airports in terms of rates (as percentages) of on-time flights.
##     Give a Series of the airport names (not codes) as index and percentages
##     as values, sorted from largest to smallest percentage. (Include any
##     ties for 12th position as usual)
q6gb = df[['flights_total', 'flights_ontime']].groupby(df['airport_name'])      # grouping total and ontime flights by airport
q6_rate = q6gb.sum()            # getting sum of total and ontime flights 
q6_rate['rate']  = (q6_rate['flights_ontime'] / q6_rate['flights_total']) *100      # calculating percentage of ontime flights for each airport
q6_rate = q6_rate.drop(['flights_ontime', 'flights_total'], axis=1)     # dropping unnecessary columns
q6_rate = q6_rate.squeeze()                     # converting to series
q6_rate = q6_rate.sort_values(ascending=False).nlargest(n=12)       # sorting 


q6 = q6_rate  # Series of airport names and percentages 


## 7.  Find the top-10 airlines in terms of rates (as percentages) of on time flights.
##     Give a Series of the airline names (not codes) as index and percentages  
##     as values, sorted from largest to smallest percentage. (Include any
##     ties for 10th position as usual)
q7gb = df[['flights_total', 'flights_ontime']].groupby(df['carrier_name'])      # grouping total and ontime flights by airline
q7_rate = q7gb.sum()            # getting sum of total and ontime flights 
q7_rate['rate']  = (q7_rate['flights_ontime'] / q7_rate['flights_total']) *100      # calculating percentage of ontime flights for each airline
q7_rate = q7_rate.drop(['flights_ontime', 'flights_total'], axis=1)     # dropping unnecessary columns
q7_rate = q7_rate.squeeze()                     # converting to series
q7_rate = q7_rate.sort_values(ascending=False).nlargest(n=10)       # sorting 


q7 = q7_rate  # Series of airline names and percentages


## 8.  Determine the average length (in minutes) by airline of a delay due
##     to the national aviation system.  Give a Series of airline name (not 
##     code) as index and average delay lengths as values, sorted from largest 
##     to smallest average delay length.
q8gb = df['mindel_aviation'].groupby(df['carrier_name'])        # grouping aviation system delay in minutes by airline
q8_avg = q8gb.mean().sort_values(ascending=False)           # getting avg delay due to aviation by airline

q8 = q8_avg  # Series of airline names and average delay times


## 9.  For each month, determine the rates (as percentages) of flights delayed 
##     by weather. Give a Series sorted by month (1, 2, ..., 12) with the 
##     corresponding percentages as values.
q9gb = df[['numdel_weather', 'flights_total']].groupby(df['month'])             # grouping weather delays and total delays by month
q9_rate = q9gb.sum()                        # getting sum of weather delays and total delays
q9_rate['rate'] = (q9_rate['numdel_weather'] / q9_rate['flights_total']) *100       # calculating rates
q9_rate = q9_rate.drop(['numdel_weather', 'flights_total'], axis=1)         # dropping unnecessary columns
q9_rate = q9_rate.squeeze()             # converting to series
q9_rate = q9_rate.sort_index(ascending=True)            # sorting by increasing index


q9 = q9_rate  # Series of months and percentages


## 10. Find all airports where the average length (in minutes) of 
##     security-related flight delays exceeds 35 minutes.  Give a Series with  
##     airport names (not codes) as index and average delay times as values, 
##     sorted from largest to smallest average delay.
q10gb = df['mindel_security'].groupby(df['airport_name'])       # grouping minutes delayed by security by airport
q10_avg = q10gb.mean()              # getting average length of delays
q10_avg = q10_avg[q10_avg > 35].sort_values(ascending=False)        # subsetting airports with delay>35 and sorting

q10 = q10_avg  # Series or airport names and average delay times


## 11. For each year, determine the airport that had the highest rate (as a 
##     percentage) of delays.  Give a Series with the years (least recent at top)  
##     and airport names (not code) as MultiIndex and the percentages as values.
q11gb = df[['flights_delayed', 'flights_total']].groupby([df['year'], df['airport_name']])      # grouping flights total and delayed by year and airport name
q11_rate = q11gb.sum()              # calculating sum of delayed and total flights
q11_rate['rate'] = (q11_rate['flights_delayed'] / q11_rate['flights_total']) *100       # calculating percentage
q11_rate = q11_rate.drop(['flights_delayed', 'flights_total'], axis=1)      # dropping unnecessary columns
q11_rate = q11_rate.squeeze()           # converting to series
q11_rate = q11_rate.groupby('year', group_keys=False).nlargest(1).sort_index(ascending=True)        # getting largest value of rate for each year


q11 = q11_rate  # Series of years/airport names and percentages


## 12. For each airline, determine the airport where that airline had its 
##     greatest percentage of delayed flights.  Give a Series with airline
##     names (not code) and airport names (not code) as MultiIndex and the
##     percentage of delayed flights as values, sorted from smallest to
##     largest percentage.
q12gb = df[['flights_delayed', 'flights_total']].groupby([df['carrier_name'], df['airport_name']])      # grouping flights total and delayed by airline and airport name
q12_rate = q12gb.sum()              # calculating sum of delayed and total flights
q12_rate['rate'] = (q12_rate['flights_delayed'] / q12_rate['flights_total']) *100       # calculating percentage
q12_rate = q12_rate.drop(['flights_delayed', 'flights_total'], axis=1)      # dropping unnecessary columns
q12_rate = q12_rate.squeeze()           # converting to series
q12_rate = q12_rate.groupby('carrier_name', group_keys=False).nlargest(1).sort_values(ascending=True)        # getting largest value of rate for each airline


q12 = q12_rate  # Series of airline/airport and percentages


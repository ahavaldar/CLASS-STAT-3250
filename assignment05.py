##
## File: assignment05.py (STAT 3250)
## Topic: Assignment 5 
##

##  This assignment requires the data file 'diabetic_data.csv'.  This file
##  contains records for over 100,000 hospitalizations for people who have
##  diabetes.  The file 'diabetic_info.pdf' contains information on the
##  codes used for a few of the entries.  Missing values are indicated by
##  a '?'.  You should be able to read in this file using the usual 
##  pandas methods.

##  The Gradescope autograder will be evaluating your code on a reduced 
##  version of the diabetic_data.csv data that includes about 35% of the
##  records.  Your code needs to automatically handle all assignments
##  to the variables q1, q2, ... to accommodate the reduced data set,
##  so do not copy/paste things from the console window, and take care
##  with hard-coding values. 

##  Note: Many of the questions on this assignment can be done without the 
##  use of loops, either explicitly or implicitly (apply). Scoring will take
##  this into account.

import pandas as pd # load pandas as pd
import numpy as np

dia = pd.read_csv('diabetic_data.csv')
dia1 = dia.loc[11:20,:]

## 1.  Determine the average number of procedures ('num_procedures') for 
##     those classified as females and for those classified as males.

proc_grby = dia['num_procedures'].groupby(dia['gender'])        # group num_procedures by gender
fem_avg = proc_grby.mean().loc['Female']                        # gets mean for females
male_avg = proc_grby.mean().loc['Male']                         # gets mean for males

q1f = fem_avg  # female average number of procedures
q1m = male_avg  # male average number of procedures


## 2.  Determine the average length of hospital stay ('time_in_hospital')
##     for each race classification.  (Omit those unknown '?' but include 
##     those classified as 'Other'.)  Give your answer as a Series with
##     race for the index sorted alphabetically.
race_gb = dia['time_in_hospital'].groupby([dia['race']])        # group time in hospital by race
race_mean = race_gb.mean().drop('?')                            # find mean of each race and drop "?"

q2 = race_mean  # Series of average length of stay by race


## 3.  Determine the percentage of total days spent in the hospital due to
##     stays ("time_in_hospital") of at least 7 days. (Do not include the %
##     symbol in your answer.)

days7 = dia.loc[dia['time_in_hospital'] >= 7,:]                    # all observations with 7+ days in hospital
len_days7 = len(days7.index)
len_total = len(dia.index)                                         # all observations 

perc_days = (len_days7/len_total)*100

q3 = perc_days  # percentage of days from stays of at least 7 days


## 4.  Among the patients in this data set, what percentage had at least
##     three recorded hospital visits?  Each distinct record can be assumed 
##     to be for a separate hospital visit. Do not include the % symbol in
##     your answer.
pat3 = dia['patient_nbr'].groupby(dia['patient_nbr'])           # group by the patient number
pat_total = pat3.count().sort_values(ascending=False)           # counts the number of time each patient visited the hospital
len_pat_total = len(pat_total.index)                            # counts the number of patients

pat3_total = pat_total[pat_total >=3]               # gets observations greater than 3
len_pat3_total = len(pat3_total.index)              # counts observations

pat_perc = (len_pat3_total/len_pat_total)*100       # calculates percentage

q4 = pat_perc  # percentage patients with at least three visits


## 5.  List the top-15 most common diagnoses, based on the codes listed 
##     collectively in the columns 'diag_1', 'diag_2', and 'diag_3'.
##     Give your response as a Series with the diagnosis code as the 
##     index and the number of occurances as the values, sorted by
##     values from largest to smallest.  If more than one value could
##     go in the 15th position, include all that could go in that 
##     position.  (This is the usual "include ties" policy.)

d1 = dia['diag_1'].groupby(dia['diag_1'])               # group by first diag
d2 = dia['diag_2'].groupby(dia['diag_2'])               # group by second diag
d3 = dia['diag_3'].groupby(dia['diag_3'])               # group by third diag

d1_ct = d1.count()                  # count of each entry
d2_ct = d2.count()
d3_ct = d3.count()

dcom_ct = d1_ct + d2_ct + d3_ct        # summation of counts 
dcom_ct = dcom_ct.sort_values(ascending=False)  # sort ascending
dcom_top15 = dcom_ct.nlargest(n=15, keep='all')                  # top 15 reasons


q5 = dcom_top15  # top-15 diagnoses plus any ties


## 6.  The 'age' in each record is given as a 10-year range of ages.  Assume
##     that the age for a person is the middle of the range.  (For instance,
##     those with 'age' [40,50) are assumed to be 45.)  Determine the average
##     age for each classification in the column 'acarbose'.  Give your
##     answer as a Series with the classification as index and averages as
##     values, sorted from largest to smallest average.

q6_age = dia[['age','acarbose']]
q6_age = q6_age[['age','acarbose']].replace({'[0-10)':5, '[10-20)':15, '[20-30)':25, 
                              '[30-40)':35, '[40-50)':45, '[50-60)':55,
                              '[60-70)':65, '[70-80)':75, '[80-90)':85,
                              '[90-100)':95})                               # replace age ranges with actual values

q6_gb = q6_age['age'].groupby(q6_age['acarbose'])           # group age by acarbose
q6_mean = q6_gb.mean().sort_values(ascending=False)         # mean of each group of acarbose

q6 = q6_mean  # Series of classifications and averages


## 7.  Determine all medical specialties that have an average hospital stay
##     (based on time_in_hospital) of at least 7 days.  Give a Series with
##     specialty as index and average hospital stay as values, sorted from
##     largest to smallest average stay.
q7_gb = dia['time_in_hospital'].groupby(dia['medical_specialty'])   # groups time in hospital by medical speciality
q7_mean = q7_gb.mean().sort_values(ascending=False)                 # takes mean and sorts values
q7_mean = q7_mean[q7_mean >= 7]                                     # extracts values greater than or equal to 7

q7 = q7_mean  # Series of specialities and average stays


##  8. Three medications for type 2 diabetes are 'glipizide', 'glimepiride',
##     and 'glyburide'.  There are columns in the data for each of these.
##     Determine the number of records for which at least two of these
##     are listed as 'Steady'.
q8_df = dia[['glipizide','glimepiride','glyburide']]            # subset the columns
q8_df['new'] = q8_df['glipizide']+q8_df['glimepiride']+q8_df['glyburide']       # concat the steady
q8_df = q8_df['new']                                                            # subset the new column
    
z = q8_df.str.count('Steady')                                   # counts how many times "steady" appears in the string
steady = z[z>=2]                                    # get all values greater than 2

steady_count = len(steady.index)

q8 = steady_count  # number of records with at least two 'Steady'


##  9. Find the percentage of "time_in_hospital" accounted for by the top-100 
##     patients in terms of number of times in file.  (Include all patients 
##     that tie the 100th patient.)

total_sum = np.sum(dia['time_in_hospital'])          # total number of hours
q9_gb = dia['time_in_hospital'].groupby(dia['patient_nbr'])     # group time in hospital by patient nbr
q9_top100 = q9_gb.sum().sort_values(ascending=False)            # sum of each patient's total hours
q9_top100 = q9_top100.nlargest(n=100, keep='all')                  # top 100 reasons

top100_sum = np.sum(q9_top100)                          # sum of top100 patient's hours
q9_perc = (top100_sum/total_sum)*100                    # calculating percentage

q9 = q9_perc  # Percentage of time from top-100 patients


## 10. What percentage of reasons for admission ('admission_source_id')
##     correspond to some form of transfer from another care source?
## 4, 5, 6, 10, 18, 22, 25, 26
q10_df = dia[['admission_source_id']]           # subsetting admission source id
q10_newdf =  q10_df[(q10_df["admission_source_id"]==4) | (q10_df["admission_source_id"]==5)|
                   (q10_df["admission_source_id"]==6) |(q10_df["admission_source_id"]==10) |
                   (q10_df["admission_source_id"]==18) |(q10_df["admission_source_id"]==22) |
                   (q10_df["admission_source_id"]==25) |(q10_df["admission_source_id"]==26)]            # subsetting out all reasons for transfer


q10_perc = (len(q10_newdf)/len(q10_df))*100     # calculating percentage

q10 = q10_perc  # Percentage of admission by transfer


## 11. The column 'discharge_disposition_id' gives codes for discharges.
##     Determine the 5 codes that resulted in the greatest percentage of
##     readmissions.  Give your answer as a Series with discharge code
##     as index and readmission percentage as value, sorted by percentage
##     from largest to smallest.

q11_df = dia[['discharge_disposition_id', 'readmitted']]
q11_df = q11_df[['discharge_disposition_id','readmitted']].replace({">30": "YES",
                                                                   "<30":"YES"})      # replacing unique readmitted values with "yes"
q11_df['total'] = q11_df['readmitted']
q11_df = q11_df.replace({'readmitted': {"YES": 1,"NO":0}})      # changing YES/NO values to 1 or 0 
q11_df = q11_df.replace({'total':{"NO":1, "YES":1}})               # making all values 1 to get the total number of observations

q11_gb = q11_df[['readmitted','total']].groupby(q11_df['discharge_disposition_id'])        # group by discharge disposition id
q11_sum = q11_gb.sum()                  # getting sum of each category
q11_perc = (q11_sum['readmitted']/q11_sum['total'])*100         # percentage of readmitted

q11_perc = q11_perc.nlargest(n=5, keep='all').sort_values(ascending=False)      # get top 5 and sorted from largest to smallest


q11 = q11_perc  # Series of discharge codes and readmission percentages


## 12. The columns from 'metformin' to 'citoglipton' are all medications, 
##     with "Up", "Down", and "Steady" indicating the patient is taking that 
##     medication.  For each of these medications, determine the average
##     number of medications from this group that patients are taking.
##     Give a Series of all medications with an average of at least 1.5,
##     with the medications as index and averages as values, sorted 
##     largest to smallest average.
##     (Hint: df.columns gives the column names of the data frame df.)

q12_df = dia.loc[:,'metformin':'citoglipton']               # subsetting the columns	
q12_df = q12_df.replace(["Up", "Down","Steady"], "Yes")     # replacing all values of usage with "yes"

q12_df = q12_df.replace(['Yes'], 1)     # replace yes with 1
q12_df = q12_df.replace(['No'], 0)      # replace no with 0

lst = []
for i in range(q12_df.shape[1]):
    df2 = q12_df.loc[q12_df.iloc[:,i] == 1]                # subset all rows where the column=1
    df3 = df2.sum(axis=1, skipna=True)                           # then sum over the rows
    x = df3.mean(axis=0, skipna=True)                            # then mean the column
    lst.append(x)                                   # then append that value to list

q12_df.columns                   # getting the names of columns to add to series
ser = pd.Series(lst, index = ['metformin', 'repaglinide', 'nateglinide', 'chlorpropamide',
       'glimepiride', 'acetohexamide', 'glipizide', 'glyburide', 'tolbutamide',
       'pioglitazone', 'rosiglitazone', 'acarbose', 'miglitol', 'troglitazone',
       'tolazamide', 'examide', 'citoglipton'])             # creating a series with drug names as index
ser = ser[ser>=1.5].sort_values(ascending=False)        # getting only values with 1.5 or higher and sorting them from large to small

q12 = ser  # Series of medications and averages

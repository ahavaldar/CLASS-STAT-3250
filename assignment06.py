##
## File: assignment06.py (STAT 3250)
## Topic: Assignment 6
##

##  The questions this assignment are based on "timing_log.txt".
##  The file "timing_log.txt" contains the set of all WeBWorK
##  log entries on April 1, 2011.  The entries are organized by
##  one log entry per line, with each line including the following:
##
##  --the date and time of the entry
##  --a number that is related to the user (but is not unique)
##  --something that appears to be the epoch time stamp
##  --a hyphen
##  --the "WeBWorK element" that was accessed
##  --the "runTime" required to process the problem
##
##  Note: Some or all of the questions on this assignment can be done without the 
##  use of loops, either explicitly or implicitly (apply). As usual, scoring 
##  will take this into account.

##  The Gradescope autograder will be evaluating your code on a reduced 
##  version of the timing_log.txt data that includes only a fraction of the
##  records.  Your code needs to automatically handle all assignments
##  to the variables q1, q2, ... to accommodate the reduced data set,
##  so do not copy/paste things from the console window, and take care
##  with hard-coding values. 


## Load pandas and read in the data set
import pandas as pd # load pandas as pd

loglines = open('timing_log.txt').read().splitlines()


## 1.  How many log entries were for requests for a PDF version of an
##     assignment?  Those are indicated by "hardcopy" appearing in the 
##     WeBWorK element.
ser = pd.Series(loglines)                   # convert to series
hardcopy = ser[ser.str.contains('hardcopy')]    # pull out all entries that contain "hardcopy"
q1_count = len(hardcopy)                        # get the number of entries

q1 = q1_count # number of log entries requesting a PDF


## 2.  What percentage of log entries involved a Spring '12 section of MATH 1320?
spring12 = ser[ser.str.contains('Spring12-MATH1320')]
q2_perc = (len(spring12)/len(ser))*100

q2 = q2_perc  # percentage of log entries, Spring '12 MATH 1320


## 3. How many different classes use the system? Treat each different name 
##    as a different class, even if there is more than one section of a course.  
course = ser.str.split(' ').str[8].str.split('/').str[2].str.replace(']', '')       # subsets only the class term
course_ct = len(course.unique())                                    # counts the unique number of classes

q3 = course_ct  # number of different classes using the system
                        

## 4.  Find the percentage of log entries that came from an initial
##     log in.  For those, the WeBWorK element has the form
##
##          [/webwork2/ClassName] or [/webwork2/ClassName/]
##
##     where "ClassName" is the name of a class.  
 
initial = ser.str.split(' ').str[8].str[2:-1]    # subsetting each string so that if the len=3 or 4 we know it is an initial entry
initial = initial.str.strip("/").str.split('/')  # strips and splits on /
new = initial.str.len()                          # getting length of string   
new = new[new==2]                                # finding all with length =2

q4_perc = (len(new)/len(ser))*100                # calculating percentage

q4 = q4_perc  # percentage of log entries from initial log in


## 5.  Determine the percentage of log entries for each section of MATH 1310
##     from Spring 2012, among the total number of log entries for MATH 1310,
##     Spring 2012.  Give the percentages as a Series with class name as
##     index and percentages as values, sorted by percentage largest to smallest.
##     (The class name should be of the form 'Spring12-MATH1310-InstructorName')
math = ser[ser.str.contains('Spring12-MATH1310')]           # subset out Spring '12 Math 1310
math2= math.str.split('-').str[3].str.split().str[0].str.split('/').str[0].str.replace(']','')  # gets only the intstructor name
math_total = ((math2.value_counts()/len(math))*100).sort_values(ascending=False)       # get percentage


q5 = math_total  # Series of MATH 1310 sections and percentage within MATH 1310


## 6.  How many log entries were from instructors performing administrative
##     tasks?  Those are indicated by "instructor" alone in the 3rd position of
##     the WeBWorK element.  
inst = ser[ser.str.contains('instructor')]          # subsetting out entries with instructor
newinst = inst.str.split('[').str[2].str.split('/').str[3]          # subsetting based on only instructor only in 3rd position
newinst = newinst[newinst==('instructor')]                      # getting only entries with instructor alone


q6 = len(newinst)  # number of instructor administrative log entries

## 7.  Find the number of log entries for each hour of the day. Give the
##     counts for the top-5 (plus ties as usual) as a Series, with hour of day
##     as index and the count as values, sorted by count from largest to 
##     smallest.
hrs = ser.str.split(']').str[0].str.split(' ').str[3].str.split(':').str[0]         # gets only the hour term
hrs_count = hrs.value_counts().sort_values(ascending=False).nlargest(n=5, keep='all')           # counts the values for hours

q7 = hrs_count  # Series of entry count by hour, top 5


## 8.  Find the number of log entries for each minute of each hour of the day. 
##     Give the counts for the top-8 (plus ties as usual)s as a Series, with 
##     hour:minute pairs as index and the count as values, sorted by count 
##     from largest to smallest.  (An example of a possible index entry
##     is 15:47)
minute = ser.str.split(']').str[0].str.split(' ').str[3].str[:-3]               # getting only hour and minute values
min_count = minute.value_counts().sort_values(ascending=False).nlargest(n=8, keep='all')        # sorting values and getting top 8 with ties

q8 = min_count  # Series of counts by hour:minute, top-8 plus ties


## 9. Determine which 5 classes had the largest average "runTime".  Give a 
##    Series of the classes and their average runTime, with class as index
##    and average runTime as value, sorted by value from largest to smallest.
df = pd.DataFrame()             # initialize empty dataframe
df['Class'] = ser.str.split(' ').str[8].str.split('/').str[2].str.replace(']', '')      # adding the class to the df
df['Time'] = ser.str.split(' ').str[11]                         # adding the runtime for each class to the df
df['Time'] = pd.to_numeric(df['Time'])                          # converting the time from character to numeric
q9_gb = df['Time'].groupby(df['Class'])                         # group runtime by class
runtime_mean = q9_gb.mean().sort_values(ascending=False).nlargest(n=5, keep='all')          # taking the average for each class and getting the top 5 values

q9 = runtime_mean  # Series of classes and average runTimes


## 10. Determine the percentage of log entries that were accessing a problem.  
##     For those, the WeBWorK element has the form
##
##           [/webwork2/ClassName/AssignmentName/Digit]
##     or
##           [/webwork2/ClassName/AssignmentName/Digit/]
##
##     where "ClassName" is the name of the class, "AssignmentName" the
##     name of the assignment, and "Digit" is a positive digit.

initial_new = ser.str.split(' ').str[8].str[2:-1]    # subsetting each string
initial_new = initial_new.str.strip("/").str.split('/').str[-1]     # splitting and getting the last element
initial_digit = initial_new.str.isdigit()                           # chekcing for digit
initial_ct = len(initial_digit[initial_digit==True])                # getting the count of all digits

q10_perc = (initial_ct/len(ser))*100                # calculating percentage

q10 = q10_perc  # percentage of log entries accessing a problem
 

## 11. Find the top-10 (plus tied) WeBWorK problems that had the most log entries,
##     and the number of entries for each (plus ties as usual).  Sort the 
##     table from largest to smallest.
##     (Note: The same problem number from different assignments and/or
##     different classes represent different WeBWorK problems.) 
##     Give your answer as a Series with index entries of the form
##
##          ClassName/AssignmentName/Digit
##
##     and counts for values, sorted by counts from largest to smallest.

q11_ser = ser.str.split(' ').str[8].str[11:-2]        # subsetting each
temp = q11_ser[q11_ser.str.count('/')==2]               # must have 2 slashes
temp2 = temp[temp.str.split('/').str[-1].str.isdigit()]     # checking if the last term is a digit

q11_gb = temp2.groupby(temp2)
q11_ct = q11_gb.count().sort_values(ascending=False).nlargest(n=10, keep='all')     # getting count of top 10 values


q11 = q11_ct  # Series of problems and counts


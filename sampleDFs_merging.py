##
## Topic: Merging data frames and sample questions
##

import pandas as pd

#### Creating a sample dataframe from lists and merging

# Define two lists (these could also be Series, both will work)
student = ['Jim','Jane','Bob','Ann']
major = ['Statistics','Math','Math','Statistics']

majorsdf = pd.DataFrame()  # Initialize an empty data frame
majorsdf['Name'] = student # Create a column of student names
majorsdf['Major'] = major  # Create a column of student majors
print(majorsdf)  # Check data frame

# Let's define another dataframe, this one includes the student name, a list
# of their courses, the credits, and the grade point equivalent.
student = ['Jim','Jim','Jim','Jane','Jane','Jane','Jane','Bob','Bob','Bob',
           'Bob','Ann','Ann']
coursemne = ['STAT','STAT','MATH','MATH','STAT','MATH','MATH','MATH','MATH',
             'CS','STAT','CS','MATH']
coursenum = ['3250','4630','2310','3351','3250','4110','3354','2310','3354',
             '2150','3250','2110','3351']
credits = [3,3,4,3,3,3,3,4,3,3,3,3,3]
grade = [4.0,3.3,2.7,3.7,3.7,3.3,2.3,3.3,3.7,4.0,4.0,3.7,3.3]

gradesdf = pd.DataFrame()   # Initialize a new data frame, then add columns
gradesdf['Name'] = student
gradesdf['Mnemonic'] = coursemne
gradesdf['Number'] = coursenum
gradesdf['Credits'] = credits
gradesdf['Grade'] = grade
print(gradesdf)  # check this dataframe

# Now we merge together the two data frames into one, as follows.
df = pd.merge(majorsdf, gradesdf, on = 'Name') # merge the dataframes
print(df)


## Sample Question 1: Find the average grade for each major.

# With our dataframe, this question is ready-made for groupby:
group1 = df['Grade'].groupby(df['Major'])
group1.mean()


## Sample Question 2: Determine the GPA for each student

# This is trickier because now the credits come into play.  We start by 
# creating a new column 'GradePts' that is the product of the 'Credits'
# and 'Grade" columns so that we can 

df['GradePts'] = df['Credits']*df['Grade']
print(df)

# For each student we need the total number of credits and the sum of
# the grade points.
group2 = df[['Credits','GradePts']].groupby(df['Name'])
sums = group2.sum()  # A table of sums
print(sums)

sums['GradePts']/sums['Credits']  # The quotients which gives GPAs


## Sample Question 3: Find the highest average grade by course for each
##                    department.

# We start by grouping the 'Grade' column values on 'Mnemonic' and 'Number'.
group3 = df['Grade'].groupby([df['Mnemonic'],df['Number']])
means = group3.mean()  # Compute the means for each group
means

# The code below will do the trick, a different application of groupby
# than usual
means.groupby('Mnemonic', group_keys=False).nlargest(1)


## Sample Question 4: Find a list of the distinct courses taken by these 
##                    students

# One approach is to use the 'unique' function on the course numbers.
df['Number'].unique()

# The above isn't quite right, because the course mnemonic is not included.
# Will 'unique work on two columns of a data frame?
df[['Mnemonic','Number']].unique()  # nope....

# For this situation the function 'drop_duplicates' is the answer.
df[['Mnemonic','Number']].drop_duplicates()



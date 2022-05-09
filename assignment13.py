##
## File: assignment13.py (STAT 3250)
## Topic: Assignment 13 
##


##  These questions are similar to reviewed lecture material, but 
##  provide some experience with Dask.

import dask.dataframe as dd #import libraries
import pandas as pd
import numpy as np

dtypes = {
 'Date First Observed': str, 'Days Parking In Effect    ': str,
 'Double Parking Violation': str, 'Feet From Curb': np.float32,
 'From Hours In Effect': str, 'House Number': str,
 'Hydrant Violation': str, 'Intersecting Street': str,
 'Issue Date': str, 'Issuer Code': np.float32,
 'Issuer Command': str, 'Issuer Precinct': np.float32,
 'Issuer Squad': str, 'Issuing Agency': str,
 'Law Section': np.float32, 'Meter Number': str,
 'No Standing or Stopping Violation': str,
 'Plate ID': str, 'Plate Type': str,
 'Registration State': str, 'Street Code1': np.uint32,
 'Street Code2': np.uint32, 'Street Code3': np.uint32,
 'Street Name': str, 'Sub Division': str,
 'Summons Number': np.uint32, 'Time First Observed': str,
 'To Hours In Effect': str, 'Unregistered Vehicle?': str,
 'Vehicle Body Type': str, 'Vehicle Color': str,
 'Vehicle Expiration Date': str, 'Vehicle Make': str,
 'Vehicle Year': np.float32, 'Violation Code': np.uint16,
 'Violation County': str, 'Violation Description': str,
 'Violation In Front Of Or Opposite': str, 'Violation Legal Code': str,
 'Violation Location': str, 'Violation Post Code': str,
 'Violation Precinct': np.float32, 'Violation Time': str
}


# CHANGE TO DD
nyc = dd.read_csv('nyc-parking-tickets2015.csv', dtype=dtypes, usecols=dtypes.keys())

## 1.  There are several missing values in the 'Vehicle Body Type' column. Impute 
##     missing values of 'Vehicle Body Type' with the mode. What is the mode?
mode = nyc['Vehicle Body Type'].value_counts().compute()        # getting value counts
mode = mode.sort_values(ascending=False).index[0]               # sorting
nyc = nyc.fillna({'Vehicle Body Type': mode})                   # filling NAs

q1 = mode # Report the mode, the most common Vehicle Body Type.


## 2.  How many missing data points are there in the 'Intersecting Street' column?
miss = nyc['Intersecting Street'].isnull().sum().compute()          # count sum of NAs

q2 = miss # Number of missing data points


## 3.  What percentage of vehicle makes are Jeeps during the months of March - 
##     September (inclusive) of 2015?
nyc['Issue Date'] = dd.to_datetime(nyc['Issue Date'])     # converting to date time
temp = nyc[['Issue Date', 'Vehicle Make']]              # subsetting
temp = temp[(temp['Issue Date'] <= '2015-09') & (temp['Issue Date'] >= '2015-03')]      # subsetting range
jeep = temp[temp['Vehicle Make']=='JEEP']     # number of jeeps
q3perc = ((jeep.index.size / temp.index.size) *100).compute()              # calculating percentage

q3 = q3perc # Percentage of Jeeps


## 4.  What's the most common color of a car in 2015? Maintain the color in all caps.
temp2 = nyc[nyc['Issue Date'].dt.year == 2015]              # subsetting on year
color = temp2['Vehicle Color'].value_counts().compute()       # getting mode for color 
color = color.sort_values(ascending=False).index[0]


q4 = color # Most common car color


## 5.  Find all the cars in any year that are the same color as q4. What percentage of 
##     those care are sedans?
temp3 = nyc[nyc['Vehicle Color']==color]            # subsetting by color
sedan = temp3[temp3['Vehicle Body Type']=='SDN']        # subset length
q5perc = ((sedan.index.size/temp3.index.size) *100).compute()       # calculating perc

q5 = q5perc # Percentage of sedans


## 6.  Make a table of the top 5 registration states, sorted greatest to least.
states = nyc['Registration State'].value_counts().compute()         # value counts of the state
states = states.nlargest(5, keep='all').sort_values(ascending=False)            # getting top 5 and sorting

q6 = states # Series of top 5 registration states


## 7.  Perhaps someone bought a new vehicle and kept the same license plate. How many license 
##     plates have more than one 'Vehicle Make' associated with the respective plate?
plate = nyc.drop_duplicates(subset=['Vehicle Make', 'Plate ID'])
plate = plate['Vehicle Make'].groupby(plate['Plate ID']).count().compute()          # getting unique values for vehicle make based on plate id
plate = plate[plate>1]              # subsetting
plate_ct = plate.index.size               # getting the count

q7 = plate_ct # Number of license plates


## 8.  Determine the top three hours that result in the most parking violations. 
##     "0011A" would be 12:11 AM and "0318P" would be 3:18 PM. Report the solution 
##     with the index in the format of "01A" and the count.
temp4 = nyc[['Violation Time']]
temp4['Violation Time'] = temp4['Violation Time'].str[:2] + temp4['Violation Time'].str[-1]                # combining hour with am or pm
viol = temp4['Violation Time'].value_counts().compute()                         # value counts      
viol = viol.nlargest(3).sort_values(ascending=False)        # getting top 3 and sorting

q8 = viol # Series with top three hours


## 9.  Among the tickets issued by Precinct 99, what is the average distance from the
##     curb in feet?
curb = nyc[nyc['Issuer Precinct']==99]          # subsetting by precinct
curb_avg = curb['Feet From Curb'].mean().compute()        # getting mean


q9 = curb_avg # Average distance from the curb

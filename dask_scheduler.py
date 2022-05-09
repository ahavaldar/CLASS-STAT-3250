#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  2 15:05:25 2022

@author: taylor
"""
import numpy as np
import pandas as pd
import dask.dataframe as dd
import psutil
import sys
import os
import dask
from dask.distributed import Client


# be careful with these schedulers!
# gradescope only likes the default/threads
#dask.config.set(scheduler='threads')  # overwrite default with threaded scheduler
#dask.config.set(scheduler='processes')  # overwrite default with threaded scheduler
client = Client()


dtypes = {
 'Date First Observed': str,
 'Days Parking In Effect    ': str,
 'Double Parking Violation': str,
 'Feet From Curb': np.float64,
 'From Hours In Effect': str,
 'House Number': str,
 'Hydrant Violation': str,
 'Intersecting Street': str,
 'Issue Date': str,
 'Issuer Code': np.float64,
 'Issuer Command': str,
 'Issuer Precinct': np.float64,
 'Issuer Squad': str,
 'Issuing Agency': str,
 'Law Section': np.float64,
 'Meter Number': str,
 'No Standing or Stopping Violation': str,
 'Plate ID': str,
 'Plate Type': str,
 'Registration State': str,
 'Street Code1': str,
 'Street Code2': str,
 'Street Code3': str,
 'Street Name': str,
 'Sub Division': str,
 'Summons Number': np.uint64,
 'Time First Observed': str,
 'To Hours In Effect': str,
 'Unregistered Vehicle?': str,
 'Vehicle Body Type': str,
 'Vehicle Color': str,
 'Vehicle Expiration Date': str,
 'Vehicle Make': str,
 'Vehicle Year': np.float64,
 'Violation Code': str,
 'Violation County': str,
 'Violation Description': str,
 'Violation In Front Of Or Opposite': str,
 'Violation Legal Code': str,
 'Violation Location': str,
 'Violation Post Code': str,
 'Violation Precinct': np.float64,
 'Violation Time': str
}


nyc_data_raw = dd.read_csv('nyc-parking-tickets*.csv', dtype=dtypes, \
                           usecols=dtypes.keys())

#select multiple columns (Plate ID and Registration State)
nyc_data_raw[['Plate ID', 'Registration State']].head()
    
#drop columns
nyc_data_raw.drop('Violation Code', axis=1).head()
#specify axis = 1 so that it knows columns and not rows


#select rows in dataframe
nyc_data_raw.loc[56].head(1)
    
missing_values = nyc_data_raw.isnull().sum() 
#creates a new Series that contains a count of missing values by column

percent_missing = ((missing_values / nyc_data_raw.index.size) * 100).compute()
percent_missing

columns_to_drop = list(percent_missing[percent_missing >= 50].index)
nyc_data_clean_stage1 = nyc_data_raw.drop(columns_to_drop, axis = 1)


count_of_vehicle_colors = nyc_data_clean_stage1['Vehicle Color'].value_counts().compute()
most_common_color = count_of_vehicle_colors.sort_values(ascending=False).index[0] 
#finds the most common vehicle color

nyc_data_clean_stage2 = nyc_data_clean_stage1.fillna({'Vehicle Color': most_common_color}) 
#imputes with most common color


license_plate_types = nyc_data_clean_stage1['Plate Type'].value_counts()
license_plate_types.compute()
#PAS = passenger type vehicles
#COM = commerical vehicles

#Recode the plate type column
condition = nyc_data_clean_stage1['Plate Type'].isin(['PAS', 'COM']) #Boolean to see if the Plate Type is PAS or COM
plate_type_masked = nyc_data_clean_stage1['Plate Type'].where(condition, 'Other') #replace other plate types with "other"
                                                                                  #stored in a new Series
nyc_data_recode_stage1 = nyc_data_clean_stage1.drop('Plate Type', axis=1) #drop old Plate Type column
nyc_data_recode_stage2 = nyc_data_recode_stage1.assign(PlateType=plate_type_masked) #add the Series to the df as a new col
nyc_data_recode_stage3 = nyc_data_recode_stage2.rename(columns={'PlateType':'Plate Type'}) #rename to include space

#parsing the issue date column
from datetime import datetime
/.,,,,,,,,,,,,,,,,,,,,,,,
issue_date_parsed = nyc_data_recode_stage3['Issue Date'].apply(lambda x: datetime.strptime(x, "%m/%d/%Y"), meta=datetime)
#parses it into a datetime object using specified format
nyc_data_derived_stage1 = nyc_data_recode_stage3.drop('Issue Date', axis=1)
nyc_data_derived_stage2 = nyc_data_derived_stage1.assign(IssueDate=issue_date_parsed)
nyc_data_derived_stage3 = nyc_data_derived_stage2.rename(columns={'IssueDate':'Issue Date'})

issue_date_month_year = nyc_data_derived_stage3['Issue Date'].apply(lambda dt: dt.strftime("%Y%m"), meta=int)
nyc_data_derived_stage4 = nyc_data_derived_stage3.assign(IssueMonthYear = issue_date_month_year)
nyc_data_derived_stage5 = nyc_data_derived_stage4.rename(columns = {'IssueMonthYear': 'Citation Issued Month Year'})

#filters and changes the index for later manipulation
condition = (nyc_data_derived_stage5['Issue Date'] > '2014-01-01') & (nyc_data_derived_stage5['Issue Date'] <= '2017-12-31')
nyc_data_filtered = nyc_data_derived_stage5[condition]
nyc_data_new_index = nyc_data_filtered.set_index('Citation Issued Month Year')

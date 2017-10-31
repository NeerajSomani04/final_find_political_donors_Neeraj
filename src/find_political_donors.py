#Load FEC Data and import needed libraries

import pandas as pd
import numpy as np

# give the path to the file 
inputfile_path = "./input/itcont.txt"
data = pd.read_csv(inputfile_path, header=None, delimiter='|', usecols=[0,10,13,14,15], dtype={10:str, 13:str, 14:str})

# Cleaning Data as per Input file considerations:

df = data[data[15].isnull()].drop(15,axis=1) # for point 1, keeping all rows where Object_Id is null and droping object_id column
df = df[pd.notnull(df[0])] #for point 5, drop all rows where CMTE_ID or TRANSACTION_AMT is null
df = df[pd.notnull(df[14])]
df[14] = df[14].astype(int) # Converting TRANSACTION_AMT to int for calculation purpose
df[13] = pd.to_datetime(df[13], format='%m%d%Y', errors='coerce')
df[10] = df[10].str[0:5].copy() #for point 3, fetching first 5 digits of zipcode 

# Creating Data frame for "medianvals_by_zip.txt" file
# to drop all records where invalid zipcode (i.e., empty, fewer than five digits)
df_zip = df[df[pd.notnull(df[10])][10].map(len) >= 5]
# calculating medianval, count, cumulative amount as per requirement
df_zip['medianvals'] = df_zip.groupby(by=(0,10),as_index=False)[14].rolling(window=3, min_periods=1).median().reset_index(level=0, drop=True).round().astype(int)
df_zip['count'] = df_zip.groupby(by = (0,10), as_index=False).cumcount() + 1
df_zip['Cum_amt'] = df_zip.groupby(by = (0,10), as_index=False)[14].cumsum()
#dropping all unnecessary columns
df_zip = df_zip.drop([13,14], axis=1)
#generating file based on above dataframe
outputfile_path_zip = './output/medianvals_by_zip.txt'
df_zip.to_csv(outputfile_path_zip, header=None, index=None, sep='|')

# Creating Data frame for "medianvals_by_date.txt" file
# to drop all records where invalid date (e.g., empty, malformed)
df_temp = df[pd.notnull(df[13])]
df_temp = df_temp.drop([10],axis=1)
grouped = df_temp.groupby([df_temp[0], df_temp[13].dt.date],as_index=False)
df_date = grouped[14].agg([np.median,np.size,np.sum]).reset_index(level=[0,1])
df_date['median'] = df_date['median'].round().astype(int)
df_date[13] = pd.to_datetime(df_date[13])
df_date[13] = df_date[13].dt.strftime('%m%d%Y')
df_date
#generating file based on above dataframe
outputfile_path_date = './output/medianvals_by_date.txt'
df_date.to_csv(outputfile_path_date, header=None, index=None, sep='|')

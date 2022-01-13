#import packages 

from numpy import dtype
import pandas as pd 
import pandas_profiling
import seaborn as sns
import matplotlib.pyplot as plt

#read the file 
df = pd.read_csv('Global_Superstore.csv', encoding = "ISO-8859-1")

#Check the datatypes 

print(df.dtypes)

#Descriptive statistics

print(df.info())

#dtypes doesn't do a clean grouping of different datatypes.So, build a function that will determine detailed type of data.

def get_var_category(series):
    unique_count = series.nunique(dropna=False)
    total_count = len(series)
    if pd.api.types.is_numeric_dtype(series):
        return 'Numerical'
    elif pd.api.types.is_datetime64_dtype(series):
        return 'Date'
    elif unique_count==total_count:
        return 'Text (Unique)'
    else:
        return 'Categorical'

def print_categories(df):
    for column_name in df.columns:
        print(column_name, ": ", get_var_category(df[column_name]))

print_categories(df)

# run the profile report
profile = df.profile_report(title='Pandas Profiling Report')
   
# save the report as html file
profile.to_file(output_file="pandas_profiling1.html")
   





import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#loading the stock market csv file
df = pd.read_csv("../data/Real_Estate_Sales_2001-2023_GL.csv")


#understanding the data

#the size of the data (rows,columns)
print(df.shape)

#first and last 5 rows
print("first and last 5 rows")
print(df.head())
print(df.tail())

#knowing the data types, columns names, and how many missing values
print("column names")
print(df.columns)
print("\nData types")
print(df.info())
print("numeric columns")
print(df.describe())

print("-"*50)
#how many missing values are present
print("missing values  per column")
print(df.isnull().sum())
print("-"*50)
print("percent of missing columns")
print(df.isnull().mean() * 100)

print("-"*50)
# number of duplicates
print("duplicates??")
print(df.duplicated().sum())
print("-"*50)


#snake_case 
df.columns = (
    df.columns.str.strip() #removes whitespaces
            .str.lower() #lowercase
            .str.replace(" ", "_") #replscing spaces w/ underscores
            .str.replace("-","_") #replacing dashes w/ underscores
)
#trimming whitespace
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)


print(df.head())

#converting to data time
df['date_recorded'] = pd.to_datetime(df['date_recorded'], errors='coerce')

print(df.head())

df['sale_amount'] = df["sale_amount"].astype('int64')
df['assessed_value'] = df["assessed_value"].astype('int64')

print(df[['sale_amount', 'assessed_value']].head())

print(df.duplicated(subset=['address', 'date_recorded']).sum())
#dropping the rows w/same address & date
df = df.drop_duplicates(subset=['address', 'date_recorded'])

df['sale_amount'].hist(bins=100, range=(0,3000000))
plt.xlabel("price")
plt.ylabel('frequency')
plt.show()

#mapping missing values with NaN
missing_values = ["", "na", "n/a", "null", "-"]
df = df.replace(missing_values, pd.NA)










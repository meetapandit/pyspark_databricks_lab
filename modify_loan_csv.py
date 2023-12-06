import pandas as pd
import numpy as np

df = pd.read_csv("/Users/meetapandit/DE_Bootcamp/databricks_lab/springboard-pyspark-project/pyspark-project/loan_updated.csv", header=None,
                 )

print(df.head())
col_names = ['Customer_ID','Age', 'Gender', 'Occupation', 'Marital Status', 'Family Size', 'Income', 'Expenditure', 'Use Frequency', 'Loan Category', 'Loan Amount', 'Overdue', 'Debt Record', 'Returned Cheque', 'Dishonour of Bill']
df = df[1:]

df.columns = col_names

print(df.head())
print(df.dtypes)

df = df.fillna(0)

dtype_dict = {'Customer_ID' : str, 
              'Age': int,
              'Gender':str, 
              'Occupation':str, 
              'Marital Status':str,
              'Family Size': int,
              'Income': int,
              'Expenditure': int,
              'Use Frequency': int,
              'Loan Category':str,
              'Loan Amount':int,
              'Overdue': int,
              'Debt Record': int,
              'Returned Cheque': int,
              'Dishonour of Bill': int}

df = df.astype(dtype_dict)

print(df.head())
print(df.dtypes)

df.to_csv("/Users/meetapandit/DE_Bootcamp/databricks_lab/springboard-pyspark-project/pyspark-project/loan_df_to_csv.csv", columns=col_names, index=False)
##############################################################################################################

# Part 1: Import necessary packages

# (a) External packages
import pandas as pd

# (b) Internal packages
from util import load_to_bigquery

##############################################################################################################

# Part 2: Set run parameters

# (Local) location of the file

default_timezone = 'UTC'
default_path = 'C:\\Users\\feiya\\OneDrive\\Desktop\\Financial Management\\'
default_filename = 'Data Structure.xlsx'
sheet_list = [

    # (a) Expense-related data
    'tblpl_expense_group', 'tblpl_expense',

    # (b) Income-related data
    'tblpl_income_group', 'tblpl_income',

    # (c) Account-related data
    'Accounts', 'Asset_Log',

    # (d) Transactions data
    'Transactions'

]

# (Cloud) location of the file
gcp_project = 'vsoesanto-gcp-finance-prod'
gcp_dataset = 'personal_finance'

##############################################################################################################

# Part 3: Main script

for i in sheet_list:

    # (a) Read the dataset
    df = pd.read_excel(default_path + default_filename, sheet_name=i)

    # (b) Set the upload date
    df['upload_date'] = pd.Timestamp(pd.Timestamp.now(), tz=default_timezone)

    # (c) Upload to bigquery
    load_to_bigquery(df, gcp_project, gcp_dataset, i, load_type='overwrite')
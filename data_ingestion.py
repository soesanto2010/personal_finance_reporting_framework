# This script contains parts for reading + cleaning the transactions and other input datasets

import pandas as pd
from util import download_data

class ingestion_pipeline(object):

    """Object to store shared state and processing methods"""
    def __init__(
            self,
            start_date,
            end_date,
            timezone,
            max_pull_retries,
            security_price_metric,
            data_input_source,
            path,
            filename,
            gcp_project,
            gcp_dataset
    ):
        """Initializes Pipeline object with shared state and inputs"""
        # (1) Run configs
        self.start_date = start_date
        self.end_date = end_date
        self.timezone = timezone
        self.max_pull_retries = max_pull_retries
        self.security_price_metric = security_price_metric
        self.data_input_source = data_input_source
        self.path = path
        self.filename = filename
        self.gcp_project = gcp_project
        self.gcp_dataset = gcp_dataset
        self.Days_Ellapsed = (end_date - start_date).days

        # (2) Download datasets
        self.Transactions = download_data(self.path, self.filename, self.gcp_project, self.gcp_dataset, 'Transactions', self.data_input_source)
        self.Accounts = download_data(self.path, self.filename, self.gcp_project, self.gcp_dataset, 'Accounts', self.data_input_source)
        self.Expense_Picklist = download_data(self.path, self.filename, self.gcp_project, self.gcp_dataset, 'tblpl_expense', self.data_input_source)
        self.Expense_Group_Picklist = download_data(self.path, self.filename, self.gcp_project, self.gcp_dataset, 'tblpl_expense_group', self.data_input_source)
        self.Income_Picklist = download_data(self.path, self.filename, self.gcp_project, self.gcp_dataset, 'tblpl_income', self.data_input_source)
        self.Income_Group_Picklist = download_data(self.path, self.filename, self.gcp_project, self.gcp_dataset, 'tblpl_income_group', self.data_input_source)

        print("finished reading inputs")

    def preprocess_transactions(self):

        # (1) Drop unused columns
        self.Transactions.drop(columns=['tr_ID','tr_supplier','tr_qty_units','tr_rate','tr_notes'],inplace=True)
        self.Accounts.drop(columns=['acc_notes','acc_last_refresh','acc_baseline_date'],inplace=True)
        self.Expense_Picklist.drop(columns=['exp_ID'],inplace=True)
        self.Income_Picklist.drop(columns=['inc_ID'],inplace=True)

        # (2) Add depreciation expenses
        Transactions_with_depex = self.add_fixture_depreciation_expense()
        print('finished adding depreciation expenses')

        # (3) Define Date-related variables
        Transactions_with_depex['Tr_Date'] = pd.to_datetime(Transactions_with_depex['tr_close_date'], format="%m/%d/%y")
        Transactions_with_depex['Tr_Week'] = Transactions_with_depex['tr_close_date'].dt.isocalendar().week
        Transactions_with_depex['Tr_Month'] = Transactions_with_depex['tr_close_date'].dt.month
        Transactions_with_depex['Tr_Year'] = Transactions_with_depex['tr_close_date'].dt.isocalendar().year
        Transactions_with_depex['Tr_Date'] = pd.to_datetime(Transactions_with_depex['tr_close_date']).dt.date

        # (4) Keep only in-window transactions
        Transactions_relevant_timeframe = Transactions_with_depex[Transactions_with_depex['Tr_Date'] <= self.end_date]

        # (5) Add Account ID
        Transactions_temp_1 = Transactions_relevant_timeframe.merge(self.Accounts.iloc[:,0:2],left_on="tr_impacted_acc_1",right_on="acc_name",how="left")
        self.Transactions_temp_2 = Transactions_temp_1.merge(self.Accounts.iloc[:,0:2],left_on="tr_impacted_acc_2",right_on="acc_name",how="left",suffixes=('_1','_2'))

        # (6) Rename columns
        self.Transactions_temp_2.rename(columns={"acc_ID_1":"Impacted_Acc_ID_1",
                                                 "acc_ID_2":"Impacted_Acc_ID_2",
                                                 "tr_impacted_acc_1":"Impacted_Acc_1",
                                                 "tr_impacted_acc_2":"Impacted_Acc_2",
                                                 "tr_impacted_acc_1_sign":"Impacted_Acc_1_Sign",
                                                 "tr_impacted_acc_2_sign":"Impacted_Acc_2_Sign"},inplace=True)

        # (7) Keep only relevant columns
        self.Transactions = self.Transactions_temp_2.drop(columns=['acc_name_1','acc_name_2','tr_init_date','tr_close_date','tr_SKU_lifetime'])
        print('finished preprocessing Transactions dataset')

    def add_fixture_depreciation_expense(self):

        """This block of code takes as input a list of capital expenditure (capex) transactions and generates
        a list of depreciation expense transactions (once for each month) between (1) the date of purchase and (2)
        the date of liquidation, which is currently set as purchase date + lifetime value (which is an input of
        the data)"""

        # (1) Pull transactions that correspond to capex investments
        capex = self.Transactions[self.Transactions["tr_expense"] == 'Housing Expense - Fixture Investment'][["tr_description","tr_amt","tr_close_date","tr_SKU_lifetime"]]
        capex = capex.reset_index()    # reset has to be reset since we later perform indexing ops
        capex['monthly_depreciation'] = capex['tr_amt'] / capex['tr_SKU_lifetime']

        # (2) Initialize the depreciation expense entries
        ls_SKU = []
        ls_depex = []
        ls_dep_date = []

        # (3) Fill in the depreciation expense for each item i in each capex transaction
        for i in list(range(0,len(capex))):

            # (a) Extract the components
            SKU = capex.iloc[i,1]
            start_date = capex.iloc[i,3]
            max_months = int(capex.iloc[i,4])    # default is float, which gives error in indexing ops in part (b)
            depex = capex.iloc[i,5]

            # (b) Loop and create depex items for each PP&E, once for each month, until the max. months is reached
            for j in list(range(1,max_months+1)):
                end_dep_date = start_date + pd.DateOffset(months=j)
                ls_SKU += [SKU]
                ls_depex += [depex]
                ls_dep_date += [end_dep_date]

        depex_table = pd.DataFrame({'tr_description':ls_SKU,'tr_amt':ls_depex,'tr_close_date':ls_dep_date})

        # (4) Define  other columns for the depreciation expense transactions
        depex_agg = depex_table.groupby(['tr_close_date'],as_index=False)['tr_amt'].sum()
        depex_agg['tr_description'] = 'Fixture Depreciation'
        depex_agg['tr_impacted_acc_1'] = 'PP&E - Fixtures'
        depex_agg['tr_impacted_acc_1_sign'] = '[-ve]'
        depex_agg['tr_impacted_acc_2'] = 'Expense - Housing'
        depex_agg['tr_impacted_acc_2_sign'] = '[+ve]'
        depex_agg['tr_expense'] = 'Housing Expense - Depreciation'

        return self.Transactions.append(depex_agg,ignore_index=True)

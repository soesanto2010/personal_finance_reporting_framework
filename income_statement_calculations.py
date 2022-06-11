# This script contains functions which generate a trend of revenue + expenses, in addition to calculating
# financial KPIs

import pandas as pd
import numpy as np

def get_revenue_trend(self):

    """This block of code determines the operating revenue by month and year"""

    self.Revenue_Subset = self.Transactions[~self.Transactions["tr_income"].isnull()].merge(self.Income_Picklist,left_on=["tr_income"],right_on=["inc_name"],how='left')
    Revenue_Relevant_Dataset = self.Revenue_Subset[self.Revenue_Subset["inc_is_operational"] == 1]
    Revenue_Relevant_Dataset = Revenue_Relevant_Dataset.merge(self.Income_Group_Picklist,on='inc_grp_ID',how='left')

    self.Revenue_Breakdown = Revenue_Relevant_Dataset.groupby("inc_grp",as_index=False)["tr_amt"].sum()
    self.Revenue_Breakdown["%"] = self.Revenue_Breakdown["tr_amt"]/self.Revenue_Breakdown["tr_amt"].sum()

    self.Revenue_Trend = Revenue_Relevant_Dataset[["tr_amt","Tr_Year","Tr_Month"]].groupby(["Tr_Year","Tr_Month"],as_index=False)["tr_amt"].sum()
    print('finished generating breakdown of operational revenue by month and year')

def get_expenses_trend(self):

    """This block of code determines the living expenses by month and year"""

    self.Expenses_Subset = self.Transactions[~self.Transactions["tr_expense"].isnull()].merge(self.Expense_Picklist,left_on=["tr_expense"],right_on=["exp_name"],how='left')
    Expenses_Relevant_Dataset = self.Expenses_Subset[self.Expenses_Subset["exp_is_live"] == 1]
    Expenses_Relevant_Dataset = Expenses_Relevant_Dataset.merge(self.Expense_Group_Picklist,on=['exp_grp_ID'],how='left')

    self.Expense_List = Expenses_Relevant_Dataset.groupby(['exp_grp','exp_name','Tr_Year','Tr_Month'],as_index=False)['tr_amt'].sum()
    self.Expense_Pivot = pd.pivot_table(self.Expense_List,index=['exp_grp','exp_name'],values=['tr_amt'],columns=['Tr_Year','Tr_Month'],aggfunc=[np.sum])
    print('finished generating breakdown of living expenses by month and year')

def calculate_financial_KPIs(self):

    """This block of code calculates primary financial KPIs based on the Class-Level Summary.
    We currently don't have any non-current assets (excluding our car) or non-current liabilities"""

    total_assets = self.Class_Level_Summary[self.Class_Level_Summary["acc_A/L/E_classification"] == "Asset"]["End_Value_Overwrite"].sum()
    liquid_net_assets = self.Class_Level_Summary[(self.Class_Level_Summary["acc_type"] == "Cash") | (self.Class_Level_Summary["acc_type"] == "Marketable Securities")]["End_Value_Overwrite"].sum()
    total_liabilities = self.Class_Level_Summary[self.Class_Level_Summary["acc_A/L/E_classification"] == "Liability"]["End_Value_Overwrite"].sum()
    revenue = self.Class_Level_Summary[self.Class_Level_Summary["acc_type"] == "Revenue"]["Net_Change_From_Operations"].sum()
    net_income = self.Class_Level_Summary[(self.Class_Level_Summary["acc_type"] == "Revenue") | (self.Class_Level_Summary["acc_type"] == "Gain")]["Net_Change_From_Operations"].sum() - self.Class_Level_Summary[(self.Class_Level_Summary["acc_type"] == "Expense") | (self.Class_Level_Summary["acc_type"] == "Loss")]["Net_Change_From_Operations"].sum()
    # Ratios
    current_ratio = total_assets / total_liabilities
    cash_ratio = liquid_net_assets / total_liabilities
    asset_turnover = revenue / total_assets
    net_profit_margin = net_income / revenue
    ROA = net_income / total_assets

    self.Summary_KPI = pd.DataFrame({'Metric':["liquid_net_assets minus liability","current_ratio","cash_ratio","asset_turnover","net_profit_margin","ROA"],
                                 'Value':[liquid_net_assets-total_liabilities,current_ratio,cash_ratio,asset_turnover,net_profit_margin,ROA]})

    print('finished generating breakdown of living expenses by month and year')

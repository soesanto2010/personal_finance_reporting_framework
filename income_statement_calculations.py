# This script contains functions which generate a trend of revenue + expenses, in addition to calculating
# financial KPIs

import pandas as pd
import numpy as np

def get_revenue_trend(self):

    """This block of code determines the revenue by period (by month and year)"""

    # (1) Filter to only transactions data that has revenue recognition
    transactions_with_revenue_recognition = self.Transactions[~self.Transactions["tr_income"].isnull()]
    self.transactions_with_income_IDs = transactions_with_revenue_recognition.merge(self.Income_Picklist, left_on=["tr_income"], right_on=["inc_name"], how='left').merge(self.Income_Group_Picklist, on='inc_grp_ID', how='left')

    # (2) Exclude revenue sources coming from:
    # (a) Insurance Reimbursement (these will later be presented as offsets to total healthcare costs in the expense report)
    # (b) Proceeds from sale of assets (the gains / losses are currently inserted manually into the transactions table)
    subset = self.transactions_with_income_IDs[~self.transactions_with_income_IDs["inc_secondary_cat"].isna()]

    # (3) Group the different revenue sources by period
    time_trend = subset.groupby(["Tr_Year", "Tr_Month", "inc_primary_cat", "inc_secondary_cat"],as_index=False)["tr_amt"].sum()

    # (4) Add the total
    agg_trend = subset.groupby(["Tr_Year", "Tr_Month"],as_index=False)["tr_amt"].sum()
    agg_trend["inc_primary_cat"] = '(0) Total'
    agg_trend["inc_secondary_cat"] = '(0) Total'

    time_trend_with_totals = time_trend.append(agg_trend)

    # (5) Create pivot table summaries (one based on the primary category, and the other based on the secondary category)
    self.Revenue_Trend_primary_cat = pd.pivot_table(time_trend_with_totals,
                                                    index=['Tr_Year', 'Tr_Month'],
                                                    values=['tr_amt'],
                                                    columns=['inc_primary_cat'],
                                                    aggfunc=[np.sum])
    self.Revenue_Trend_secondary_cat = pd.pivot_table(time_trend_with_totals,
                                                      index=['Tr_Year', 'Tr_Month'],
                                                      values=['tr_amt'],
                                                      columns=['inc_primary_cat', 'inc_secondary_cat'],
                                                      aggfunc=[np.sum])

    print('finished generating breakdown of revenue by month and year')

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

    total_assets = self.Class_Level_Summary[self.Class_Level_Summary["acc_A/L/E_classification"] == "Asset"]["MV"].sum()
    liquid_net_assets = self.Class_Level_Summary[(self.Class_Level_Summary["acc_type"] == "Cash") | (self.Class_Level_Summary["acc_type"] == "Marketable Securities")]["MV"].sum()
    total_liabilities = self.Class_Level_Summary[self.Class_Level_Summary["acc_A/L/E_classification"] == "Liability"]["MV"].sum()
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

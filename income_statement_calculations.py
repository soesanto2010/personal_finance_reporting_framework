#####################################################################################
# This script contains functions which generate a trend of revenue + expenses, in
# addition to calculating financial KPIs

import pandas as pd
import numpy as np


def generate_profit_and_loss_statement(self):

    """This block of code determines the profit & loss (P&L) by period
    (by month and year)"""

    columns_to_keep = [
        "Tr_Year",
        "Tr_Month",
        "P_L_primary_cat",
        "P_L_secondary_cat",
        "P_L_secondary_cat_after_offset",
        "P_L_offset_flag",
        "tr_amt",
    ]

    # (1) Filter to only transactions data that has revenue and/or expense recognition
    transactions_with_revenue_recognition = self.Transactions[
        ~self.Transactions["tr_income"].isnull()
    ]
    transactions_with_expense_recognition = self.Transactions[
        ~self.Transactions["tr_expense"].isnull()
    ]

    # (2) Add the P&L IDs
    self.transactions_with_income_IDs = transactions_with_revenue_recognition.merge(
        self.Income_Picklist, left_on=["tr_income"], right_on=["inc_name"], how="left"
    ).merge(self.Income_Group_Picklist, on="inc_grp_ID", how="left")
    self.transactions_with_expense_IDs = transactions_with_expense_recognition.merge(
        self.Expense_Picklist, left_on=["tr_expense"], right_on=["exp_name"], how="left"
    ).merge(self.Expense_Group_Picklist, on=["exp_grp_ID"], how="left")

    # (3) Include only transactions with non-NULL P&L IDs:
    # Exclude certain expenses (like depreciation and acquisition of shares) and
    # revenues (like proceeds from sales of shares)
    subset_income = self.transactions_with_income_IDs[
        ~self.transactions_with_income_IDs["P_L_primary_cat"].isna()
    ]
    subset_expenses = self.transactions_with_expense_IDs[
        ~self.transactions_with_expense_IDs["P_L_primary_cat"].isna()
    ]

    # (4) Keep only the required columns
    subset_income = subset_income[columns_to_keep]
    subset_expenses = subset_expenses[columns_to_keep]

    # (5) Assign the magnitudes (+1 for income and -1 for expense)
    subset_income["amount"] = subset_income["tr_amt"] * 1
    subset_expenses["amount"] = subset_expenses["tr_amt"] * -1

    # (6) Determine the transaction type
    subset_income["type"] = "(1) revenue"
    subset_expenses["type"] = "(2) expenses"
    # Those with offsets need to change transaction type (for example, healthcare
    # insurance reimbursements are classified as offsets to the healthcare expense
    # instead of a revenue line item)
    subset_income.loc[subset_income["P_L_offset_flag"] == 1, "type"] = "(2) expenses"
    subset_expenses.loc[subset_expenses["P_L_offset_flag"] == 1, "type"] = "(1) revenue"

    # (6) Append the revenue and expense statements together
    subset = subset_income.append(subset_expenses, ignore_index=True)

    # (7) Get the remapped secondary P&L category
    subset.loc[
        subset["P_L_secondary_cat_after_offset"].isna(),
        "P_L_secondary_cat_after_offset",
    ] = subset["P_L_secondary_cat"]

    # (8) Group the line items by period
    time_trend = subset.groupby(
        [
            "Tr_Year",
            "Tr_Month",
            "type",
            "P_L_primary_cat",
            "P_L_secondary_cat",
            "P_L_secondary_cat_after_offset",
        ],
        as_index=False,
    )["amount"].sum()

    # (9) Create pivot table summaries (one based on a higher-level, and
    # the other based on a deep dive)
    self.P_L_statement_overall = pd.pivot_table(
        time_trend,
        index=["type", "P_L_primary_cat", "P_L_secondary_cat_after_offset"],
        columns=["Tr_Year", "Tr_Month"],
        values=["amount"],
        aggfunc=[np.sum],
    )
    self.P_L_statement_deep_dive = pd.pivot_table(
        time_trend,
        index=["type", "P_L_primary_cat", "P_L_secondary_cat"],
        columns=["Tr_Year", "Tr_Month"],
        values=["amount"],
        aggfunc=[np.sum],
    )

    print("finished generating breakdown of P&L statement by month and year")


def calculate_financial_KPIs(self):

    """This block of code calculates primary financial KPIs based on the Class-Level
    Summary. We currently don't have any non-current assets (excluding our car) or
    non-current liabilities"""

    total_assets = self.Class_Level_Summary[
        self.Class_Level_Summary["acc_A_L_E_classification"] == "Asset"
    ]["MV"].sum()
    liquid_net_assets = self.Class_Level_Summary[
        (self.Class_Level_Summary["acc_type"] == "Cash")
        | (self.Class_Level_Summary["acc_type"] == "Marketable Securities")
    ]["MV"].sum()
    total_liabilities = self.Class_Level_Summary[
        self.Class_Level_Summary["acc_A_L_E_classification"] == "Liability"
    ]["MV"].sum()
    revenue = self.Class_Level_Summary[
        self.Class_Level_Summary["acc_type"] == "Revenue"
    ]["Net_Change_From_Operations"].sum()
    net_income = (
        self.Class_Level_Summary[
            (self.Class_Level_Summary["acc_type"] == "Revenue")
            | (self.Class_Level_Summary["acc_type"] == "Gain")
        ]["Net_Change_From_Operations"].sum()
        - self.Class_Level_Summary[
            (self.Class_Level_Summary["acc_type"] == "Expense")
            | (self.Class_Level_Summary["acc_type"] == "Loss")
        ]["Net_Change_From_Operations"].sum()
    )
    # Ratios
    current_ratio = total_assets / total_liabilities
    cash_ratio = liquid_net_assets / total_liabilities
    asset_turnover = revenue / total_assets
    net_profit_margin = net_income / revenue
    ROA = net_income / total_assets

    self.Summary_KPI = pd.DataFrame(
        {
            "Metric": [
                "liquid_net_assets minus liability",
                "current_ratio",
                "cash_ratio",
                "asset_turnover",
                "net_profit_margin",
                "ROA",
            ],
            "Value": [
                liquid_net_assets - total_liabilities,
                current_ratio,
                cash_ratio,
                asset_turnover,
                net_profit_margin,
                ROA,
            ],
        }
    )

    print("finished generating breakdown of living expenses by month and year")

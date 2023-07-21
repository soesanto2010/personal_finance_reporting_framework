#####################################################################################
# This script contains functions which generate a snapshot of the value of (1) an
# account, (2) account type or (3) a balance sheet item (asset, liability, equity)
# given a transaction dataset

import pandas as pd


def get_account_level_balance_sheet(self):

    """This block of code calculates the (1) start value, (2) end value and (3) delta
    for each account. For example, a specific bank account is a specific account"""

    # (1) For each account, determine (1) the baseline amount and (2) change in
    # amount between the start and end dates
    unique_accounts = list(self.Accounts["acc_ID"].drop_duplicates())
    baseline_amount = self.Accounts.copy()
    delta_amount = calculate_change(
        unique_accounts, self.Transactions_preprocessed, "tr_amt"
    )
    df = baseline_amount.merge(
        delta_amount, left_on=["acc_ID"], right_on=["Impacted_Acc_ID"], how="left"
    )
    df.fillna(value=0, inplace=True)

    # (2) Calculate the total book value (BV) at the closing date
    df["BV"] = df["acc_baseline_value"] + df["Net_Change"]

    # (3) Determine the total market value (MV) and total cost basis (CB), which are
    # determined by the securities valuation module for marketable securities
    self.Acct_Level_Summary = df.merge(
        self.securities[["acc_ID", "MV", "CB"]], on=["acc_ID"], how="left"
    )
    self.Acct_Level_Summary.rename(
        columns={
            "acc_baseline_value": "Baseline_Value",
            "Net_Change": "Net_Change_From_Operations",
        },
        inplace=True,
    )

    # Certain assets require market values to come from the accounts table (e.g. cars):
    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["acc_name"].isin(self.asset_list_to_use_fallback), "MV"
    ] = self.Acct_Level_Summary["acc_security_value_fallback"]

    # For line items other than marketable securities set:
    # (a) the total market value equal to the book value
    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["MV"].isnull(), "MV"
    ] = self.Acct_Level_Summary["BV"]
    # (b) the cost basis equal to the book value
    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["CB"].isnull(), "CB"
    ] = self.Acct_Level_Summary["BV"]

    self.Acct_Level_Summary["Net_Change_From_Market_Adjustment"] = (
        self.Acct_Level_Summary["MV"] - self.Acct_Level_Summary["BV"]
    )
    self.Acct_Level_Summary.sort_values(
        by=["acc_report_rank"], ascending=True, inplace=True
    )
    columns_to_keep = [
        "acc_A_L_E_classification",
        "acc_A_L_E_sign",
        "acc_type",
        "acc_ID",
        "acc_name",
        "acc_def_inc_tax_rate",
        "acc_pre_tax_acct",
    ] + self.balance_sheet_num_columns
    self.Acct_Level_Summary = self.Acct_Level_Summary[columns_to_keep]

    # (4) Calculate unrealized gain / loss due to market valuation
    Unrealized_gain = self.Acct_Level_Summary[
        self.Acct_Level_Summary["Net_Change_From_Market_Adjustment"] > 0
    ]["Net_Change_From_Market_Adjustment"].sum()
    Unrealized_loss = self.Acct_Level_Summary[
        self.Acct_Level_Summary["Net_Change_From_Market_Adjustment"] < 0
    ]["Net_Change_From_Market_Adjustment"].sum()

    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Gain"),
        "MV",
    ] = Unrealized_gain
    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Gain"),
        "Net_Change_From_Market_Adjustment",
    ] = Unrealized_gain

    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Loss"),
        "MV",
    ] = (
        Unrealized_loss * -1
    )
    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Loss"),
        "Net_Change_From_Market_Adjustment",
    ] = (
        Unrealized_loss * -1
    )

    # (5) Split the revenue account items into (a) retained earnings and
    # (b) unrealized gain / loss
    unchanged_accounts = self.Acct_Level_Summary.loc[
        (self.Acct_Level_Summary["acc_A_L_E_classification"] != "Equity")
        | (self.Acct_Level_Summary["acc_name"].str.contains("Unrealized"))
        | (self.Acct_Level_Summary["acc_name"] == "Expense - Deferred Taxes (Est)")
    ]

    accounts_to_merge = self.Acct_Level_Summary.loc[
        (self.Acct_Level_Summary["acc_A_L_E_classification"] == "Equity")
        & (~self.Acct_Level_Summary["acc_name"].str.contains("Unrealized"))
        & (self.Acct_Level_Summary["acc_name"] != "Expense - Deferred Taxes (Est)")
    ]
    accounts_to_merge.loc[accounts_to_merge["acc_A_L_E_sign"] == "[+ve]", "Vector"] = 1
    accounts_to_merge.loc[accounts_to_merge["acc_A_L_E_sign"] == "[-ve]", "Vector"] = -1

    # Mutiply magnitude by either +1 (for revenue / gains) or -1 for (expense / loss)
    for i in self.balance_sheet_num_columns:
        accounts_to_merge[i] = accounts_to_merge[i] * accounts_to_merge["Vector"]

    accounts_to_merge["acc_A_L_E_sign"] = "[+ve]"
    accounts_to_merge["acc_type"] = "Retained Earnings"

    self.acc_ID_for_retained_earnings = accounts_to_merge["acc_ID"].min()

    accounts_to_merge["acc_ID"] = self.acc_ID_for_retained_earnings
    accounts_to_merge["acc_name"] = "Retained Earnings"
    retained_earnings = accounts_to_merge.groupby(
        [
            "acc_A_L_E_classification",
            "acc_A_L_E_sign",
            "acc_type",
            "acc_ID",
            "acc_name",
            "acc_def_inc_tax_rate",
            "acc_pre_tax_acct",
        ],
        as_index=False,
    )[self.balance_sheet_num_columns].sum()

    self.Acct_Level_Summary = pd.concat(
        [unchanged_accounts, retained_earnings], ignore_index=True
    )

    # (6) Add "unrealized" deferred tax statements based on theoretical distribution
    # of pre-tax accounts and sale of assets at time of closing
    subset_temp = self.Acct_Level_Summary[
        self.Acct_Level_Summary["acc_def_inc_tax_rate"] > 0
    ]

    # (a) For pre-tax accounts like HSA and 401(k), disbursement is taxed with zero
    # cost basis since we're contributing to it with pre-tax dollars
    tax_deferred_accts = subset_temp[subset_temp["acc_pre_tax_acct"] == 1]
    tax_deferred_accts["deferred_tax_amount"] = (
        tax_deferred_accts["acc_def_inc_tax_rate"] * tax_deferred_accts["MV"]
    )
    deferred_taxes_from_dist_of_tax_deferred_accts = tax_deferred_accts[
        "deferred_tax_amount"
    ].sum()

    # (b) For non-pre tax accounts like stocks outside of retirement accounts,
    # disbursement is taxed with reference to the weighted-average cost basis
    non_tax_deferred_accts = subset_temp[subset_temp["acc_pre_tax_acct"] == 0]
    non_tax_deferred_accts["deferred_tax_amount"] = non_tax_deferred_accts[
        "acc_def_inc_tax_rate"
    ] * (non_tax_deferred_accts["MV"] - non_tax_deferred_accts["CB"])
    deferred_taxes_from_dist_of_non_tax_deferred_accts = max(
        0, non_tax_deferred_accts["deferred_tax_amount"].sum()
    )

    # (c) Creating the additional deferred tax accounts
    deferred_taxes_line_item = self.Accounts[
        self.Accounts["acc_name"].isin(
            ["Deferred Taxes Payable", "Expense - Deferred Taxes (Est)"]
        )
    ]
    deferred_taxes_line_item["MV"] = (
        deferred_taxes_from_dist_of_tax_deferred_accts
        + deferred_taxes_from_dist_of_non_tax_deferred_accts
    )
    deferred_taxes_line_item["BV"] = 0
    deferred_taxes_line_item["CB"] = 0
    deferred_taxes_line_item["Net_Change_From_Market_Adjustment"] = 0
    deferred_taxes_line_item["Baseline_Value"] = 0
    deferred_taxes_line_item["Net_Change_From_Operations"] = 0

    # (d) Append and combine entries
    agg_accounts = pd.concat([self.Acct_Level_Summary, deferred_taxes_line_item])
    final_output = agg_accounts.groupby(
        [
            "acc_A_L_E_classification",
            "acc_A_L_E_sign",
            "acc_type",
            "acc_ID",
            "acc_name",
        ],
        as_index=False,
    )[
        "Baseline_Value",
        "Net_Change_From_Operations",
        "BV",
        "CB",
        "Net_Change_From_Market_Adjustment",
        "MV",
    ].sum()

    # (7) Keep only rows with at least one non-NULL value in MV
    self.Acct_Level_Summary = final_output[
        (final_output["Baseline_Value"] > 0) | (final_output["MV"] != 0)
    ]

    self.Acct_Level_Summary.sort_values(by=["acc_ID"], ascending=True, inplace=True)

    print("finished preparing Account-level report")


def get_account_type_level_balance_sheet(self):

    """This block of code calculates the start value, end value and delta for the three
    sub-items of the balance sheet. For example, cash is a specific account type"""

    self.Class_Level_Summary = self.Acct_Level_Summary.groupby(
        ["acc_A_L_E_classification", "acc_A_L_E_sign", "acc_type"], as_index=False
    )[self.balance_sheet_num_columns].sum()
    Acc_type_priority = self.Accounts.groupby("acc_type", as_index=False)[
        "acc_report_rank"
    ].min()
    self.Class_Level_Summary = self.Class_Level_Summary.merge(
        Acc_type_priority, on="acc_type", how="left"
    )

    self.Class_Level_Summary.loc[
        self.Class_Level_Summary["acc_type"] == "Retained Earnings", "acc_report_rank"
    ] = self.acc_ID_for_retained_earnings
    self.Class_Level_Summary.sort_values(
        by=["acc_report_rank"], ascending=True, inplace=True
    )

    print("finished preparing Class-level report")


def get_overall_balance_sheet(self):

    """This block of code calculates the start value, end value and delta for the three
    line items of the balance sheet - asset, liability and equity"""

    Parts = self.Class_Level_Summary.copy()

    Parts.loc[Parts["acc_A_L_E_sign"] == "[+ve]", "Vector"] = 1
    Parts.loc[Parts["acc_A_L_E_sign"] == "[-ve]", "Vector"] = -1
    Parts["Baseline_Value"] = Parts["Vector"] * Parts["Baseline_Value"]
    Parts["Net_Change_From_Operations"] = (
        Parts["Vector"] * Parts["Net_Change_From_Operations"]
    )
    Parts["Net_Change_From_Market_Adjustment"] = (
        Parts["Vector"] * Parts["Net_Change_From_Market_Adjustment"]
    )
    Parts["BV"] = Parts["Vector"] * Parts["BV"]
    Parts["MV"] = Parts["Vector"] * Parts["MV"]

    self.BS_Level_Summary = Parts.groupby("acc_A_L_E_classification", as_index=False)[
        self.balance_sheet_num_columns
    ].sum()
    BS_Item_Priority = self.Accounts.groupby(
        "acc_A_L_E_classification", as_index=False
    )["acc_report_rank"].min()

    self.BS_Level_Summary = self.BS_Level_Summary.merge(
        BS_Item_Priority, on="acc_A_L_E_classification", how="left"
    )
    self.BS_Level_Summary.sort_values(
        by=["acc_report_rank"], ascending=True, inplace=True
    )

    print("finished preparing overall Balance Sheet")


def calculate_change(S, Transactions, metric):

    """This block of code takes as input a list of accounts, and returns the
    (operational) net change in a specific metric of that account between Start_Date
    and End_Date as a result of the recorded transactions. Since each line of
    transaction involves two accounts, Acc_1 and Acc_2, we have to repeat this twice"""

    # (1) Amounts coming from Acc_1 side
    df_Acc_1 = Transactions[Transactions["Impacted_Acc_ID_1"].isin(S)]
    df_Acc_1 = df_Acc_1[["Impacted_Acc_ID_1", "Tr_Date", "Impacted_Acc_1_Sign", metric]]
    df_Acc_1.rename(
        columns={
            "Impacted_Acc_ID_1": "Impacted_Acc_ID",
            "Impacted_Acc_1_Sign": "Sign",
            metric: "Mag",
        },
        inplace=True,
    )

    # (2) Amounts coming from Acc_2 side
    df_Acc_2 = Transactions[Transactions["Impacted_Acc_ID_2"].isin(S)]
    df_Acc_2 = df_Acc_2[["Impacted_Acc_ID_2", "Tr_Date", "Impacted_Acc_2_Sign", metric]]
    df_Acc_2.rename(
        columns={
            "Impacted_Acc_ID_2": "Impacted_Acc_ID",
            "Impacted_Acc_2_Sign": "Sign",
            metric: "Mag",
        },
        inplace=True,
    )

    # (3) Append amounts coming from both sides
    df = pd.concat([df_Acc_1, df_Acc_2], ignore_index=True)

    # (4) Calculate net change
    df.loc[df["Sign"] == "[+ve]", "Vector"] = 1
    df.loc[df["Sign"] == "[-ve]", "Vector"] = -1
    df["Net_Change"] = df["Vector"] * df["Mag"]

    # (5) Group by account
    Change = df.groupby(["Impacted_Acc_ID"], as_index=False)["Net_Change"].sum()
    Change.sort_values(by=["Impacted_Acc_ID"], ascending=True, inplace=True)

    return Change

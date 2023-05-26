#####################################################################################
# This script contains functions which generate a snapshot of the value of (1) an
# account, (2) account type or (3) a balance sheet item (asset, liability, equity)
# given a transaction dataset


def get_account_level_balance_sheet(self):

    """This block of code calculates the (1) start value, (2) end value and (3) delta
    for each account. For example, a specific bank account is a specific account"""

    # (1) For each account, we determine the baseline and calculate the change in
    # value between the start and end dates
    unique_accounts = list(self.Accounts["acc_ID"].drop_duplicates())
    baseline_amount = self.Accounts.copy()
    delta_amount = calculate_change(unique_accounts, self.Transactions, "tr_amt")
    df = baseline_amount.merge(
        delta_amount, left_on=["acc_ID"], right_on=["Impacted_Acc_ID"], how="left"
    )
    df.fillna(value=0, inplace=True)

    # (2) Calculate the total book value (BV) at the closing date
    df["BV"] = df["acc_baseline_value"] + df["Net_Change"]

    # (3) Determine the total market value (MV), which is determined by the securities
    # valuation module
    self.Acct_Level_Summary = df.merge(
        self.securities[["acc_ID", "MV"]], on=["acc_ID"], how="left"
    )
    self.Acct_Level_Summary.rename(
        columns={
            "acc_baseline_value": "Baseline_Value",
            "Net_Change": "Net_Change_From_Operations",
        },
        inplace=True,
    )
    # For line items other than marketable securities, set the total market value equal
    # to the book value
    self.Acct_Level_Summary.loc[
        self.Acct_Level_Summary["MV"].isnull(), "MV"
    ] = self.Acct_Level_Summary["BV"]
    self.Acct_Level_Summary["Net_Change_From_Market_Adjustment"] = (
        self.Acct_Level_Summary["MV"] - self.Acct_Level_Summary["BV"]
    )
    self.Acct_Level_Summary.sort_values(
        by=["acc_report_rank"], ascending=True, inplace=True
    )
    self.Acct_Level_Summary = self.Acct_Level_Summary[
        [
            "acc_A_L_E_classification",
            "acc_A_L_E_sign",
            "acc_type",
            "acc_ID",
            "acc_name",
            "Baseline_Value",
            "Net_Change_From_Operations",
            "BV",
            "Net_Change_From_Market_Adjustment",
            "MV",
        ]
    ]

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

    # (5) Keep only rows with at least one non-NULL value in MV
    self.Acct_Level_Summary = self.Acct_Level_Summary[
        (self.Acct_Level_Summary["Baseline_Value"] != 0)
        | (self.Acct_Level_Summary["MV"] != 0)
    ]

    print("finished preparing Account-level report")


def get_account_type_level_balance_sheet(self):

    """This block of code calculates the start value, end value and delta for the three
    sub-items of the balance sheet. For example, cash is a specific account type"""

    self.Class_Level_Summary = self.Acct_Level_Summary.groupby(
        ["acc_A_L_E_classification", "acc_A_L_E_sign", "acc_type"], as_index=False
    )[
        "Baseline_Value",
        "Net_Change_From_Operations",
        "BV",
        "Net_Change_From_Market_Adjustment",
        "MV",
    ].sum()
    Acc_type_priority = self.Accounts.groupby("acc_type", as_index=False)[
        "acc_report_rank"
    ].min()
    self.Class_Level_Summary = self.Class_Level_Summary.merge(
        Acc_type_priority, on="acc_type", how="left"
    )
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
        [
            "Baseline_Value",
            "Net_Change_From_Operations",
            "BV",
            "Net_Change_From_Market_Adjustment",
            "MV",
        ]
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
    df = df_Acc_1.append(df_Acc_2, ignore_index=True)

    # (4) Calculate net change
    df.loc[df["Sign"] == "[+ve]", "Vector"] = 1
    df.loc[df["Sign"] == "[-ve]", "Vector"] = -1
    df["Net_Change"] = df["Vector"] * df["Mag"]

    # (5) Group by account
    Change = df.groupby(["Impacted_Acc_ID"], as_index=False)["Net_Change"].sum()
    Change.sort_values(by=["Impacted_Acc_ID"], ascending=True, inplace=True)

    return Change

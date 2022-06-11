# This script contains functions which generate a snapshot of the value of (1) an account, (2) account type or (3)
# a balance sheet item (asset, liability, equity) given a transaction dataset

def get_account_level_balance_sheet(self):

    """This block of code calculates the start value, end value and delta for each specific account.
    For example, a specific bank account is a specific account"""

    # For each account, we determine the baseline and calculate the change in value between the start and end dates
    Unique_Accounts = list(self.Accounts["acc_ID"].drop_duplicates())
    Baseline_Value = self.Accounts.iloc[:,[0,7]]
    Change = calculate_change(Unique_Accounts,self.Transactions)
    df = Baseline_Value.merge(Change,left_on=["acc_ID"],right_on=["Impacted_Acc_ID"],how="left")
    df.fillna(value=0,inplace=True)
    df["End_Value"] = df["acc_baseline_value"] + df["Net_Change"]

    # Overwrite end value if it already exists in Accounts (this is the case for equities)
    self.Acct_Level_Summary = df.merge(self.Accounts,on=["acc_ID"],how="left")
    self.Acct_Level_Summary.rename(columns={"acc_baseline_value_x":"Baseline_Value",
                                                "Net_Change":"Net_Change_From_Operations"},inplace=True)
    self.Acct_Level_Summary["End_Value_Overwrite"] = self.Acct_Level_Summary["acc_end_value_overwrite"]
    self.Acct_Level_Summary.loc[self.Acct_Level_Summary["acc_end_value_overwrite"].isnull(),"End_Value_Overwrite"] = self.Acct_Level_Summary["End_Value"]
    self.Acct_Level_Summary["Net_Change_From_Market_Adjustment"] = self.Acct_Level_Summary["End_Value_Overwrite"] - self.Acct_Level_Summary["End_Value"]
    self.Acct_Level_Summary.sort_values(by=["acc_report_rank"],ascending=True,inplace=True)
    self.Acct_Level_Summary = self.Acct_Level_Summary[["acc_A/L/E_classification","acc_A/L/E_sign","acc_type","acc_ID","acc_name","Baseline_Value","Net_Change_From_Operations","Net_Change_From_Market_Adjustment","End_Value_Overwrite"]]

    # Calculating unrealized gain / loss due to Market Valuation
    Unrealized_gain = self.Acct_Level_Summary[self.Acct_Level_Summary["Net_Change_From_Market_Adjustment"] > 0]["Net_Change_From_Market_Adjustment"].sum()
    Unrealized_loss = self.Acct_Level_Summary[self.Acct_Level_Summary["Net_Change_From_Market_Adjustment"] < 0]["Net_Change_From_Market_Adjustment"].sum()

    self.Acct_Level_Summary.loc[self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Gain"),"End_Value_Overwrite"] = Unrealized_gain
    self.Acct_Level_Summary.loc[self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Gain"),"Net_Change_From_Market_Adjustment"] = Unrealized_gain

    self.Acct_Level_Summary.loc[self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Loss"),"End_Value_Overwrite"] = Unrealized_loss * -1
    self.Acct_Level_Summary.loc[self.Acct_Level_Summary["acc_name"].str.contains("Unrealized Investment Loss"),"Net_Change_From_Market_Adjustment"] = Unrealized_loss * -1

    # Keep only rows with at least one non-NULL value in {End_Value}
    self.Acct_Level_Summary = self.Acct_Level_Summary[(self.Acct_Level_Summary["Baseline_Value"] != 0) | (self.Acct_Level_Summary["End_Value_Overwrite"] != 0)]

    print('finished preparing Account-level report')

def get_account_type_level_balance_sheet(self):

    """This block of code calculates the start value, end value and delta for the three sub-items of the balance sheet
    For example, cash is a specific account type"""

    self.Class_Level_Summary = self.Acct_Level_Summary.groupby(["acc_A/L/E_classification","acc_A/L/E_sign","acc_type"],as_index=False)["Baseline_Value","Net_Change_From_Operations","Net_Change_From_Market_Adjustment","End_Value_Overwrite"].sum()
    Acc_type_priority = self.Accounts.groupby("acc_type",as_index=False)["acc_report_rank"].min()
    self.Class_Level_Summary = self.Class_Level_Summary.merge(Acc_type_priority,on="acc_type",how="left")
    self.Class_Level_Summary.sort_values(by=["acc_report_rank"],ascending=True,inplace=True)

    print('finished preparing Class-level report')

def get_overall_balance_sheet(self):

    """This block of code calculates the start value, end value and delta for the three line items of the balance
    balance - asset, liability and equity"""

    Parts = self.Class_Level_Summary.copy()

    Parts.loc[Parts["acc_A/L/E_sign"] == "[+ve]","Vector"] = 1
    Parts.loc[Parts["acc_A/L/E_sign"] == "[-ve]","Vector"] = -1
    Parts["Baseline_Value"] = Parts["Vector"] * Parts["Baseline_Value"]
    Parts["Net_Change_From_Operations"] = Parts["Vector"] * Parts["Net_Change_From_Operations"]
    Parts["Net_Change_From_Market_Adjustment"] = Parts["Vector"] * Parts["Net_Change_From_Market_Adjustment"]
    Parts["End_Value_Overwrite"] = Parts["Vector"] * Parts["End_Value_Overwrite"]

    self.BS_Level_Summary = Parts.groupby("acc_A/L/E_classification",as_index=False)[["Baseline_Value","Net_Change_From_Operations","Net_Change_From_Market_Adjustment","End_Value_Overwrite"]].sum()
    BS_Item_Priority = self.Accounts.groupby("acc_A/L/E_classification",as_index=False)["acc_report_rank"].min()

    self.BS_Level_Summary = self.BS_Level_Summary.merge(BS_Item_Priority,on="acc_A/L/E_classification",how="left")
    self.BS_Level_Summary.sort_values(by=["acc_report_rank"],ascending=True,inplace=True)

    print('finished preparing overall Balance Sheet')

def calculate_change(S,Transactions):

    """This block of code takes as input a list of accounts, and returns the (operational) net change in the amount of
    that account between Start_Date and End_Date as a result of the recorded transactions. Since each line of
    transaction involves two accounts, Acc_1 and Acc_2, we have to repeat this twice"""

    # (1) Amounts coming from Acc_1 side
    df_Acc_1 = Transactions[Transactions["Impacted_Acc_ID_1"].isin(S)]
    df_Acc_1 = df_Acc_1[["Impacted_Acc_ID_1","Tr_Date","Impacted_Acc_1_Sign","Impacted_Acc_1_Mag"]]
    df_Acc_1.rename(columns={"Impacted_Acc_ID_1":"Impacted_Acc_ID",
                             "Impacted_Acc_1_Sign":"Sign",
                             "Impacted_Acc_1_Mag":"Mag"},inplace=True)

    # (2) Amounts coming from Acc_2 side
    df_Acc_2 = Transactions[Transactions["Impacted_Acc_ID_2"].isin(S)]
    df_Acc_2 = df_Acc_2[["Impacted_Acc_ID_2","Tr_Date","Impacted_Acc_2_Sign","Impacted_Acc_2_Mag"]]
    df_Acc_2.rename(columns={"Impacted_Acc_ID_2":"Impacted_Acc_ID",
                                        "Impacted_Acc_2_Sign":"Sign",
                                        "Impacted_Acc_2_Mag":"Mag"},inplace=True)

    # (3) Append amounts coming from both sides
    df = df_Acc_1.append(df_Acc_2,ignore_index=True)

    # (4) Calculate net change
    df.loc[df["Sign"] == "[+ve]","Vector"] = 1
    df.loc[df["Sign"] == "[-ve]","Vector"] = -1
    df["Net_Change"] = df["Vector"] * df["Mag"]

    # (5) Group by account
    Change = df.groupby(["Impacted_Acc_ID"],as_index=False)["Net_Change"].sum()
    Change.sort_values(by=["Impacted_Acc_ID"],ascending=True,inplace=True)

    return Change

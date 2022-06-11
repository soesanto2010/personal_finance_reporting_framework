# This script performs various data checks

def verify_accounting_equation(self):

    """Check if the accounting equation: asset = liability + equity is preserved"""

    LHS = self.Class_Level_Summary[(self.Class_Level_Summary["acc_A/L/E_classification"]=="Asset") & (self.Class_Level_Summary["acc_A/L/E_sign"]=="[+ve]")]["End_Value_Overwrite"].sum() - self.Class_Level_Summary[(self.Class_Level_Summary["acc_A/L/E_classification"]=="Asset") & (self.Class_Level_Summary["acc_A/L/E_sign"]=="[-ve]")]["End_Value_Overwrite"].sum()
    RHS = self.Class_Level_Summary[(self.Class_Level_Summary["acc_A/L/E_classification"]!="Asset") & (self.Class_Level_Summary["acc_A/L/E_sign"]=="[+ve]")]["End_Value_Overwrite"].sum() - self.Class_Level_Summary[(self.Class_Level_Summary["acc_A/L/E_classification"]!="Asset") & (self.Class_Level_Summary["acc_A/L/E_sign"]=="[-ve]")]["End_Value_Overwrite"].sum()

    if abs(LHS - RHS) < 0.01:
        print("(1) ✓ Accounting Equation Preserved")
    else:
        print("(1) ❌ Account Equation Violated")

def ensure_closing_date_is_later_than_initiation_dates(self):

    """Check if closing date is always later than initial date in the transaction dataset"""

    Non_chronological_dates = len(self.Transactions_temp_2[self.Transactions_temp_2["tr_close_date"] < self.Transactions_temp_2["tr_init_date"]])

    if Non_chronological_dates > 0:
        print("(2) ❌ Non-chronological dates detected")
    else:
        print("(2) ✓ Dates Are Chronological")

def ensure_no_non_standard_account_names(self):

    """Check if there are non-standard account names in the transaction dataset"""

    Non_standard_acc_names = len(self.Transactions_temp_2[(self.Transactions_temp_2["Impacted_Acc_ID_1"].isnull()) | (self.Transactions_temp_2["Impacted_Acc_2"].isnull())])
    Non_standard_rev_names = len(self.Revenue_Subset[self.Revenue_Subset["inc_grp_ID"].isnull()])
    Non_standard_exp_names = len(self.Expenses_Subset[self.Expenses_Subset["exp_grp_ID"].isnull()])

    if Non_standard_acc_names + Non_standard_rev_names + Non_standard_exp_names > 0:
        print("(3) ❌ Non-standard account names detected")
    else:
        print("(3) ✓ Account names are standardized")

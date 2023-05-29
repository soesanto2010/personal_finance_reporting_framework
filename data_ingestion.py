# This script contains parts for reading + cleaning the transactions
# and other input datasets

import pandas as pd
from util import download_data
from util import calc_base_qty_and_cost_basis, calc_delta_qty_and_cost_basis
import numpy as np

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
        gcp_dataset,
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
        self.Transactions = download_data(
            self.path,
            self.filename,
            self.gcp_project,
            self.gcp_dataset,
            "Transactions",
            self.data_input_source,
        )
        self.Accounts = download_data(
            self.path,
            self.filename,
            self.gcp_project,
            self.gcp_dataset,
            "Accounts",
            self.data_input_source,
        )
        self.Expense_Picklist = download_data(
            self.path,
            self.filename,
            self.gcp_project,
            self.gcp_dataset,
            "tblpl_expense",
            self.data_input_source,
        )
        self.Expense_Group_Picklist = download_data(
            self.path,
            self.filename,
            self.gcp_project,
            self.gcp_dataset,
            "tblpl_expense_group",
            self.data_input_source,
        )
        self.Income_Picklist = download_data(
            self.path,
            self.filename,
            self.gcp_project,
            self.gcp_dataset,
            "tblpl_income",
            self.data_input_source,
        )
        self.Income_Group_Picklist = download_data(
            self.path,
            self.filename,
            self.gcp_project,
            self.gcp_dataset,
            "tblpl_income_group",
            self.data_input_source,
        )

        print("finished reading inputs")

    def preprocess_transactions(self):

        # (1) Drop unused columns
        self.Transactions.drop(
            columns=["tr_ID", "tr_supplier", "tr_qty_units", "tr_rate", "tr_notes"],
            inplace=True,
        )
        self.Accounts.drop(
            columns=["acc_notes", "acc_last_refresh", "acc_baseline_date"], inplace=True
        )
        self.Expense_Picklist.drop(columns=["exp_ID"], inplace=True)
        self.Income_Picklist.drop(columns=["inc_ID"], inplace=True)

        # (2) Add depreciation expenses
        self.Transactions_with_depex = self.add_fixture_depreciation_expense()
        print("finished adding depreciation expenses")

        # WIP: Add realized gain / losses
        Transactions_with_gain_and_loss = self.add_realized_gain_and_loss()
        print("finished adding realized gains and losses")

        # (3) Define Date-related variables
        Transactions_with_gain_and_loss["Tr_Date"] = pd.to_datetime(
            Transactions_with_gain_and_loss["tr_close_date"], format="%m/%d/%y"
        )
        Transactions_with_gain_and_loss["Tr_Week"] = (
            Transactions_with_gain_and_loss["tr_close_date"].dt.isocalendar().week
        )
        Transactions_with_gain_and_loss["Tr_Month"] = Transactions_with_gain_and_loss[
            "tr_close_date"
        ].dt.month
        Transactions_with_gain_and_loss["Tr_Year"] = (
            Transactions_with_gain_and_loss["tr_close_date"].dt.isocalendar().year
        )
        Transactions_with_gain_and_loss["Tr_Date"] = pd.to_datetime(
            Transactions_with_gain_and_loss["tr_close_date"]
        ).dt.date

        # (4) Keep only in-window transactions
        Transactions_relevant_timeframe = Transactions_with_gain_and_loss[
            Transactions_with_gain_and_loss["Tr_Date"] <= self.end_date
        ]

        # (5) Add Account ID
        Transactions_temp_1 = Transactions_relevant_timeframe.merge(
            self.Accounts.iloc[:, 0:2],
            left_on="tr_impacted_acc_1",
            right_on="acc_name",
            how="left",
        )
        self.Transactions_temp_2 = Transactions_temp_1.merge(
            self.Accounts.iloc[:, 0:2],
            left_on="tr_impacted_acc_2",
            right_on="acc_name",
            how="left",
            suffixes=("_1", "_2"),
        )

        # (6) Rename columns
        self.Transactions_temp_2.rename(
            columns={
                "acc_ID_1": "Impacted_Acc_ID_1",
                "acc_ID_2": "Impacted_Acc_ID_2",
                "tr_impacted_acc_1": "Impacted_Acc_1",
                "tr_impacted_acc_2": "Impacted_Acc_2",
                "tr_impacted_acc_1_sign": "Impacted_Acc_1_Sign",
                "tr_impacted_acc_2_sign": "Impacted_Acc_2_Sign",
            },
            inplace=True,
        )

        # (7) Keep only relevant columns
        self.Transactions = self.Transactions_temp_2.drop(
            columns=[
                "acc_name_1",
                "acc_name_2",
                "tr_init_date",
                "tr_close_date",
                "tr_SKU_lifetime",
            ]
        )
        print("finished preprocessing Transactions dataset")

    def add_fixture_depreciation_expense(self):

        """This block of code takes as input a list of capital expenditure
        (capex) transactions and generates a list of depreciation expense
        transactions (once for each month) between (1) the date of purchase
        and (2) the date of liquidation, which is currently set as purchase
        date + lifetime value (which is an input of the data)"""

        # (1) Pull transactions that correspond to capex investments
        capex = self.Transactions[
            self.Transactions["tr_expense"] == "Housing Expense - Fixture Investment"
        ][["tr_description", "tr_amt", "tr_close_date", "tr_SKU_lifetime"]]
        capex = (
            capex.reset_index()
        )  # reset has to be reset since we later perform indexing ops
        capex["monthly_depreciation"] = capex["tr_amt"] / capex["tr_SKU_lifetime"]

        # (2) Initialize the depreciation expense entries
        ls_SKU = []
        ls_depex = []
        ls_dep_date = []

        # (3) Fill in the depreciation expense for each item i in each
        # capex transaction
        for i in list(range(0, len(capex))):

            # (a) Extract the components
            SKU = capex.iloc[i, 1]
            start_date = capex.iloc[i, 3]
            max_months = int(capex.iloc[i, 4])
            # default is float, which gives error in indexing ops in
            # part (b)
            depex = capex.iloc[i, 5]

            # (b) Loop and create depex items for each PP&E, once for
            # each month, until the max. months is reached
            for j in list(range(1, max_months + 1)):
                end_dep_date = start_date + pd.DateOffset(months=j)
                ls_SKU += [SKU]
                ls_depex += [depex]
                ls_dep_date += [end_dep_date]

        depex_table = pd.DataFrame(
            {"tr_description": ls_SKU, "tr_amt": ls_depex, "tr_close_date": ls_dep_date}
        )

        # (4) Define  other columns for the depreciation expense transactions
        depex_agg = depex_table.groupby(["tr_close_date"], as_index=False)[
            "tr_amt"
        ].sum()
        depex_agg["tr_description"] = "Fixture Depreciation"
        depex_agg["tr_impacted_acc_1"] = "PP&E - Fixtures"
        depex_agg["tr_impacted_acc_1_sign"] = "[-ve]"
        depex_agg["tr_impacted_acc_2"] = "Expense - Housing"
        depex_agg["tr_impacted_acc_2_sign"] = "[+ve]"
        depex_agg["tr_expense"] = "Housing Expense - Depreciation"

        return self.Transactions.append(depex_agg, ignore_index=True)





    def add_realized_gain_and_loss(self):

        """This block of code takes as input a list of security sale transactions
        and generates the equivalent line item corresponding to the gain or loss
        by comparing the BV and MV at the time of sale"""

        columns_to_keep = ["tr_impacted_acc_1", "tr_impacted_acc_2", "tr_amt", "tr_qty", "tr_close_date", "wash_sale"]
        sale = self.Transactions_with_depex[self.Transactions_with_depex["security_transaction_flag"] == "Sale"][columns_to_keep]

        Transactions_copy = self.Transactions_with_depex.copy()
        Transactions_copy["Tr_Date"] = pd.to_datetime(Transactions_copy["tr_close_date"], format="%m/%d/%y")

        # (2) Initialize the realized gain/loss entries
        ls_cash_acc = []
        ls_security_acc = []
        ls_amount = []
        ls_qty = []
        ls_date = []
        ls_type = []

        # (3) Fill in the realized gain/loss expense for each item i in each security sale transaction
        for i in list(range(0, len(sale))):

            # (a) Extract the components
            cash_account = sale.iloc[i, 0]
            security_account = sale.iloc[i, 1]
            sale_proceeds = sale.iloc[i, 2]
            sale_qty = sale.iloc[i, 3]
            sale_date = sale.iloc[i, 4]
            sale_wash = sale.iloc[i, 5]

            # (b) For wash sale, we only look at purchase transactions in the past week

            if sale_wash == 1:
                t0 = sale_date - pd.Timedelta(days=7)
            else:
                t0 = np.datetime64(self.start_date)

            # (c) Get the base quantity and cost basis at inception
            (base_qty, base_cost_basis) = calc_base_qty_and_cost_basis(self.Accounts, security_account)

            # (d) Get the change in quantity and cost basis between the (i) start date and (ii) sale date
            (change_qty, change_cost_basis) = calc_delta_qty_and_cost_basis(Transactions_copy, security_account, "Tr_Date", t0, sale_date, "tr_impacted_acc_1", "tr_impacted_acc_2")

            # (e) Calculate the total cost basis at time of sale
            if sale_wash == 1:
                bv_per_share = change_cost_basis / change_qty
            else:
                bv_per_share = (base_cost_basis + change_cost_basis) / (base_qty + change_qty)
            cost_basis_at_sale = bv_per_share * sale_qty

            # (f) Calculate the realized gain (or loss)

            if cost_basis_at_sale <= sale_proceeds:
                type = 'Gain - Realized Investment Gain'
                amount = sale_proceeds - cost_basis_at_sale
            else:
                type = 'Loss - Realized Investment Loss'
                amount = cost_basis_at_sale - sale_proceeds

            # (g) Create journal entries

            ls_cash_acc += [cash_account]
            ls_security_acc += [security_account]
            ls_amount += [amount]
            ls_qty += [sale_qty]
            ls_date += [sale_date]
            ls_type += [type]

        gain_loss_table = pd.DataFrame({"tr_impacted_acc_1": ls_cash_acc,
                                        "tr_amt": ls_amount,
                                        "tr_qty": ls_qty,
                                        "tr_close_date": ls_date,
                                        "tr_impacted_acc_2": ls_type})

        # (4) Define other columns for the gain / loss transactions
        gain_loss_table["tr_description"] = "Realized Investment Gain / Loss"
        gain_loss_table["tr_init_date"] = gain_loss_table["tr_close_date"]
        gain_loss_table.loc[gain_loss_table['tr_impacted_acc_2'] == 'Gain - Realized Investment Gain', "tr_impacted_acc_1_sign"] = "[+ve]"
        gain_loss_table.loc[gain_loss_table['tr_impacted_acc_2'] == 'Loss - Realized Investment Loss', "tr_impacted_acc_1_sign"] = "[-ve]"
        gain_loss_table["tr_impacted_acc_2_sign"] = "[+ve]"
        gain_loss_table.loc[gain_loss_table['tr_impacted_acc_2'] == 'Gain - Realized Investment Gain', "tr_income"] = "Investment - Realized Gains"
        gain_loss_table.loc[gain_loss_table['tr_impacted_acc_2'] == 'Loss - Realized Investment Loss', "tr_expense"] = "Investment Expense - Realized Loss"

        self.gain_loss_table = gain_loss_table.copy()

        return self.Transactions_with_depex.append(gain_loss_table, ignore_index=True)

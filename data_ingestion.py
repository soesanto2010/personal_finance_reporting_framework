# This script contains parts for reading + cleaning the transactions
# and other input datasets

import pandas as pd
from util import download_data
from util import (
    calc_base_qty_and_cost_basis,
    calc_delta_qty_and_cost_basis,
    generate_deferred_tax_statements,
)
import numpy as np


class ingestion_pipeline(object):

    """Object to store shared state and processing methods"""

    def __init__(
        self,
        start_date,
        start_date_def_tax,
        end_date,
        timezone,
        max_pull_retries,
        security_price_metric,
        data_input_source,
        output_publish,
        output_publish_report,
        path,
        filename,
        gcp_project,
        gcp_dataset,
    ):
        """Initializes Pipeline object with shared state and inputs"""
        # (1) Run configs
        self.start_date = start_date
        self.start_date_def_tax = start_date_def_tax
        self.end_date = end_date
        self.timezone = timezone
        self.max_pull_retries = max_pull_retries
        self.security_price_metric = security_price_metric
        self.data_input_source = data_input_source
        self.output_publish = output_publish
        self.output_publish_report = output_publish_report
        self.path = path
        self.filename = filename
        self.gcp_project = gcp_project
        self.gcp_dataset = gcp_dataset
        self.Days_Ellapsed = (end_date - start_date).days
        self.asset_list_to_use_fallback = ["PP&E - Car"]
        self.balance_sheet_num_columns = [
            "Baseline_Value",
            "Net_Change_From_Operations",
            "BV",
            "CB",
            "Net_Change_From_Market_Adjustment",
            "MV",
        ]

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

        # (2) Define a datetime column in the transactions table
        # This is critical for filtering out transactions based on date intervals
        self.Transactions["Tr_Date"] = pd.to_datetime(
            self.Transactions["tr_close_date"], format="%m/%d/%y"
        )

        # (3) Add depreciation expenses
        self.Transactions_with_depex = self.add_fixture_depreciation_expense()
        print("finished adding depreciation expenses")

        # (4) Add realized gain / losses
        self.Transactions_with_gain_and_loss = self.add_realized_gain_and_loss()
        print("finished adding realized gains and losses")

        # (5) Add deferred tax statements based on realized income
        self.Transactions_with_deferred_tax_statements = (
            self.add_deferred_tax_transactions_based_on_realized_income()
        )
        print("finish adding deferred tax transactions based on realized income")

        # (6) Define Date-related variables
        self.Transactions_with_deferred_tax_statements["Tr_Week"] = (
            self.Transactions_with_deferred_tax_statements["tr_close_date"]
            .dt.isocalendar()
            .week
        )
        self.Transactions_with_deferred_tax_statements[
            "Tr_Month"
        ] = self.Transactions_with_deferred_tax_statements["tr_close_date"].dt.month
        self.Transactions_with_deferred_tax_statements["Tr_Year"] = (
            self.Transactions_with_deferred_tax_statements["tr_close_date"]
            .dt.isocalendar()
            .year
        )
        self.Transactions_with_deferred_tax_statements["Tr_Date"] = pd.to_datetime(
            self.Transactions_with_deferred_tax_statements["tr_close_date"]
        ).dt.date

        # (7) Keep only in-window transactions
        Transactions_relevant_timeframe = (
            self.Transactions_with_deferred_tax_statements[
                self.Transactions_with_deferred_tax_statements["Tr_Date"]
                <= self.end_date
            ]
        )

        # (8) Add Account ID
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

        # (9) Rename columns
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

        # (10) Keep only relevant columns
        self.Transactions_preprocessed = self.Transactions_temp_2.drop(
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
        depex_agg["Tr_Date"] = depex_agg["tr_close_date"]

        return pd.concat([self.Transactions, depex_agg], ignore_index=True)

    def add_realized_gain_and_loss(self):

        """This block of code takes as input a list of security sale transactions
        and splits the amount into (1) the proceeds based on total cost basis at sale
        and (2) the realized gain / loss by comparing the cost basis and MV at sale"""

        # (1) Pull the relevant datasets
        columns_to_keep = [
            "tr_impacted_acc_1",
            "tr_impacted_acc_2",
            "tr_amt",
            "tr_qty",
            "tr_close_date",
            "wash_sale",
        ]
        sale = self.Transactions_with_depex[
            self.Transactions_with_depex["security_transaction_flag"] == "Sale"
        ][columns_to_keep]

        # (2) Initialize the realized gain/loss entries
        ls_cash_acc = []
        ls_security_acc = []
        ls_amount = []
        ls_qty = []
        ls_date = []
        ls_type = []

        # (3) Fill in the realized gain/loss for each security sale transaction i
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
            (base_qty, base_cost_basis) = calc_base_qty_and_cost_basis(
                self.Accounts, security_account
            )

            # (d) Get the change in quantity and cost basis between the (i) start date
            # and (ii) sale date
            (change_qty, change_cost_basis) = calc_delta_qty_and_cost_basis(
                self.Transactions, security_account, "Tr_Date", t0, sale_date
            )

            # (e) Calculate the total cost basis at time of sale
            if sale_wash == 1:
                cb = change_cost_basis / change_qty
            else:
                cb = (base_cost_basis + change_cost_basis) / (base_qty + change_qty)
            CB = cb * sale_qty

            # (f) Calculate the realized gain (or loss)

            if CB <= sale_proceeds:
                type = "Gain - Realized Investment Gain"
                amount = sale_proceeds - CB
            else:
                type = "Loss - Realized Investment Loss"
                amount = CB - sale_proceeds

            # (g) Create separate journal entries for the gain / loss

            ls_cash_acc += [cash_account]
            ls_security_acc += [security_account]
            ls_amount += [amount]
            ls_qty += [sale_qty]
            ls_date += [sale_date]
            ls_type += [type]

            # (h) Change tr_amt in the transactions table to cost basis (since the
            # gain / loss component has been separated)

            self.Transactions_with_depex.loc[
                (
                    (
                        self.Transactions_with_depex["security_transaction_flag"]
                        == "Sale"
                    )
                    & (
                        self.Transactions_with_depex["tr_impacted_acc_1"]
                        == cash_account
                    )
                    & (
                        self.Transactions_with_depex["tr_impacted_acc_2"]
                        == security_account
                    )
                    & (self.Transactions_with_depex["tr_amt"] == sale_proceeds)
                    & (self.Transactions_with_depex["tr_qty"] == sale_qty)
                    & (self.Transactions_with_depex["tr_close_date"] == sale_date)
                ),
                "tr_amt",
            ] = CB

        gain_loss_table = pd.DataFrame(
            {
                "tr_impacted_acc_1": ls_cash_acc,
                "tr_amt": ls_amount,
                "tr_qty": ls_qty,
                "tr_close_date": ls_date,
                "tr_impacted_acc_2": ls_type,
            }
        )

        # (4) Define other columns for the gain / loss transactions
        gain_loss_table["tr_description"] = "Realized Investment Gain / Loss"
        gain_loss_table["tr_init_date"] = gain_loss_table["tr_close_date"]
        gain_loss_table.loc[
            gain_loss_table["tr_impacted_acc_2"] == "Gain - Realized Investment Gain",
            "tr_impacted_acc_1_sign",
        ] = "[+ve]"
        gain_loss_table.loc[
            gain_loss_table["tr_impacted_acc_2"] == "Loss - Realized Investment Loss",
            "tr_impacted_acc_1_sign",
        ] = "[-ve]"
        gain_loss_table["tr_impacted_acc_2_sign"] = "[+ve]"
        gain_loss_table.loc[
            gain_loss_table["tr_impacted_acc_2"] == "Gain - Realized Investment Gain",
            "tr_income",
        ] = "Investment - Realized Gains"
        gain_loss_table.loc[
            gain_loss_table["tr_impacted_acc_2"] == "Loss - Realized Investment Loss",
            "tr_expense",
        ] = "Investment Expense - Realized Loss"

        return pd.concat(
            [self.Transactions_with_depex, gain_loss_table], ignore_index=True
        )

    def add_deferred_tax_transactions_based_on_realized_income(self):

        """This block of code takes as input a list of realized transactions that
        would cause a tax liability and creates the necessary deferred tax statements
        based on those transactions"""

        # (1) Pull the relevant datasets
        df = self.Transactions_with_gain_and_loss.copy()
        deferred_tax_base_dataset = df[
            (df["tr_close_date"] >= np.datetime64(self.start_date_def_tax))
            & (df["tr_close_date"] <= np.datetime64(self.end_date))
            &
            # Exclude dividends / interests / capital gains going into 401(k) accounts
            (~df["tr_impacted_acc_1"].str.contains("Index Funds"))
            & (~df["tr_impacted_acc_2"].str.contains("Index Funds"))
            & (df["tr_impacted_acc_1"] != "A/R - Others")
        ]

        # (2) Pull in accounts that trigger deferred tax transactions
        revenue_streams_with_deferred_taxes = self.Income_Picklist[
            self.Income_Picklist["inc_deferred_tax_flag"] == 1
        ]
        revenue_streams_with_deferred_taxes.rename(
            columns={
                "inc_name": "acc_name",
                "inc_deferred_tax_federal_tax_rate": "federal_tax_rate",
                "inc_deferred_tax_state_tax_rate": "state_tax_rate",
                "inc_deferred_tax_FICA_tax_rate": "FICA_tax_rate",
            },
            inplace=True,
        )
        expense_streams_with_deferred_taxes = self.Expense_Picklist[
            self.Expense_Picklist["exp_deferred_tax_flag"] == 1
        ]
        expense_streams_with_deferred_taxes.rename(
            columns={
                "exp_name": "acc_name",
                "exp_deferred_tax_federal_tax_rate": "federal_tax_rate",
                "exp_deferred_tax_state_tax_rate": "state_tax_rate",
                "exp_deferred_tax_FICA_tax_rate": "FICA_tax_rate",
            },
            inplace=True,
        )
        accounts_with_deferred_taxes = pd.concat(
            [revenue_streams_with_deferred_taxes, expense_streams_with_deferred_taxes]
        )

        # (3) Create the deferred tax transactions
        relevant_subset = generate_deferred_tax_statements(
            deferred_tax_base_dataset, accounts_with_deferred_taxes
        )
        def_tax_transactions = relevant_subset[
            [
                "tr_description",
                "tr_amt",
                "tr_init_date",
                "tr_close_date",
                "tr_impacted_acc_1",
                "tr_impacted_acc_1_sign",
                "tr_impacted_acc_2",
                "tr_impacted_acc_2_sign",
            ]
        ]

        return pd.concat(
            [self.Transactions_with_gain_and_loss, def_tax_transactions],
            ignore_index=True,
        )

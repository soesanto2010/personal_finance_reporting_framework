# This script stores the functions to pull the market values of marketable securities

import yfinance as yf
import pandas as pd
from balance_sheet_calculations import calculate_change
from util import calc_base_qty_and_cost_basis, calc_delta_qty_and_cost_basis
import numpy as np


def get_quantities_on_closing_date(self):

    """This block of code takes as input a list of accounts, and returns the
    (operational) net change in the quantity of shares of that account between
    Start_Date and End_Date as a result of the security trades. Since each line of
    transaction involves two accounts, Acc_1 and Acc_2, we repeat this twice"""

    # (1) Get baseline quantities on starting date
    security_accounts = list(
        self.Accounts[~self.Accounts["acc_security_ticker"].isna()][
            "acc_ID"
        ].drop_duplicates()
    )
    baseline_quantity = self.Accounts[~self.Accounts["acc_security_ticker"].isna()][
        [
            "acc_ID",
            "acc_name",
            "acc_sub_type",
            "acc_security_ticker",
            "acc_baseline_qty",
            "acc_security_value_fallback",
            "acc_security_sale_commision_percentage",
        ]
    ]

    # (2) Calculate the change in quantities based on trade of the securities
    delta_quantity = calculate_change(
        security_accounts, self.Transactions_preprocessed, "tr_qty"
    )
    self.securities = baseline_quantity.merge(
        delta_quantity, left_on=["acc_ID"], right_on=["Impacted_Acc_ID"], how="left"
    )
    self.securities.fillna(value=0, inplace=True)

    # (3) Calculate the end quantity
    self.securities["End_Quantity"] = (
        self.securities["acc_baseline_qty"] + self.securities["Net_Change"]
    )


def get_market_values_on_closing_date(self):

    """Adds to the securities table the:
    (1) mv, the market value per share (in USD/share). The fallback is used if no
    market value is pulled
    (2) mv_pull_date, the market value pull date
    (3) mv_source_type, the market value source type
    (4) MV, the total market value for all shares (in USD)
    """

    # (1) Pull from yfinance
    for i in list(self.securities["acc_security_ticker"]):
        (i_mv, i_mv_pull_date, i_mv_source_type) = get_security_value_using_yfinance(
            ticker_key=i,
            date_key=self.end_date,
            max_retries=self.max_pull_retries,
            price_metric=self.security_price_metric,
        )
        self.securities.loc[self.securities["acc_security_ticker"] == i, "mv"] = i_mv
        self.securities.loc[
            self.securities["acc_security_ticker"] == i, "mv_pull_date"
        ] = i_mv_pull_date
        self.securities.loc[
            self.securities["acc_security_ticker"] == i, "mv_source_type"
        ] = i_mv_source_type

    # (2) Reformat the pull date to date (so that it can be saved into CSV)
    self.securities["mv_pull_date"] = pd.to_datetime(
        self.securities["mv_pull_date"]
    ).dt.date

    # (3) Use the fallback if the market value per share is not successfully pulled
    self.securities.loc[self.securities["mv"].isna(), "mv"] = self.securities[
        "acc_security_value_fallback"
    ]

    # (4) Calculate the total market value
    self.securities["MV_pre_commisions"] = (
        self.securities["End_Quantity"] * self.securities["mv"]
    )

    # (5) Calculate the total (final) market value (after deducting commisions and fees)
    self.securities["acc_security_sale_commision_percentage"].fillna(0)
    self.securities["MV"] = self.securities["MV_pre_commisions"] * (
        1 - self.securities["acc_security_sale_commision_percentage"]
    )


def get_cost_basis_on_closing_date(self):

    """Adds to the securities table CB, the total cost basis for all shares (in USD)"""

    # (1) Pull the relevant datasets
    subset_security_account = self.securities[["acc_name", "End_Quantity"]]

    # (2) For each security, we determine the:
    # (a) baseline qty
    # (b) baseline total cost basis
    # (c) change in qty
    # (d) change in total cost basis
    # We then use these to calculate the qty and total cost basis at closing

    for i in list(range(0, len(self.securities))):

        # (a) Extract the components
        security_account = subset_security_account.iloc[i, 0]
        end_quantity = subset_security_account.iloc[i, 1]

        # (b) Get the base quantity and cost basis at inception
        (base_qty, base_cost_basis) = calc_base_qty_and_cost_basis(
            self.Accounts, security_account
        )

        # (c) Get the change in quantity and cost basis between the (i) start date
        # and (ii) closing date
        (change_qty, change_cost_basis) = calc_delta_qty_and_cost_basis(
            self.Transactions,
            security_account,
            "Tr_Date",
            np.datetime64(self.start_date),
            np.datetime64(self.end_date),
        )

        # (d) Calculate cost basis per share (cb) and total cost basis at closing (CB)
        cb = (base_cost_basis + change_cost_basis) / (base_qty + change_qty)
        CB = cb * end_quantity

        # (e) Insert the total cost basis into the table
        self.securities.loc[self.securities["acc_name"] == security_account, "CB"] = CB


def get_security_value_using_yfinance(ticker_key, date_key, max_retries, price_metric):

    """The core function in yfinance pulls security values based on the (1) ticker
    and (2) datetime as inputs. If the function fails (for example, because the day
    falls on a market close), we retry based on the previous date"""

    # (1) Initial parameters
    day_decrement = 0
    updated_date = date_key - pd.Timedelta(days=day_decrement)

    # (2) Initial database pull
    df = yf.download(tickers=ticker_key, start=updated_date, end=updated_date)

    # (3) Repull database by trying previous day if fails (max retries dictated by
    # default_max_yfinance_pull_retries)
    while len(df) == 0 and day_decrement <= max_retries:
        print(
            "Unable to retrieve the security value of "
            + ticker_key
            + " on "
            + str(updated_date)
        )
        day_decrement = day_decrement + 1
        updated_date = date_key - pd.Timedelta(days=day_decrement)
        df = yf.download(tickers=ticker_key, start=updated_date, end=updated_date)

    # (4) Pull out the security value of the price metric (price metric dictated by
    # default_security_price_metric)
    if date_key == updated_date:
        security_value = list(df[price_metric])[0]
        print("Successful value pull for " + ticker_key + " on target closing date")
        return security_value, date_key, "exact"
    elif len(df) > 0:
        security_value = list(df[price_metric])[0]
        print("Alternative value pull for " + ticker_key + " on " + str(updated_date))
        return security_value, updated_date, "est based on prev close"
    else:
        security_value = None
        print(
            "Pull still unsuccessful for "
            + ticker_key
            + " after "
            + str(max_retries)
            + " tries"
        )
        return security_value, date_key, "fallback"

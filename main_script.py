# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 20:55:14 2021

@author: vsoesanto
"""

#####################################################################################

# Part 1: Import necessary packages

# (a) External packages
import pandas as pd

# (b) Internal packages
from data_ingestion import ingestion_pipeline
from security_valuation import (
    get_quantities_on_closing_date,
    get_market_values_on_closing_date,
)
from balance_sheet_calculations import (
    get_account_level_balance_sheet,
    get_account_type_level_balance_sheet,
    get_overall_balance_sheet,
)
from income_statement_calculations import (
    calculate_financial_KPIs,
    generate_profit_and_loss_statement,
)
from run_qc_tests import (
    verify_accounting_equation,
    ensure_closing_date_is_later_than_initiation_dates,
    ensure_no_non_standard_account_names,
)
from output_generation import generate_csv_outputs

#####################################################################################

# Part 2: Set run parameters

# start date for tracking expenses
default_start_date = "2021-06-25 00:00:00"

# end date for tracking expenses
# (enter 'YYYY-MM-DD HH:MM:SS' to close on a specific(past) date; otherwise, enter
# None to close on the current datetime)
default_end_date = None

default_timezone = "UTC"
default_max_pull_retries = 5

# default type of price for measuring the value of securities. Options include
# {Open, High, Low, Close, Adj Close}
default_security_price_metric = "Adj Close"

# default source for datasets is 'cloud', but 'local' can be used during dev ops
default_data_input_source = "local"

# Cloud path
default_gcp_project = "vsoesanto-gcp-finance-prod"
default_gcp_dataset = "personal_finance"

# Local path
default_path = "C:\\Users\\feiya\\OneDrive\\Desktop\\Financial Management\\"
default_filename = "Data Structure.xlsx"

#####################################################################################

# Part 3: Determine the start & end dates for the run

Start_Date = pd.Timestamp(default_start_date, tz=default_timezone)

if default_end_date is None:
    End_Date = pd.Timestamp(pd.Timestamp.now(), tz=default_timezone)
else:
    End_Date = pd.Timestamp(default_end_date, tz=default_timezone)

#####################################################################################

# Part 4: Main script


def main():

    # (a) Initialization & cleaning (most will directly use the defaults in Part (3)
    # without additional cleaning)
    datasets = ingestion_pipeline(
        start_date=Start_Date,
        end_date=End_Date,
        timezone=default_timezone,
        max_pull_retries=default_max_pull_retries,
        security_price_metric=default_security_price_metric,
        data_input_source=default_data_input_source,
        path=default_path,
        filename=default_filename,
        gcp_project=default_gcp_project,
        gcp_dataset=default_gcp_dataset,
    )
    datasets.preprocess_transactions()

    # (b) Determine the total market values of securities on closing date
    get_quantities_on_closing_date(datasets)
    get_market_values_on_closing_date(datasets)

    # (c) Get balance sheet level items
    get_account_level_balance_sheet(datasets)
    get_account_type_level_balance_sheet(datasets)
    get_overall_balance_sheet(datasets)

    # (d) Get income statement by period
    generate_profit_and_loss_statement(datasets)

    # (e) Ratio calculations
    calculate_financial_KPIs(datasets)

    # (f) Run QC tests
    verify_accounting_equation(datasets)
    ensure_closing_date_is_later_than_initiation_dates(datasets)
    ensure_no_non_standard_account_names(datasets)

    # (g) Generate output files
    generate_csv_outputs(datasets)

    print("run completed")


if __name__ == "__main__":
    main()

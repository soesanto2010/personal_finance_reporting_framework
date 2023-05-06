# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 20:55:14 2021

@author: vsoesanto
"""

##############################################################################################################

# Part 1: Import necessary packages

import datetime
from data_ingestion import ingestion_pipeline
from balance_sheet_calculations import get_account_level_balance_sheet, get_account_type_level_balance_sheet, get_overall_balance_sheet
from income_statement_calculations import get_revenue_trend, get_expenses_trend, calculate_financial_KPIs
from run_qc_tests import verify_accounting_equation, ensure_closing_date_is_later_than_initiation_dates, ensure_no_non_standard_account_names
from output_generation import generate_csv_outputs

##############################################################################################################

# Part 2: Set run parameters

Start_Date = datetime.date(2021,6,25)        # start of date for tracking expenses
End_Date = datetime.date(2023,5,5)
path = 'C:\\Users\\feiya\\OneDrive\\Desktop\\Financial Management\\'
filename = 'Data Structure.xlsx'

##############################################################################################################

# Part 3: Main script

def main(path,filename,Start_Date,End_Date):

    # (1) Initialization & cleaning
    data_path = path
    End_Date = End_Date
    Start_Date = Start_Date
    filename = filename
    datasets = ingestion_pipeline(
        path=data_path,
        filename=filename,
        start_date=Start_Date,
        end_date=End_Date)
    datasets.preprocess_transactions()

    # (2) Get balance sheet level items
    get_account_level_balance_sheet(datasets)
    get_account_type_level_balance_sheet(datasets)
    get_overall_balance_sheet(datasets)

    # (3) Get revenue and expense trends
    get_revenue_trend(datasets)
    get_expenses_trend(datasets)

    # (4) Ratio calculations
    calculate_financial_KPIs(datasets)

    # (5) Run QC tests
    verify_accounting_equation(datasets)
    ensure_closing_date_is_later_than_initiation_dates(datasets)
    ensure_no_non_standard_account_names(datasets)

    # (6) Generate output files
    generate_csv_outputs(datasets)

    print("run completed")

if __name__ == "__main__":
    main(
        path=path,
        filename=filename,
        End_Date=End_Date,
        Start_Date =Start_Date
    )

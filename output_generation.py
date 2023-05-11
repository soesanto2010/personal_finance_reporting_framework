# This script writes the output to a local location

import pandas as pd

def generate_csv_outputs(self):

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(self.path+'Output.xlsx', engine='xlsxwriter')

    # Write each dataframe to a different worksheet
    self.BS_Level_Summary.to_excel(writer, sheet_name='Overall')
    self.Class_Level_Summary.to_excel(writer, sheet_name='Class')
    self.Acct_Level_Summary.to_excel(writer, sheet_name='Account')
    self.Revenue_Breakdown.to_excel(writer, sheet_name='Revenue_Breakdown')
    self.Revenue_Trend.to_excel(writer, sheet_name='Revenue_Trend')
    self.Expense_Pivot.to_excel(writer,sheet_name='Expenses_Trend')
    self.Summary_KPI.to_excel(writer,sheet_name='Summary_KPI')
    self.securities.to_excel(writer, sheet_name='Security_valuations')

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    print("finish writing outputs")

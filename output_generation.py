# This script writes the output to a local location

import pandas as pd


def generate_csv_outputs(self):

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(self.path + "Output.xlsx", engine="xlsxwriter")

    # Write each dataframe to a different worksheet
    self.BS_Level_Summary.to_excel(writer, sheet_name="Overall")
    self.Class_Level_Summary.to_excel(writer, sheet_name="Class")
    self.Acct_Level_Summary.to_excel(writer, sheet_name="Account")
    self.P_L_statement_overall.to_excel(writer, sheet_name="monthly_P_L_overall")
    self.P_L_statement_deep_dive.to_excel(writer, sheet_name="monthly_P_L_deep_dive")
    self.securities.to_excel(writer, sheet_name="Security_valuations")

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

    print("finish writing outputs")

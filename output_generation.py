# This script writes the output to a local location

import pandas as pd
from util import load_to_bigquery

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

    print("finish writing outputs locally")

def output_upload_to_cloud(self):

    # (1) Set the publish date for the balance sheet dataset
    self.Acct_Level_Summary['publish_date'] = self.end_date

    # (2) Upload to BigQuery
    load_to_bigquery(self.Acct_Level_Summary, self.gcp_project, self.gcp_dataset, "tbl_"+self.output_publish_report+"_balance_sheet", "overwrite")
    load_to_bigquery(self.time_trend, self.gcp_project, self.gcp_dataset, "tbl_dataset_income_statement", "overwrite")

    self.securities.rename(
        columns={"mv": "market_value_per_share"},
        inplace=True)
    load_to_bigquery(self.securities, self.gcp_project, self.gcp_dataset, "tbl_recent_securities_valuation", "overwrite")

    print("finish uploading outputs to cloud")



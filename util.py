from google.cloud import bigquery
import pandas as pd


def load_to_bigquery(df, project, dataset, table, load_type):

    """The following function loads data to bigquery table"""

    if load_type == "overwrite":
        load_input = "WRITE_TRUNCATE"
    elif load_type == "append":
        load_input = "WRITE_APPEND"
    else:
        print("input error")

    dataset_table_name = dataset + "." + table
    client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(write_disposition=load_input)
    client.load_table_from_dataframe(df, dataset_table_name, job_config=job_config)
    print("finish " + load_type + " of " + project + "." + dataset + "." + table)


def read_from_bigquery(project, dataset, table):

    """The following function reads data from bigquery table"""

    # (1) Determine the query
    table_to_query = project + "." + dataset + "." + table
    sql_query = """SELECT * FROM """ + table_to_query

    # (2) Query the data
    client = bigquery.Client(project=project)
    return client.query(sql_query).to_dataframe(create_bqstorage_client=False)


def read_from_local_folder(path, filename, sheet_name):

    """The following function reads data from a local file"""

    return pd.read_excel(path + filename, sheet_name=sheet_name)


def download_data(
    local_path, local_filename, cloud_project, cloud_dataset, table, data_input_source
):

    """The following function downloads data from either local or cloud path"""

    if data_input_source == "cloud":
        print("pulling " + table + " from cloud")
        return read_from_bigquery(cloud_project, cloud_dataset, table)
    elif data_input_source == "local":
        print("pulling " + table + " from local directory")
        return read_from_local_folder(local_path, local_filename, table)
    else:
        print("invalid input for data_input_source")


def calc_base_qty_and_cost_basis(accounts_table, security_account):

    """Calculates quantity and total cost basis for a security at the baseline date"""

    base_qty = list(
        accounts_table[accounts_table["acc_name"] == security_account][
            "acc_baseline_qty"
        ]
    )[0]
    base_cost_basis = list(
        accounts_table[accounts_table["acc_name"] == security_account][
            "acc_baseline_cost_basis"
        ]
    )[0]

    return base_qty, base_cost_basis


def calc_delta_qty_and_cost_basis(
    transactions_table, security_account, date_key, t0, tf
):

    """Determines change in quantity and total cost basis for a security between
    the (i) start_date (t0) and (2) end_date (tf)"""

    relevant_purchases = transactions_table[
        (transactions_table["security_transaction_flag"] == "Purchase")
        & (
            (transactions_table["tr_impacted_acc_1"] == security_account)
            | (transactions_table["tr_impacted_acc_2"] == security_account)
        )
        & (transactions_table[date_key] < tf)
        & (transactions_table[date_key] >= t0)
    ]

    change_qty = relevant_purchases["tr_qty"].sum()
    change_cost_basis = relevant_purchases["tr_amt"].sum()

    return change_qty, change_cost_basis


def generate_deferred_tax_statements(transactions_to_convert, deferred_accounts):

    """Generate deferred tax statements"""

    # (1) Pull the set of transactions that would trigger deferred tax liability
    income = transactions_to_convert.merge(
        deferred_accounts, left_on="tr_income", right_on="acc_name", how="left"
    )
    income = income[income["inc_deferred_tax_flag"] == 1]
    expenses = transactions_to_convert.merge(
        deferred_accounts, left_on="tr_expense", right_on="acc_name", how="left"
    )
    expenses = expenses[expenses["exp_deferred_tax_flag"] == 1]
    expenses["tr_amt"] = expenses["tr_amt"] * -1
    agg_transactions = pd.concat([income, expenses])

    # (2) For transactions that cannot offset together, group them directly:
    non_offsetting_transactions = agg_transactions[
        agg_transactions["tr_description"] != "Realized Investment Gain / Loss"
    ]
    non_offsetting_transactions = non_offsetting_transactions.groupby(
        ["tr_description", "federal_tax_rate", "state_tax_rate", "FICA_tax_rate"],
        as_index=False,
    )["tr_amt"].sum()

    # (3) For transactions that can offset one another in each year, like
    # capital gains/loss and business profits/losses), we (a) offset them in each year,
    # and (b) set the net to 0 if the net is negative (since the net loss cannot be used
    # to offset non-offsetting income or ordinary income
    offseting_transactions = agg_transactions[
        agg_transactions["tr_description"] == "Realized Investment Gain / Loss"
    ]
    offseting_transactions["Tr_Year"] = (
        offseting_transactions["tr_close_date"].dt.isocalendar().year
    )
    offsetting_transactions_grouped = offseting_transactions.groupby(
        [
            "tr_description",
            "federal_tax_rate",
            "state_tax_rate",
            "FICA_tax_rate",
            "Tr_Year",
        ],
        as_index=False,
    )["tr_amt"].sum()
    offsetting_transactions_grouped.loc[
        offsetting_transactions_grouped["tr_amt"] <= 0, "tr_amt"
    ] = 0
    offsetting_transactions_grouped.drop(columns=["Tr_Year"], inplace=True)

    # (4) Append the non-offsetting and offsetting transactions together
    df = pd.concat([non_offsetting_transactions, offsetting_transactions_grouped])

    # (5) Calculate the deferred tax amount
    df["deferred_tax_amount"] = df["tr_amt"] * (
        df["federal_tax_rate"] + df["state_tax_rate"] + df["FICA_tax_rate"]
    )

    # (6) Fill in the column values for the deferred tax transactions
    df["tr_init_date"] = agg_transactions["tr_close_date"].max()
    df["tr_close_date"] = agg_transactions["tr_close_date"].max()
    df["tr_description"] = "Deferred taxes"
    df["tr_amt"] = df["deferred_tax_amount"]
    df["tr_impacted_acc_1"] = "Deferred Taxes Payable"
    df["tr_impacted_acc_1_sign"] = "[+ve]"
    df["tr_impacted_acc_2"] = "Expense - Deferred Taxes (Est)"
    df["tr_impacted_acc_2_sign"] = "[+ve]"

    return df

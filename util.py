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

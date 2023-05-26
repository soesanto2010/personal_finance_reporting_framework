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

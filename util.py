from google.cloud import bigquery

def load_to_bigquery(df, project, dataset, table, load_type):

    """The following function loads data to bigquery table"""

    if load_type == 'overwrite':
        load_input = 'WRITE_TRUNCATE'
    elif load_type == 'append':
        load_input = 'WRITE_APPEND'
    else:
        print("input error")

    dataset_table_name = dataset + "." + table
    client = bigquery.Client(project=project)
    job_config = bigquery.LoadJobConfig(write_disposition=load_input)
    client.load_table_from_dataframe(df, dataset_table_name, job_config=job_config)
    print("finish "+load_type+" of "+project+"."+dataset+"."+table)
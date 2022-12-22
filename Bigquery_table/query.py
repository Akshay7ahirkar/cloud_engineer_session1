from google.cloud import bigquery
if __name__ == '__main__':
    bqclient = bigquery.Client()

    query = """
    with data_input1 as
    (
       select 1 as id,
       ["comedy","sports","news"] as category,
       b"lkasjdf" as byteval
    )
     SELECT * FROM `atomic-hash-364705.BWT_Dataengg.emp10` t join data_input1 t1 on t.emp_id=t1.id;
    """
    query_job= bqclient.query(query)
    for row in query_job:
        print(row)

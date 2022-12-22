from google.cloud import bigquery

if __name__ == '__main__':
    obj_client = bigquery.Client()

    load_conf = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("dept_id", "integer"),
            bigquery.SchemaField("dept_name", "string"),
            bigquery.SchemaField("dept_date", "timestamp")
        ],
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.YEAR,
            field="dept_date"
        )
    )

    gcs_path = "gs://bwt_session_bucket1/dept.csv"

    load_job = obj_client. \
        load_table_from_uri(project="hopeful-text-362215",
                            source_uris=gcs_path,
                            destination="BWT_Session_dataset1.dept1",
                            location= "US",
                            job_config= load_conf,
                            job_id_prefix = "loadcsv")
    
    load_job.result()

    table = obj_client.get_table("BWT_Session_dataset1.dept1")

    print("successfully load data into bigquery table: {}, numbers of records loaded:{}".format("BWT_Session_dataset1.dept1",table.num_rows))


from google.cloud import bigquery
from google.oauth2 import service_account
if __name__ == '__main__':
    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-2c9b6b3e6b42.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = "stellar-sunrise-352209"

    obj_client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    schema = [
        bigquery.SchemaField(name="std_id", field_type="INTEGER"),
        bigquery.SchemaField("std_name", "STRING"),
        bigquery.SchemaField(name="GENDER", field_type="STRING"),
        bigquery.SchemaField(name="admission_date", field_type="Date")
    ]
    table = bigquery.Table("stellar-sunrise-352209.bwt_session_dataset.student_tb1",schema=schema)
    print("table object created: {}".format(table))

    table.range_partitioning = bigquery.RangePartitioning(field="std_id",range_=bigquery.PartitionRange(start=0,end=1000,interval=10)
    )
    table = obj_client.create_table(table)
    print("created table {}.{}.{}".format(table.project,table.dataset_id,table.table_id))

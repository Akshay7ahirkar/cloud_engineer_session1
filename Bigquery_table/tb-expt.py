from google.cloud import bigquery
client = bigquery.Client()
bucket_name = 'ak2/ak1'
project = "atomic-hash-364705"
dataset_id = "BWT_Dataengg"
table_id = "emp10"

destination_uri = "gs://{}/{}".format(bucket_name, "clib.csv")
dataset_ref = bigquery.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table(table_id)
extract_job = client.extract_table(table_ref,destination_uri)
extract_job.result()

print("Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri))
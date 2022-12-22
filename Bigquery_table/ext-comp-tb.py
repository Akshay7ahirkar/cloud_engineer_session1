from google.cloud import bigquery
client = bigquery.Client()
bucket_name = 'ak2/ak1'

destination_uri = "gs://{}/{}".format(bucket_name, "kr.csv.gz")
dataset_ref = bigquery.DatasetReference("atomic-hash-364705","BWT_Dataengg")
table_ref = dataset_ref.table("emp10")
job_config = bigquery.job.ExtractJobConfig()
job_config.compression = bigquery.Compression.GZIP

extract_job = client.extract_table(table_ref,destination_uri,job_config=job_config)
extract_job.result()
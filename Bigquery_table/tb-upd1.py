import datetime

from google.cloud import bigquery
client = bigquery.Client()
project = client.project
dataset_ref = bigquery.DatasetReference("atomic-hash-364705","BWT_Session")
table_ref = dataset_ref.table('employee')
table = client.get_table(table_ref)


expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
    days=6
)
table.expires = expiration
table = client.update_table(table, ["expires"])


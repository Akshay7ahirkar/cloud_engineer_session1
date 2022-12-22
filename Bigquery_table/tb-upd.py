from google.cloud import bigquery
client = bigquery.Client()
project = client.project
dataset_ref = bigquery.DatasetReference("atomic-hash-364705","BWT_Session")
table_ref = dataset_ref.table('employee1_Recovered')
table = client.get_table(table_ref)

table.description = "I am Akshay  Divya"

table = client.update_table(table, ["description"])


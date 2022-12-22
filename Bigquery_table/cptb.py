from google.cloud import bigquery
client = bigquery.Client()
source_table_id = "atomic-hash-364705.BWT_Session.emp"

destination_table_id = "atomic-hash-364705.BWT_Class.employee1"

job = client.copy_table(source_table_id, destination_table_id)
job.result()

print("A copy of the table created.")
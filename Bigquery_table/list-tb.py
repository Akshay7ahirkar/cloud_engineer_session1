
from google.cloud import bigquery

client = bigquery.Client()

dataset_id = 'atomic-hash-364705.BWT_Session'

tables = client.list_tables(dataset_id)

print("Tables contained in '{}':".format(dataset_id))
for table in tables:
    print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

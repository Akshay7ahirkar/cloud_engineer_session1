from google.cloud import bigquery

client = bigquery.Client()


table_id = 'atomic-hash-364705.BWT_Session.employee'

table = client.get_table(table_id)

print(
    "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
)
print("Table schema: {}".format(table.schema))
print("Table description: {}".format(table.description))
print("Table has {} rows".format(table.num_rows))
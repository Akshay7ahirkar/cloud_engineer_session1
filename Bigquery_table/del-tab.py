from google.cloud import bigquery

client = bigquery.Client()

table_id = 'atomic-hash-364705.BWT_Session.employee1'

client.delete_table(table_id, not_found_ok=True)
print("Deleted table '{}'.".format(table_id))
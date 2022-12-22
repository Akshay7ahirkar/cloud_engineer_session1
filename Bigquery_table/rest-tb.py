import time

from google.cloud import bigquery
client = bigquery.Client()
table_id = "atomic-hash-364705.BWT_Class.employee1"
recovered_table_id = "atomic-hash-364705.BWT_Session.employee1_Recovered"
snapshot_epoch = int(time.time() * 1000)

client.delete_table(table_id)

snapshot_table_id = "{}@{}".format(table_id,snapshot_epoch)

job = client.copy_table(snapshot_table_id,recovered_table_id)

job.result()

print("Copied data from deleted table {} to {}".format(table_id, recovered_table_id))
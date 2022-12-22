from google.cloud import bigquery
client = bigquery.Client()

desttable_id = "atomic-hash-364705.BWT_Dataengg.emp10"
table_ids = ["atomic-hash-364705.BWT_Session.emp","atomic-hash-364705.BWT_Class.employee1"]

job = client.copy_table(table_ids, desttable_id)
job.result()

print("The tables {} have been appended to {}".format(table_ids, desttable_id))
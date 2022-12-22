from google.cloud import bigquery

client = bigquery.Client()

table_id = "atomic-hash-364705.BWT_Dataengg.emp10"

rows_iter = client.list_rows(table_id)
rows = list(rows_iter)
print("Downloaded {} rows from table {}".format(len(rows), table_id))

table = client.get_table(table_id)
fields = table.schema[:]
rows_iter = client.list_rows(table_id, selected_fields=fields)
rows = list(rows_iter)
print("Selected {} columns from table {}.".format(len(rows_iter.schema), table_id))
rows = client.list_rows(table)
format_string = "{!s:<16} " * len(rows.schema)
field_names = [field.name for field in rows.schema]
print(format_string.format(*field_names))
for row in rows:
    print(format_string.format(*row))

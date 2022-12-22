from google.cloud import bigquery

client = bigquery.Client()

table_id = "hopeful-text-362215.BWT_Session_dataset1.department"

table = client.get_table(table_id)
original_schema = table.schema
new_schema = original_schema[:]
new_schema.append(bigquery.SchemaField("phone", "STRING"))

table.schema = new_schema
table = client.update_table(table, ["schema"])

if len(table.schema) == len(original_schema) + 1 == len(new_schema):
    print("A new column has been added.")
else:
    print("The column has not been added.")
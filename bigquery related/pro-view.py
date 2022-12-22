from google.cloud import bigquery
from google.oauth2 import service_account
if __name__ == '__main__':
    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-2c9b6b3e6b42.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = "stellar-sunrise-352209"

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    view_id = "stellar-sunrise-352209.bwt_aks.Employee5"
    view = client.get_table(view_id)

    print(f"Retrieved {view.table_type}: {str(view.reference)}")
    print(f"View Query:\n{view.view_query}")
    print(f"View Description:\n{view.description}")
    print(f"View Use legacy sql:\n{view.view_use_legacy_sql}")
    print(f"View lables:\n{view.labels}")
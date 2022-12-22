from google.cloud import bigquery
from google.oauth2 import service_account
if __name__ == '__main__':
    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-2c9b6b3e6b42.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = "stellar-sunrise-352209"

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    view_id = "stellar-sunrise-352209.bwt_session_dataset.Employee4"
    source_id = "stellar-sunrise-352209.bwt_session_dataset.Employee"
    view = bigquery.Table(view_id)
    view.view_query = "Select * FROM stellar-sunrise-352209.bwt_aks.Employee"

    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")
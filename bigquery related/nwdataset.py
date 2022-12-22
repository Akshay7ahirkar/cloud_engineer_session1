from google.cloud import bigquery
from google.oauth2 import service_account
if __name__ == '__main__':
    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-2c9b6b3e6b42.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = "stellar-sunrise-352209"
    dataset_id = "bwt_class_dataset"

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )

    client.delete_dataset(dataset_id,delete_contents= True ,not_found_ok=True)
    print("deleted dataset '{}'".format(dataset_id))

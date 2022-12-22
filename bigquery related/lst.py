from google.cloud import bigquery
from google.oauth2 import service_account
if __name__ == '__main__':
    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-2c9b6b3e6b42.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = "stellar-sunrise-352209"

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    datasets = list(client.list_datasets())
    project = client.project
    if datasets:
        print("datasets in project{}:" ,format(project))
        for dataset in datasets:
            print("\t{}".format(dataset.dataset_id))
    else:
        print("{} project does not contain any datasets.".format(project) )
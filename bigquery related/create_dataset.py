from google.cloud import bigquery
from google.oauth2 import service_account
if __name__ == '__main__':
    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-2c9b6b3e6b42.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    project_id = "stellar-sunrise-352209"

    obj = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    # obj.create_dataset()

    dataset = obj.dataset(dataset_id="bwt_session_dataset",project="stellar-sunrise-352209")
    print(dataset)
    dataset.location = "US"

    final_output = obj.create_dataset(dataset=dataset)
    print("successfully created : project_id:{}, dataset:{}".format(obj.project, dataset.dataset_id))
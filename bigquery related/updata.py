from google.cloud import bigquery
from google.oauth2 import service_account
if __name__ == '__main__':
    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-2c9b6b3e6b42.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    project_id = "stellar-sunrise-352209"
    dataset_id = "bwt_aks"

    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    dataset = client.get_dataset(dataset_id)
    dataset.description = "This is bwt DCE class1"
    dataset = client.update_dataset(dataset, ["description"])
    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    print("updated dataset '{}' with description '{}'.".format(full_dataset_id, dataset.description))

    dataset.default_table_expiration_ms = 240*60*60*1000
    dataset = client.update_dataset(dataset, ["default_table_expiration_ms"])
    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    print("updated dataset '{}' with new expiration '{}'.".format(full_dataset_id, dataset.default_table_expiration_ms))

    dataset.labels = {"state": "active","name": "akash"}
    dataset = client.update_dataset(dataset, ["labels"])
    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    print("updated dataset '{}' with new labels '{}'.".format(full_dataset_id, dataset.labels))

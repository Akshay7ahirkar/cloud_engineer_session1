from google.cloud import storage
from google.oauth2 import service_account
if __name__ == '__main__':

    key_path = r"C:\Users\Q\Downloads\stellar-sunrise-352209-d75716d50949.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )



    storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
    bucket = storage_client.bucket("bwt-createbucket2-session1")
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="US")

    print("bucket name: " + str(new_bucket.name))
    print("bucket storage class: " + str(new_bucket.storage_class))
    print("bucket location: " + str(new_bucket.location))
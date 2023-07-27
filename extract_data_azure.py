from azure.storage.blob import BlobServiceClient
import boto3
import time

service = BlobServiceClient(
    account_url="")
container_client = service.get_container_client('source-files')

s3 = boto3.client('s3', aws_access_key_id='',
                  aws_secret_access_key='')


def list_files():
    blobs = container_client.list_blob_names(**{"results_per_page": 100})
    for blob in blobs:
        if blob[len(blob) - 3:] == '.gz':
            start_time = time.time()
            upload_blob(download_blob(blob), blob)
            print(f"--- Blob {blob} uploaded after {(time.time() - start_time)} seconds ---")
            break


def download_blob(filepath):
    blob = container_client.get_blob_client(filepath)
    download = blob.download_blob()
    return download.readall()


def upload_blob(file_bytes, file_path):
    s3.put_object(
        Bucket='bucket-teste-byfrost',
        Key=file_path,
        Body=file_bytes
    )


if _name_ == '_main_':
    list_files()
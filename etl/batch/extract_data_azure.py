from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()
ACCOUNT_URL = os.getenv("ACCOUNT_URL")


class DataDownloader:
    def __init__(self):
        self.service = BlobServiceClient(account_url=ACCOUNT_URL) 
        self.container_client = self.service.get_container_client('source-files')

    def get_file_names(self):
        blob_names = self.container_client.list_blob_names(**{"results_per_page": 100})
        return blob_names
    
    def download_blob(self, filepath):           
        blob = self.container_client.get_blob_client(filepath)
        download = blob.download_blob()
        return download.readall()
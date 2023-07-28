from azure.storage.blob import ContainerClient
from dotenv import load_dotenv



load_dotenv()
ACCOUNT_URL = os.getenv("ACCOUNT_URL")


class DataDownloader:
    def __init__(self):
        self.container_client = ContainerClient.from_container_url(ACCOUNT_URL)

    def get_file_names(self):
        blob_names = self.container_client.list_blobs( include=['metadata'], name_starts_with='amazon_reviews')
        return blob_names
    
    def download_blob(self, filepath):           
        blob = self.container_client.get_blob_client(filepath)
        return blob.download_blob().readall()

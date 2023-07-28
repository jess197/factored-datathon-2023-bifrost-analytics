# from extract_data_azure import DataDownloader
from send_data_s3 import DataUploader
from azure_extraction import DataDownloader


uploader = DataUploader()
downloader = DataDownloader()

for file_name in downloader.get_file_names():
    if file_name[len(file_name) - 3:] == '.gz':
        print(file_name)
        uploader.upload_file(downloader.download_blob(file_name), file_name)
        break


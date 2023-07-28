# from extract_data_azure import DataDownloader
from .send_data_s3 import DataUploader
from .extract_data_azure import DataDownloader
import datetime
import logging

# Configuração do logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

uploader = DataUploader()
downloader = DataDownloader()

def upload_data_s3():
    today = datetime.date.today().strftime('%Y-%m-%d')
    for file_name in downloader.get_file_names():
        if file_name[len(file_name) - 3:] == '.gz':
            file_name_split = file_name.split('/')
            s3_file_name = f'source-files/{file_name_split[0]}/ingestion_date={today}/{file_name_split[2]}'
            logging.info(file_name)
            logging.info(s3_file_name)
            uploader.upload_file(downloader.download_blob(file_name), s3_file_name)

# source-files/amazon-reviews/ingestion_date="date"/file
# source-files/amazon-metadata/ingestion_date="date"/file



from .send_data_s3 import DataUploader
from .extract_data_azure import DataDownloader
import datetime
import logging
from threading import Thread

# Configuração do logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

uploader = DataUploader()
downloader = DataDownloader()

today = datetime.date.today().strftime('%Y-%m-%d')

MAX_THREADS = 60

def upload_data_s3():
    threads_list = list()
    for file_name in downloader.get_file_names():
        if file_name[len(file_name) - 3:] == '.gz' and 'amazon_reviews' in file_name:
            thread = Thread(target=lambda: download_and_upload_file(file_name=file_name))
            threads_list.append(thread)
            thread.start()
            logging.info(f'Threads count:{len(threads_list)}')
            if(len(threads_list) == MAX_THREADS):
                for thread in threads_list:
                    thread.join()
                threads_list = list()
    if(len(threads_list) > 0):
        for thread in threads_list:
            thread.join()

def download_and_upload_file(file_name):
    if uploader.blob_exists(get_s3_file_name(file_name)):
        logging.info(f'Skipping download. file: {file_name}')
        return
    logging.info(f'Starting new thread for download file: {file_name}')
    socoped_downloader = DataDownloader()
    uploader.upload_file(socoped_downloader.download_blob(file_name), get_s3_file_name(file_name))

def get_s3_file_name(file_name):
    file_name_split = file_name.split('/')
    return f'source-files/{file_name_split[0]}/ingestion_date={today}/{file_name_split[2]}'
    




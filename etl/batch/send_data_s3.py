import boto3
import os
from dotenv import load_dotenv

load_dotenv()

class DataUploader:

    def __init__(self):
        self.bucket_name = 'amazon-data-bucket'
        self.aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID') 
        self.aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)

    

    def upload_file(self,file_bytes, file_path):
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=file_path,
                Body=file_bytes
            )
            print(f"Uploaded {file_path} to Amazon S3 with success")
        except Exception as e:
            print("An error occurred during the upload of the file to Amazon S3:")
            print(str(e))

    def blob_exists(self, file_name):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=file_name)
        return 'Contents' in response
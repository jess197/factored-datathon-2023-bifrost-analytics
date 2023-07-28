import boto3
import os
from dotenv import load_dotenv
import logging
load_dotenv()

AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')



class MessagerSender:
    def __init__ (self):
        self.sqs = boto3.client(
                service_name = 'sqs',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name='us-east-1'
            )
        self.queue_url = 'https://sqs.us-east-1.amazonaws.com/158061202810/amazon-reviews'


    def send_message(self, message_body):
        # Send message to SQS queue
        response = self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=(
                message_body
            )
        )

        logging.info(response['MessageId'])


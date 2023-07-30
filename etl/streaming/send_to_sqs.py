import boto3
import os
from dotenv import load_dotenv
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import uuid

today = datetime.date.today().strftime('%Y-%m-%d')

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
        self.message_list = []


    def send_message(self, message_body):
        # Send message to SQS queue
        response = self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=(
                message_body
            )
        )
        logging.info(response['MessageId'])


    def read_messages(self):
        while True:
            # Retrieving a stack of messages from the queue
            messages_stack = self.sqs.receive_message(
                QueueUrl= self.queue_url,
                MaxNumberOfMessages= 10
            )
            
            # Storing the messages in the message_list and deleting them from the queue
            if len(messages_stack) > 0:
                for message in messages_stack['Messages']:
                    self.message_list.append(message['Body'])

                    receipt_handle = message['ReceiptHandle']
                    
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt_handle
                    )
            
            # Breaking out of the loop if there are no more messages in the queue
            else:
                logging.info("No messages readed")
                break
            

    def get_messages(self):
        
        logging.info("Starting Thread")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            #print("teste")
            for x in range(40):
                futures.append(executor.submit(self.read_messages))
            for future in as_completed(futures):
                pass

        

    def send_messages_to_s3(self):
        bucket = 'amazon-data-bucket'
        object_name = f'factored-datathon/amazon_review/ingestion_date={today}/{str(uuid.uuid1())}.json'

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1'
        )

        s3_client = session.resource('s3')
    
        messages = s3_client.Object(
            bucket_name=bucket, 
            key=object_name
        )
        messages.put(Body= json.dumps(self.message_list))


if __name__ == "__main__":
    msg = MessagerSender()
    msg.get_messages()
    msg.send_messages_to_s3()
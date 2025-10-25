import boto3
import json
import logging
from collections import deque

class S3Retriever:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        logging.info(f"S3 Retriever initialized for bucket: {self.bucket}")

    def get_request(self):
        # try to get one object from the bucket
        try:
            # list one object
            response = self.s3.list_objects_v2(Bucket=self.bucket, MaxKeys=1)

            # if there are no objects, return None
            if 'Contents' not in response or len(response['Contents']) == 0:
                return None

            # if there is an object, get its key
            key = response['Contents'][0]['Key']
            logging.info("Request {} found in bucket.".format(response['Contents'][0]['Key']))

            # log the key being processed
            logging.info(f"Retrieving request: {key}")

            # get the object data
            get_response = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = get_response['Body'].read().decode('utf-8')
            request_data = json.loads(body)

            # delete the object after processing to avoid re-processing
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            logging.info(f"Processed request: {key}")

            return request_data

        except Exception as e:
            logging.error(f"Error in the retriever's get_request method: {e}")
            return None
        
class SQSRetriever:
    def __init__(self, queue_name):
        self.sqs = boto3.client('sqs')
        # get the full url for the queue name
        response = self.sqs.get_queue_url(QueueName=queue_name)
        self.queue_url = response['QueueUrl']
        logging.info(f"SQS Retriever initialized for queue: {self.queue_url}")

        # cache for messages fetched from SQS
        self.message_cache = deque()

    def get_request(self):
        # if cache is empty, poll SQS (up to 10 messages per assignment requirement)
        if not self.message_cache:
            logging.info("Polling SQS for messages...")
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=10,    
                    WaitTimeSeconds=20,      
                    MessageAttributeNames=['All']
                )
                
                # if messages were received, add them to the cache
                if 'Messages' in response:
                    # Extend the cache with the new messages
                    self.message_cache.extend(response['Messages'])
                    logging.info(f"Received {len(response['Messages'])} messages.")
                else:
                    return None
                    
            except Exception as e:
                logging.error(f"Error receiving messages from SQS: {e}")
                return None

        # Process the next message in the cache
        if self.message_cache:
            message = self.message_cache.popleft()
            
            try:
                # Parse the body
                request_data = json.loads(message['Body'])
                # Get the handle needed for deletion
                receipt_handle = message['ReceiptHandle']
                
                # Return both the data and the handle
                return (request_data, receipt_handle)
                
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode message body: {e}. Message: {message['Body']}")
                # delete messed up message to avoid re-processing
                self.delete_message(message['ReceiptHandle'])
                logging.warning("Deleted malformed JSON message.")
                return None
        
        # Do nothing if no messages are available
        return None

    def delete_message(self, receipt_handle):
        try:
            # delete the message from the queue using its receipt handle
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logging.info(f"Successfully deleted message.")
        except Exception as e:
            logging.error(f"Failed to delete message with handle {receipt_handle}: {e}")
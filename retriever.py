import boto3
import json
import logging

class S3Retriever:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        logging.info(f"Retriever initialized for bucket: {self.bucket}")

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
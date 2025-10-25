import boto3
import json
import logging

# Stprage handler for S3
class S3Storage:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        logging.info(f"S3Storage initialized for bucket: {self.bucket}")

    def _format_key(self, owner, widget_id):
        # format the key as specified in the assignment: 'widgets/{owner}/{widget id}'
        formatted_owner = owner.lower().replace(' ', '-')
        return f"widgets/{formatted_owner}/{widget_id}"

    def create_widget(self, request):
        # Extract necessary fields from the request
        widget_id = request['widgetId']
        owner = request['owner']
        key = self._format_key(owner, widget_id)
        
        # The body is the JSON string of the entire request
        body = json.dumps(request)
        
        # Store the object in S3
        logging.info(f"Storing widget in S3 at key: {key}")
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body
        )

# Storage handler for DynamoDB
class DynamoDBStorage:
    def __init__(self, table_name):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
        logging.info(f"DynamoDBStorage initialized for table: {table_name}")

    def create_widget(self, request):
        # Prepare the item to be stored in DynamoDB
        item = {
            'id': request['widgetId'],
            'owner': request['owner']
        }
        
        # Include optional fields if they exist
        if 'label' in request:
            item['label'] = request['label']
        if 'description' in request:
            item['description'] = request['description']

        # Include any other attributes dynamically
        if 'otherAttributes' in request:
            for attr in request['otherAttributes']:
                item[attr['name']] = attr['value']

        logging.info(f"Storing widget in DynamoDB: {item['id']}")

        # Store the item in DynamoDB
        self.table.put_item(Item=item)
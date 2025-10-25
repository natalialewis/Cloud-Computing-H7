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

    def update_widget(self, request):
        # update the widget by overwriting it with the new information
        logging.info(f"Updating widget in S3: {request['widgetId']}")
        self.create_widget(request)

    def delete_widget(self, request):
        widget_id = request['widgetId']
        # I need the owner to delete the object from S3 so this checks if it's present
        if 'owner' not in request:
            logging.error(f"Cannot delete widget {widget_id} from S3: 'owner' is missing from request.")
            raise ValueError("Delete request for S3 must include 'owner'")
            
        owner = request['owner']
        key = self._format_key(owner, widget_id)
        
        logging.info(f"Deleting widget from S3: {key}")
        self.s3.delete_object(Bucket=self.bucket, Key=key)

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

    def update_widget(self, request):
        widget_id = request['widgetId']
        logging.info(f"Updating widget in DynamoDB: {widget_id}")

        # Build the update expression dynamically
        update_expression = "SET "
        expression_attribute_values = {}
        expression_attribute_names = {}
        
        # Iterate over fields that can be updated
        updatable_fields = ['owner', 'label', 'description']
        for field in updatable_fields:
            if field in request:

                # Use a safe placeholder
                name_placeholder = f"#{field}" 
                value_placeholder = f":{field}" 

                update_expression += f"{name_placeholder} = {value_placeholder}, "
                
                # Map the safe attribute names and values to their real counterparts
                expression_attribute_names[name_placeholder] = field
                expression_attribute_values[value_placeholder] = request[field]
        
        # Handle otherAttributes
        if 'otherAttributes' in request:
            for i, attr in enumerate(request['otherAttributes']):
                attr_name = attr['name']
                attr_val = attr['value']
                
                # Create a safe placeholder
                name_placeholder = f"#attr{i}"
                value_placeholder = f":val{i}"

                update_expression += f"{name_placeholder} = {value_placeholder}, "

                # Map the safe attribute names and values to their real counterparts
                expression_attribute_names[name_placeholder] = attr_name
                expression_attribute_values[value_placeholder] = attr_val

        # if there are no fields to update, skip the operation
        if not expression_attribute_values:
            logging.warning(f"Update request for {widget_id} had no fields to update. Skipping.")
            return

        # Remove the trailing comma and space
        update_expression = update_expression.rstrip(', ')
        
        # Perform the update
        if expression_attribute_names:
            self.table.update_item(
                Key={'id': widget_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
        else:
             # If we didn't have any 'otherAttributes', run the old command
             self.table.update_item(
                Key={'id': widget_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )

    def delete_widget(self, request):
        widget_id = request['widgetId']
        logging.info(f"Deleting widget from DynamoDB: {widget_id}")
        self.table.delete_item(
            Key={'id': widget_id}
        )
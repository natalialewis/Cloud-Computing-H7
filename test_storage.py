# Used resources such as the Python documentation and ChatGPT to help understand tests and debug this file.

import unittest
import json
from unittest.mock import patch, MagicMock
from storage import S3Storage, DynamoDBStorage  # Assumes classes are in storage.py

class TestS3Storage(unittest.TestCase):

    def setUp(self):
        """Set up a sample request data for all S3 tests."""
        self.sample_request = {
            "type": "create",
            "requestId": "req-s3-test",
            "widgetId": "w-s3-001",  ### <-- MUST be widgetId (INPUT)
            "owner": "Test User S3",
            "label": "S3 Widget",
            "description": "A widget for S3."
        }
        self.bucket_name = "test-widget-bucket"

    @patch('boto3.client')
    def test_create_widget(self, mock_boto_client):
        """
        Tests that create_widget formats the key and body correctly
        and calls put_object.
        """
        # 1. Set up
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.put_object.return_value = {} # Mock the call

        # 2. Stimulate
        storage = S3Storage(self.bucket_name)
        storage.create_widget(self.sample_request)

        # 3. Compare
        
        # Calculate the expected key based on assignment rules:
        expected_key = "widgets/test-user-s3/w-s3-001"
        
        # Calculate the expected body (JSON string of the whole request)
        expected_body = json.dumps(self.sample_request)
        
        # Check that put_object was called once with the correct parameters
        mock_s3.put_object.assert_called_once_with(
            Bucket=self.bucket_name,
            Key=expected_key,
            Body=expected_body
        )

    @patch('boto3.client')
    def test_update_widget(self, mock_boto_client):
        storage = S3Storage(self.bucket_name)

        # mock create the boto3 s3 client and its delete_object method
        with patch.object(storage, 'create_widget') as mock_create:
            storage.update_widget(self.sample_request)
            # make sure update calls create to overwrite
            mock_create.assert_called_once_with(self.sample_request)

    @patch('boto3.client')
    def test_delete_widget(self, mock_boto_client):
        # Set up the mock S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        storage = S3Storage(self.bucket_name)

        # A sample request to delete (must include owner and widgetId)
        delete_request = {
            "widgetId": "w-s3-001",  ### <-- MUST be widgetId (INPUT)
            "owner": "Test User S3"
        }

        # Simulate the delete
        storage.delete_widget(delete_request)

        # Compare that delete_object was called with correct parameters
        expected_key = "widgets/test-user-s3/w-s3-001"
        mock_s3.delete_object.assert_called_once_with(
            Bucket=self.bucket_name,
            Key=expected_key
        )


class TestDynamoDBStorage(unittest.TestCase):

    def setUp(self):
        """Set up sample requests for all DynamoDB tests."""
        self.table_name = "test-widget-table"
        
        # Sample request *with* otherAttributes
        self.request_with_attribs = {
            "type": "create",
            "requestId": "req-ddb-test-1",
            "widgetId": "w-ddb-001",  ### <-- MUST be widgetId (INPUT)
            "owner": "Test User DDB",
            "label": "DDB Widget",
            "description": "A widget for DDB.",
            "otherAttributes": [
                {"name": "color", "value": "blue"},
                {"name": "size", "value": "medium"}
            ]
        }
        
        # Sample request *without* otherAttributes
        self.request_no_attribs = {
            "type": "create",
            "requestId": "req-ddb-test-2",
            "widgetId": "w-ddb-002",  ### <-- MUST be widgetId (INPUT)
            "owner": "Another User DDB",
            "label": "Simple DDB Widget"
            # No description or otherAttributes
        }

    @patch('boto3.resource')
    def test_create_widget_with_attributes(self, mock_boto_resource):
        """
        Tests that create_widget correctly flattens 'otherAttributes'
        into the DynamoDB item.
        """
        # 1. Set up
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_boto_resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table
        
        mock_table.put_item.return_value = {} # Mock the call

        # 2. Stimulate
        storage = DynamoDBStorage(self.table_name)
        storage.create_widget(self.request_with_attribs)

        # 3. Compare
        
        # This is the expected item that should be sent to put_item.
        expected_item = {
            "id": "w-ddb-001",           ### <-- MUST be id (OUTPUT)
            "owner": "Test User DDB",
            "label": "DDB Widget",
            "description": "A widget for DDB.",
            "color": "blue",
            "size": "medium"
        }
        
        mock_boto_resource.assert_called_with('dynamodb')
        mock_dynamodb.Table.assert_called_with(self.table_name)
        mock_table.put_item.assert_called_once_with(Item=expected_item)

    @patch('boto3.resource')
    def test_create_widget_no_attributes(self, mock_boto_resource):
        """
        Tests that create_widget works correctly for a request
        that has no 'otherAttributes' or 'description'.
        """
        # 1. Set up
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_boto_resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table
        mock_table.put_item.return_value = {}

        # 2. Stimulate
        storage = DynamoDBStorage(self.table_name)
        storage.create_widget(self.request_no_attribs)

        # 3. Compare
        
        expected_item = {
            "id": "w-ddb-002",           ### <-- MUST be id (OUTPUT)
            "owner": "Another User DDB",
            "label": "Simple DDB Widget"
        }
        
        mock_dynamodb.Table.assert_called_with(self.table_name)
        mock_table.put_item.assert_called_once_with(Item=expected_item)

    @patch('boto3.resource')
    def test_update_widget(self, mock_boto_resource):
        # Set up the mock DynamoDB resource and table
        mock_dynamodb = MagicMock()
        mock_table = MagicMock()
        mock_boto_resource.return_value = mock_dynamodb
        mock_dynamodb.Table.return_value = mock_table
        storage = DynamoDBStorage(self.table_name)

        # Create a sample update request
        update_request = {
            "widgetId": "w-ddb-001",
            "label": "New Label",
            "otherAttributes": [
                {"name": "color", "value": "red"}
            ]
        }

        # Stimulate the update
        storage.update_widget(update_request)

        # Compare the expected key
        expected_key = {'id': 'w-ddb-001'} ### <-- MUST be id (OUTPUT)

        # Compare the expected update expression and values
        expected_update_expression = "SET #label = :label, #attr0 = :val0"
        expected_names = {
            '#label': 'label',
            '#attr0': 'color'
        }
        expected_values = {
            ":label": "New Label",
            ":val0": "red"
        }

        # Get the actual arguments passed to update_item
        call_args = mock_table.update_item.call_args

        # Check the key
        self.assertEqual(call_args.kwargs['Key'], expected_key)

        # Check the values
        self.assertEqual(call_args.kwargs['ExpressionAttributeValues'], expected_values)

        # check the names
        self.assertEqual(call_args.kwargs['ExpressionAttributeNames'], expected_names)

        # Check the expression
        actual_expr = call_args.kwargs['UpdateExpression']
        actual_parts = sorted(actual_expr.replace("SET ", "").split(', '))
        expected_parts = sorted(expected_update_expression.replace("SET ", "").split(', '))
        self.assertEqual(actual_parts, expected_parts)
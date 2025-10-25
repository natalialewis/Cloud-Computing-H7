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
            "widgetId": "w-s3-001",
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
        # "widgets/{owner}/{widget id}"
        # "replacing spaces with dashes and converting the whole string to lower case"
        expected_key = "widgets/test-user-s3/w-s3-001"
        
        # Calculate the expected body (JSON string of the whole request)
        expected_body = json.dumps(self.sample_request)
        
        # Check that put_object was called once with the correct parameters
        mock_s3.put_object.assert_called_once_with(
            Bucket=self.bucket_name,
            Key=expected_key,
            Body=expected_body
        )


class TestDynamoDBStorage(unittest.TestCase):

    def setUp(self):
        """Set up sample requests for all DynamoDB tests."""
        self.table_name = "test-widget-table"
        
        # Sample request *with* otherAttributes
        self.request_with_attribs = {
            "type": "create",
            "requestId": "req-ddb-test-1",
            "widgetId": "w-ddb-001",
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
            "widgetId": "w-ddb-002",
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
        # Notice 'color' and 'size' are top-level keys.
        expected_item = {
            "widgetId": "w-ddb-001",
            "owner": "Test User DDB",
            "label": "DDB Widget",
            "description": "A widget for DDB.",
            "color": "blue",
            "size": "medium"
        }
        
        # Check that boto3.resource('dynamodb') was called
        mock_boto_resource.assert_called_with('dynamodb')
        
        # Check that the Table object was retrieved with the correct name
        mock_dynamodb.Table.assert_called_with(self.table_name)
        
        # Check that put_item was called once with the correctly structured item
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
        
        # The expected item should only contain the properties that exist
        # in the request.
        expected_item = {
            "widgetId": "w-ddb-002",
            "owner": "Another User DDB",
            "label": "Simple DDB Widget"
            # It should gracefully handle the missing 'description' and 'otherAttributes'
        }
        
        mock_dynamodb.Table.assert_called_with(self.table_name)
        mock_table.put_item.assert_called_once_with(Item=expected_item)

if __name__ == '__main__':
    unittest.main()
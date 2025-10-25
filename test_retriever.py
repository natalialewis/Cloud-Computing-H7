# Used resources such as the Python documentation and ChatGPT to help understand tests and debug this file.

import unittest
import json
import io
from unittest.mock import patch, MagicMock
from retriever import S3Retriever  # Assumes your class is in retriever.py

class TestS3Retriever(unittest.TestCase):

    # This is a sample widget request, like one we'd find in Bucket 2
    SAMPLE_REQUEST_JSON = {
        "type": "create",
        "requestId": "req-abc-123",
        "widgetId": "w-001",
        "owner": "Test User"
    }
    
    # We patch 'boto3.client' because we assume S3Retriever creates its own client
    # The patch replaces boto3.client with a MagicMock for the duration of the test
    @patch('boto3.client')
    def test_get_request_success(self, mock_boto_client):
        """
        Tests the successful retrieval and deletion of a single request.
        """
        # 1. Set up the initial state (Mocking)
        
        # Configure the mock S3 client that boto3.client() will return
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock the response for list_objects_v2
        # We simulate one object being in the bucket
        list_response = {
            'Contents': [
                {'Key': 'request-001.json'}
            ]
        }
        mock_s3.list_objects_v2.return_value = list_response

        # Mock the response for get_object
        # The body needs to be a stream, so we use io.BytesIO
        request_body_str = json.dumps(self.SAMPLE_REQUEST_JSON)
        request_body_bytes = io.BytesIO(request_body_str.encode('utf-8'))
        get_response = {
            'Body': request_body_bytes
        }
        mock_s3.get_object.return_value = get_response
        
        # Mock the delete_object response (it doesn't need to return anything complex)
        mock_s3.delete_object.return_value = {}

        # 2. Stimulate the thing to be tested
        bucket_name = "test-consume-bucket"
        retriever = S3Retriever(bucket_name)
        result = retriever.get_request()

        # 3. Compare expected results to predicted results
        
        # Check that the correct request was returned
        self.assertEqual(result, self.SAMPLE_REQUEST_JSON)

        # Check that our mock AWS functions were called correctly
        # Check if we listed objects from the correct bucket
        mock_s3.list_objects_v2.assert_called_with(Bucket=bucket_name, MaxKeys=1)

        # Check if we retrieved the correct object
        mock_s3.get_object.assert_called_with(Bucket=bucket_name, Key='request-001.json')

        # Check if we deleted the correct object
        mock_s3.delete_object.assert_called_with(Bucket=bucket_name, Key='request-001.json')

    @patch('boto3.client')
    def test_get_request_empty_bucket(self, mock_boto_client):
        """
        Tests the case where no requests are available in the bucket.
        """
        # 1. Set up
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock an empty response for list_objects_v2
        empty_list_response = {
            # No 'Contents' key means the bucket is empty
        }
        mock_s3.list_objects_v2.return_value = empty_list_response

        # 2. Stimulate
        bucket_name = "test-consume-bucket"
        retriever = S3Retriever(bucket_name)
        result = retriever.get_request()

        # 3. Compare
        # Result should be None as per the polling loop logic
        self.assertIsNone(result)

        # Check that we *only* called list_objects_v2
        mock_s3.list_objects_v2.assert_called_with(Bucket=bucket_name, MaxKeys=1)
        
        # get_object and delete_object should NOT have been called
        mock_s3.get_object.assert_not_called()
        mock_s3.delete_object.assert_not_called()

if __name__ == '__main__':
    unittest.main()
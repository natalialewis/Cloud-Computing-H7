# Used resources such as the Python documentation and ChatGPT to help understand tests and debug this file.

import unittest
import json
import io
from unittest.mock import patch, MagicMock
from retriever import S3Retriever, SQSRetriever

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

class TestSQSRetriever(unittest.TestCase):

    # A sample message from SQS
    SAMPLE_MESSAGE = {
        'MessageId': 'msg-123',
        'ReceiptHandle': 'handle-123',
        'Body': json.dumps({
            "type": "create",
            "requestId": "req-abc-123",
            "widgetId": "w-001",
            "owner": "Test User"
        })
    }
    
    SAMPLE_REQUEST_DATA = json.loads(SAMPLE_MESSAGE['Body'])

    @patch('boto3.client')
    def setUp(self, mock_boto_client):
        # This setup runs before each test
        self.mock_sqs = MagicMock()
        mock_boto_client.return_value = self.mock_sqs
        
        # Mock the get_queue_url call
        self.mock_sqs.get_queue_url.return_value = {
            'QueueUrl': 'http://fake-queue.url'
        }
        
        self.retriever = SQSRetriever(queue_name='test-queue')

    def test_get_request_success(self):
        """Tests getting a single message from SQS."""
        # 1. Set up
        self.mock_sqs.receive_message.return_value = {
            'Messages': [self.SAMPLE_MESSAGE]
        }
        
        # 2. Stimulate
        result = self.retriever.get_request()
        
        # 3. Compare
        self.assertIsNotNone(result)
        request_data, receipt_handle = result
        
        self.assertEqual(request_data, self.SAMPLE_REQUEST_DATA)
        self.assertEqual(receipt_handle, 'handle-123')
        self.mock_sqs.receive_message.assert_called_once()
        self.assertEqual(len(self.retriever.message_cache), 0)

    def test_get_request_empty_queue(self):
        """Tests polling an empty queue."""
        # 1. Set up
        self.mock_sqs.receive_message.return_value = {} # No 'Messages' key
        
        # 2. Stimulate
        result = self.retriever.get_request()
        
        # 3. Compare
        self.assertIsNone(result)
        self.mock_sqs.receive_message.assert_called_once()
        self.assertEqual(len(self.retriever.message_cache), 0)

    def test_get_request_caching(self):
        """Tests that messages are cached locally."""
        # 1. Set up
        message2 = {
            'MessageId': 'msg-456',
            'ReceiptHandle': 'handle-456',
            'Body': json.dumps({"type": "delete"})
        }
        self.mock_sqs.receive_message.return_value = {
            'Messages': [self.SAMPLE_MESSAGE, message2]
        }
        
        # 2. Stimulate
        # First call: polls SQS, gets 2 messages, returns 1
        result1 = self.retriever.get_request()
        self.assertEqual(len(self.retriever.message_cache), 1) # One left in cache
        
        # Second call: should get from cache, NOT poll SQS
        result2 = self.retriever.get_request()
        self.assertEqual(len(self.retriever.message_cache), 0) # Cache now empty
        
        # 3. Compare
        # Check that receive_message was ONLY called once
        self.mock_sqs.receive_message.assert_called_once()
        
        # Check first message
        self.assertEqual(result1[0], self.SAMPLE_REQUEST_DATA)
        self.assertEqual(result1[1], 'handle-123')
        
        # Check second message
        self.assertEqual(result2[0], {"type": "delete"})
        self.assertEqual(result2[1], 'handle-456')

    def test_delete_message(self):
        """Tests the delete_message method."""
        # 1. Set up (done in setUp)
        
        # 2. Stimulate
        self.retriever.delete_message('test-handle')
        
        # 3. Compare
        self.mock_sqs.delete_message.assert_called_once_with(
            QueueUrl='http://fake-queue.url',
            ReceiptHandle='test-handle'
        )

if __name__ == '__main__':
    unittest.main()
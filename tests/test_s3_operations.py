import unittest
from unittest.mock import patch, MagicMock, mock_open
import io
import json
import sys
import os

# Add parent directory to path so we can import the lambda_function
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import lambda_function

class TestS3Operations(unittest.TestCase):
    
    @patch('boto3.client')
    def test_get_pdf_s3_keys_success(self, mock_boto3_client):
        # Setup
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        
        # Mock the S3 response
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({
            "pdfs": ["file1.pdf", "file2.pdf"]
        }).encode('utf-8')
        
        mock_s3.get_object.return_value = {
            'Body': mock_body
        }
        
        # Execute
        result = lambda_function.get_pdf_s3_keys("test-bucket", "test-key.json")
        
        # Assert
        mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="test-key.json")
        self.assertEqual(result, ["file1.pdf", "file2.pdf"])
    
    @patch('boto3.client')
    def test_get_pdf_s3_keys_empty(self, mock_boto3_client):
        # Setup
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        
        # Mock the S3 response with empty PDF list
        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps({
            "pdfs": []
        }).encode('utf-8')
        
        mock_s3.get_object.return_value = {
            'Body': mock_body
        }
        
        # Execute
        result = lambda_function.get_pdf_s3_keys("test-bucket", "test-key.json")
        
        # Assert
        self.assertEqual(result, [])
    
    @patch('boto3.client')
    def test_get_pdf_s3_keys_invalid_json(self, mock_boto3_client):
        # Setup
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        
        # Mock the S3 response with invalid JSON
        mock_body = MagicMock()
        mock_body.read.return_value = "invalid-json".encode('utf-8')
        
        mock_s3.get_object.return_value = {
            'Body': mock_body
        }
        
        # Execute and Assert
        with self.assertRaises(Exception) as context:
            lambda_function.get_pdf_s3_keys("test-bucket", "test-key.json")
        
        self.assertIn("Error parsing JSON", str(context.exception))
    
    @patch('boto3.client')
    def test_download_pdf_from_s3(self, mock_boto3_client):
        # Setup
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        
        # Mock the S3 response
        mock_body = MagicMock()
        mock_body.read.return_value = b"PDF content"
        
        mock_s3.get_object.return_value = {
            'Body': mock_body
        }
        
        # Execute
        result = lambda_function.download_pdf_from_s3("test-bucket", "test.pdf")
        
        # Assert
        mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="test.pdf")
        self.assertEqual(result, b"PDF content")
    
    @patch('boto3.resource')
    def test_upload_file_to_s3(self, mock_boto3_resource):
        # Setup
        mock_s3_resource = MagicMock()
        mock_boto3_resource.return_value = mock_s3_resource
        
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket
        
        # Mock file opening
        m = mock_open(read_data=b"test file content")
        
        # Execute
        with patch('builtins.open', m):
            result = lambda_function.upload_file_to_s3("test-bucket", "test-key.pdf", "/tmp/test.pdf")
        
        # Assert
        mock_s3_resource.Bucket.assert_called_once_with("test-bucket")
        mock_bucket.put_object.assert_called_once()
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
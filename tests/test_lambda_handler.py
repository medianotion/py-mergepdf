import unittest
import json
from unittest.mock import patch, MagicMock
import sys
import os

# Add parent directory to path so we can import the lambda_function
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import lambda_function

class TestLambdaHandler(unittest.TestCase):
    
    @patch('lambda_function.handle')
    def test_lambda_handler_success(self, mock_handle):
        # Setup
        event = {"input_bucket": "test-bucket"}
        context = {}
        
        # Execute
        result = lambda_function.lambda_handler(event, context)
        
        # Assert
        mock_handle.assert_called_once_with(event)
        self.assertEqual(result['statusCode'], 200)
        self.assertIn('PDF merge completed successfully', result['body'])
    
    @patch('lambda_function.handle')
    def test_lambda_handler_error(self, mock_handle):
        # Setup
        event = {"input_bucket": "test-bucket"}
        context = {}
        mock_handle.side_effect = Exception("Test error")
        
        # Execute
        result = lambda_function.lambda_handler(event, context)
        
        # Assert
        self.assertEqual(result['statusCode'], 500)
        self.assertIn('Test error', result['body'])
    
    def test_handle_direct_invoke_with_optimize_true(self):
        # Setup
        event = {
            "input_bucket": "test-bucket",
            "input_file_key": "test.json",
            "output_bucket": "output-bucket",
            "output_file_key": "output.pdf",
            "optimize_pdf": True
        }
        
        with patch('lambda_function.process_merge') as mock_process:
            # Execute
            lambda_function.handle(event)
            
            # Assert
            mock_process.assert_called_once_with(
                "test-bucket", "test.json", "output-bucket", "output.pdf", True
            )
    
    def test_handle_direct_invoke_with_optimize_false(self):
        # Setup
        event = {
            "input_bucket": "test-bucket",
            "input_file_key": "test.json",
            "output_bucket": "output-bucket",
            "output_file_key": "output.pdf",
            "optimize_pdf": False
        }
        
        with patch('lambda_function.process_merge') as mock_process:
            # Execute
            lambda_function.handle(event)
            
            # Assert
            mock_process.assert_called_once_with(
                "test-bucket", "test.json", "output-bucket", "output.pdf", False
            )
    
    def test_handle_direct_invoke_without_optimize(self):
        # Setup - no optimize_pdf parameter specified
        event = {
            "input_bucket": "test-bucket",
            "input_file_key": "test.json",
            "output_bucket": "output-bucket",
            "output_file_key": "output.pdf"
        }
        
        with patch('lambda_function.process_merge') as mock_process:
            # Execute
            lambda_function.handle(event)
            
            # Assert - should default to False
            mock_process.assert_called_once_with(
                "test-bucket", "test.json", "output-bucket", "output.pdf", False
            )
    
    def test_handle_sqs_event_with_optimize(self):
        # Setup
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "input_bucket": "test-bucket",
                        "input_file_key": "test.json",
                        "output_bucket": "output-bucket",
                        "output_file_key": "output.pdf",
                        "optimize_pdf": True
                    })
                }
            ]
        }
        
        with patch('lambda_function.process_merge') as mock_process:
            # Execute
            lambda_function.handle(event)
            
            # Assert
            mock_process.assert_called_once_with(
                "test-bucket", "test.json", "output-bucket", "output.pdf", True
            )
    
    def test_handle_sqs_event_without_optimize(self):
        # Setup
        event = {
            "Records": [
                {
                    "messageId": "test-message-id",
                    "body": json.dumps({
                        "input_bucket": "test-bucket",
                        "input_file_key": "test.json",
                        "output_bucket": "output-bucket",
                        "output_file_key": "output.pdf"
                    })
                }
            ]
        }
        
        with patch('lambda_function.process_merge') as mock_process:
            # Execute
            lambda_function.handle(event)
            
            # Assert - should default to False
            mock_process.assert_called_once_with(
                "test-bucket", "test.json", "output-bucket", "output.pdf", False
            )
    
    def test_handle_invalid_format(self):
        # Setup
        event = {"invalid_key": "value"}
        
        # Execute and Assert
        with self.assertRaises(Exception) as context:
            lambda_function.handle(event)
        
        self.assertIn("unrecognized format", str(context.exception))

if __name__ == '__main__':
    unittest.main()
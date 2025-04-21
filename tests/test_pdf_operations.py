import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import sys
import os

# Add parent directory to path so we can import the lambda_function
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import lambda_function

class TestPdfOperations(unittest.TestCase):
    
    @patch('lambda_function.download_pdf_from_s3')
    @patch('fitz.open')
    def test_merge_pdfs_with_optimization(self, mock_fitz_open, mock_download):
        # Setup
        # Mock the PDF download
        mock_download.return_value = b"PDF content"
        
        # Mock fitz objects
        mock_pdf_doc = MagicMock()
        mock_merged_pdf = MagicMock()
        
        # Configure mock_fitz_open to return different objects based on arguments
        def mock_fitz_open_side_effect(*args, **kwargs):
            if 'stream' in kwargs:
                return mock_pdf_doc
            return mock_merged_pdf
        
        mock_fitz_open.side_effect = mock_fitz_open_side_effect
        
        # Execute with optimize_pdf=True
        lambda_function.merge_pdfs("test-bucket", ["file1.pdf", "file2.pdf"], "/tmp/output.pdf", True)
        
        # Assert
        self.assertEqual(mock_download.call_count, 2)
        mock_merged_pdf.insert_pdf.assert_called()
        # Verify optimization was applied
        mock_merged_pdf.save.assert_any_call('/tmp/output.pdf', deflate=True, garbage=4, clean=True)
        mock_merged_pdf.close.assert_called_once()
    
    @patch('lambda_function.download_pdf_from_s3')
    @patch('fitz.open')
    def test_merge_pdfs_without_optimization(self, mock_fitz_open, mock_download):
        # Setup
        # Mock the PDF download
        mock_download.return_value = b"PDF content"
        
        # Mock fitz objects
        mock_pdf_doc = MagicMock()
        mock_merged_pdf = MagicMock()
        
        # Configure mock_fitz_open to return different objects based on arguments
        def mock_fitz_open_side_effect(*args, **kwargs):
            if 'stream' in kwargs:
                return mock_pdf_doc
            return mock_merged_pdf
        
        mock_fitz_open.side_effect = mock_fitz_open_side_effect
        
        # Execute with optimize_pdf=False
        lambda_function.merge_pdfs("test-bucket", ["file1.pdf", "file2.pdf"], "/tmp/output.pdf", False)
        
        # Assert
        self.assertEqual(mock_download.call_count, 2)
        mock_merged_pdf.insert_pdf.assert_called()
        # Verify basic save was called without optimization parameters
        mock_merged_pdf.save.assert_called_once_with('/tmp/output.pdf')
        mock_merged_pdf.close.assert_called_once()
    
    @patch('lambda_function.get_pdf_s3_keys')
    @patch('lambda_function.merge_pdfs')
    @patch('lambda_function.upload_file_to_s3')
    @patch('os.path.isfile')
    @patch('os.remove')
    def test_process_merge_with_optimization(self, mock_remove, mock_isfile, mock_upload, mock_merge, mock_get_keys):
        # Setup
        mock_get_keys.return_value = ["file1.pdf", "file2.pdf"]
        mock_isfile.return_value = True
        
        # Execute with optimize_pdf=True
        lambda_function.process_merge("input-bucket", "input-key.json", "output-bucket", "output-key.pdf", True)
        
        # Assert
        mock_get_keys.assert_called_once_with("input-bucket", "input-key.json")
        # Check that merge_pdfs was called with optimize_pdf=True
        mock_merge.assert_called_once()
        self.assertEqual(mock_merge.call_args[0][3], True)  # Fourth parameter should be optimize_pdf=True
        mock_upload.assert_called_once()
        mock_isfile.assert_called_once()
        mock_remove.assert_called_once()
    
    @patch('lambda_function.get_pdf_s3_keys')
    @patch('lambda_function.merge_pdfs')
    @patch('lambda_function.upload_file_to_s3')
    @patch('os.path.isfile')
    @patch('os.remove')
    def test_process_merge_without_optimization(self, mock_remove, mock_isfile, mock_upload, mock_merge, mock_get_keys):
        # Setup
        mock_get_keys.return_value = ["file1.pdf", "file2.pdf"]
        mock_isfile.return_value = True
        
        # Execute with default optimize_pdf (False)
        lambda_function.process_merge("input-bucket", "input-key.json", "output-bucket", "output-key.pdf")
        
        # Assert
        mock_get_keys.assert_called_once_with("input-bucket", "input-key.json")
        # Check that merge_pdfs was called with optimize_pdf=False (default)
        mock_merge.assert_called_once()
        self.assertEqual(mock_merge.call_args[0][3], False)  # Fourth parameter should be optimize_pdf=False
        mock_upload.assert_called_once()
        mock_isfile.assert_called_once()
        mock_remove.assert_called_once()
    
    @patch('lambda_function.get_pdf_s3_keys')
    def test_process_merge_error(self, mock_get_keys):
        # Setup
        mock_get_keys.side_effect = Exception("Test error")
        
        # Execute and Assert
        with self.assertRaises(Exception) as context:
            lambda_function.process_merge("input-bucket", "input-key.json", "output-bucket", "output-key.pdf", True)
        
        self.assertIn("Test error", str(context.exception))

if __name__ == '__main__':
    unittest.main()
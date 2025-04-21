import boto3
import os
import json
import uuid
import fitz
import traceback


def lambda_handler(event, context):
    try:
        print('start merge pdf')
        print('event:', event)
        
        handle(event)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'PDF merge completed successfully'})
        }
    except Exception as e:
        error_message = str(e)
        stack_trace = traceback.format_exc()
        print(f"Error in lambda_handler: {error_message}")
        print(f"Stack trace: {stack_trace}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }

def handle(event):
    try:
        # get input from direct invoke cli
        if 'input_bucket' in event:
            print('cli command for input_bucket')
            # Check for optimize_pdf in direct CLI invocation, default to false
            optimize_pdf = event.get('optimize_pdf', False)
            print(f'optimize_pdf: {optimize_pdf}')
            process_merge(event['input_bucket'], event['input_file_key'], 
                         event['output_bucket'], event['output_file_key'], optimize_pdf)
        elif 'Records' in event:
            print('SQS')
            for rec in event['Records']:
                # get input from SQS events
                if 'messageId' in rec:
                    print('SQS')
                    message = json.loads(rec['body'])
                    if 'input_bucket' in message:
                        # Check for optimize_pdf in SQS message, default to false
                        optimize_pdf = message.get('optimize_pdf', False)
                        print(f'optimize_pdf: {optimize_pdf}')
                        process_merge(message['input_bucket'], message['input_file_key'], 
                                     message['output_bucket'], message['output_file_key'], optimize_pdf)
                    else:
                        error_msg = 'SQS JSON has an unrecognized format. Missing key for input_bucket or input_string.'
                        print(error_msg)
                        raise Exception(error_msg)
                else:
                    error_msg = 'Records JSON has an unrecognized format. Missing key for input_bucket or input_string.'
                    print(error_msg)
                    raise Exception(error_msg)
        else:
            error_msg = 'JSON has an unrecognized format. NOT a CLI or SQS.'
            print(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        print(f"Error in handle: {str(e)}")
        raise
  
def process_merge(input_bucket, input_file_key, output_bucket, output_file_key, optimize_pdf=False):
    try:
        print('input_bucket:',input_bucket)
        print('input_file_key:',input_file_key)
        print('output_bucket:',output_bucket)
        print('output_file_key:', output_file_key)
        print('optimize_pdf:', optimize_pdf)

        # final output file name
        guid = str(uuid.uuid4())
        local_output_file = f'/tmp/{guid}.pdf'

        print(f"Processing PDFs from JSON file: s3://{input_bucket}/{input_file_key}")
        pdf_keys = get_pdf_s3_keys(input_bucket, input_file_key)
        
        print(f"Downloading and merging PDFs from S3")
        merge_pdfs(input_bucket, pdf_keys, local_output_file, optimize_pdf)
        
        print(f"Uploading merged PDF to S3: s3://{output_bucket}/{output_file_key}")
        upload_file_to_s3(output_bucket, output_file_key, local_output_file)
            
        # clean up temp files
        if os.path.isfile(local_output_file):
            os.remove(local_output_file)
    except Exception as e:
        print(f"Error in process_merge: {str(e)}")
        raise

def get_pdf_s3_keys(input_bucket, input_file_key):
    """
    Read a JSON file from S3 containing PDF filenames and extract the PDF list.
    
    Args:
        input_bucket (str): S3 bucket containing the JSON file
        input_file_key (str): S3 object key for the JSON file
    
    Returns:
        list: Array of PDF filenames
    """
    try:
        print(f"Retrieving JSON file from S3: s3://{input_bucket}/{input_file_key}")
        # Create S3 client
        s3 = boto3.client('s3')
        
        # Get the JSON file directly from S3
        response = s3.get_object(Bucket=input_bucket, Key=input_file_key)
        
        # Read the JSON content from the response
        json_content = response['Body'].read().decode('utf-8')
        
        # Parse the JSON data
        json_data = json.loads(json_content)
        
        # Extract the array of PDF filenames
        pdf_files = json_data.get('pdfs', [])
        
        if not pdf_files:
            print("Warning: No PDF files found in JSON")
            return []
        
        print(f"Found {len(pdf_files)} PDFs to process")
        return pdf_files
    
    except json.JSONDecodeError as e:
        error_msg = f"Error parsing JSON data from S3: {e}"
        print(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Error retrieving or processing S3 object: {e}"
        print(error_msg)
        raise Exception(error_msg)
    
def download_pdf_from_s3(s3_bucket, s3_key):
    """
    Download a PDF file from S3 and return its binary content.
    
    Args:
        s3_key (str): S3 object key for the PDF file
    
    Returns:
        bytes: Binary content of the PDF file
    """
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        return response['Body'].read()
    except Exception as e:
        error_msg = f"Error downloading PDF from S3: {e}"
        print(error_msg)
        raise Exception(error_msg)

def merge_pdfs(input_s3_bucket, s3_keys, output_file,optimize_pdf=False):
    """
    Merge multiple PDF files into a single PDF.
    
    Args:
        input_s3_bucket (str): S3 bucket name containing the PDF files
        s3_keys (list): Array of S3 object keys for the PDFs to merge
        output_file (str): Path to save the merged PDF
    """
    try:
        # Initialize a new PDF document
        merged_pdf = fitz.open()
        
        for s3_key in s3_keys:
            # Download each PDF file from S3
            pdf_data = download_pdf_from_s3(input_s3_bucket, s3_key)
            
            if pdf_data:
                # Open the downloaded PDF data as a document
                pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
                
                # Append the document to the merged PDF
                merged_pdf.insert_pdf(pdf_document)
        
        # Save the merged PDF to disk        
        if optimize_pdf:
            print("Save PDF with PDF optimization")        
            merged_pdf.save(output_file, 
            deflate=True, 
            garbage=4, 
            clean=True)
        else:
            print("Save PDF.  NO optimization") 
            merged_pdf.save(output_file)

        merged_pdf.close()
        
        print(f"Merged PDF saved to {output_file}")
    
    except Exception as e:
        error_msg = f"Error merging PDFs: {e}"
        print(error_msg)
        raise Exception(error_msg)

def upload_file_to_s3(output_s3_bucket, output_file_key, local_output_file):
    """
    Upload a local file to an S3 bucket.
    
    Args:
        output_s3_bucket (str): Destination S3 bucket name
        output_file_key (str): S3 object key for the uploaded file
        local_output_file (str): Path to the local file to upload
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        print(f"Uploading {local_output_file} to s3://{output_s3_bucket}/{output_file_key}")
        
        # Create S3 bucket resource
        s3_bucket = boto3.resource("s3").Bucket(output_s3_bucket)
        
        # Upload file to S3
        with open(local_output_file, "rb") as file_data:
            s3_bucket.put_object(Key=output_file_key, Body=file_data)
        
        print(f"Successfully uploaded file to s3://{output_s3_bucket}/{output_file_key}")
        return True
        
    except Exception as e:
        error_msg = f"Error uploading file to S3: {e}"
        print(error_msg)
        raise Exception(error_msg)
    

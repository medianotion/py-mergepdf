# py-mergepdf
* AWS python 3.13 lambda that merges PDFs stored on S3 and stores the merged PDF on S3.
* It uses the fitz/pymupdf library via a Lambda layer (see build_layer.sh) to do the merging of PDFs.
* It has a optimize_pdf option that will shrink the merged PDF using fitz deflate, garbage and clean options.  Only use this on PDFs know to be bloated.
* This lambda can be called from CLI/Lambda_Invoke or by SQS trigger.

# build_layer.sh
Creates a lambda layer that must be deployed to AWS for fitz/pymupdf PDF library.


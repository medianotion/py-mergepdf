#!/bin/bash

set -e

rm -rf python pymupdf-layer.zip

docker run --platform linux/amd64 --rm -v "$PWD":/lambda-build -w /lambda-build amazonlinux:2023 bash -c "
  yum install -y gcc gcc-c++ make python3-devel python3-pip zip &&
  python3 -m venv venv &&
  source venv/bin/activate &&
  pip install --upgrade pip &&
  mkdir -p python &&
  pip install PyMuPDF -t python/ &&
  zip -r9 pymupdf-layer.zip python
"

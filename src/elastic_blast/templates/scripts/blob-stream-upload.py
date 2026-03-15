#!/usr/bin/env python3
"""
blob-stream-upload.py — Stream stdin to Azure Blob Storage

Used by blast-run-aks.sh to pipe BLAST output directly to Blob Storage
without writing intermediate files to disk.

Usage:
  blast ... -out /dev/stdout | gzip | python3 /scripts/blob-stream-upload.py BLOB_URL BLOB_NAME

Environment:
  Uses DefaultAzureCredential (Managed Identity in AKS pods).
"""

import sys
import os
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def parse_blob_url(url: str):
    """Parse https://<account>.blob.core.windows.net/<container>/path into components."""
    parsed = urlparse(url)
    account_url = f'{parsed.scheme}://{parsed.netloc}'
    parts = parsed.path.lstrip('/').split('/', 1)
    container = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    return account_url, container, prefix


def upload_stream(blob_url: str, blob_name: str):
    """Upload stdin to a blob under the given URL."""
    from azure.storage.blob import BlobServiceClient
    from azure.identity import DefaultAzureCredential

    account_url, container, prefix = parse_blob_url(blob_url)
    blob_path = f'{prefix}/{blob_name}'.lstrip('/')

    credential = DefaultAzureCredential()
    client = BlobServiceClient(account_url=account_url, credential=credential)
    blob_client = client.get_blob_client(container=container, blob=blob_path)

    data = sys.stdin.buffer.read()
    size = len(data)
    logging.info(f'Uploading {size} bytes to {container}/{blob_path}')
    blob_client.upload_blob(data, overwrite=True)
    logging.info(f'Upload complete: {size} bytes')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f'Usage: {sys.argv[0]} BLOB_URL BLOB_NAME', file=sys.stderr)
        sys.exit(1)
    upload_stream(sys.argv[1], sys.argv[2])

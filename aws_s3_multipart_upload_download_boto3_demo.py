"""
This is a demo of Multipart Upload/Download using AWS Python SDK -boto3 library.

This module provides high level abstractions for efficient
uploads/download.

* Automatically switching to multipart transfer when
  a file is over a specific size threshold
* Uploading/downloading a file in parallel
* Progress callbacks to monitor transfers

Written By: ANKHI PAUL
"""
import boto3
from boto3.s3.transfer import TransferConfig
import os
import threading
import sys

bucket_name = 'first-aws-bucket-1'

s3_resource = boto3.resource('s3')

# multipart_threshold : Ensure that multipart uploads/downloads only happen if the size of a transfer
# is larger than 25 MB
# multipart_chunksize : Each part size is of 25 MB
config = TransferConfig(multipart_threshold=1024 * 25,
                        max_concurrency=10,
                        multipart_chunksize=1024 * 25,
                        use_threads=True)

# Function to upload the file to s3 using multipart functionality
def multipart_upload_boto3():

    file_path = os.path.dirname(__file__) + '/multipart_upload_example.pdf'
    key = 'multipart-test/multipart_upload_example.pdf'


    s3_resource.Object(bucket_name, key).upload_file(file_path,
                            ExtraArgs={'ContentType': 'text/pdf'},
                            Config=config,
                            Callback=ProgressPercentage(file_path)
                            )

# Function to download the file to s3 using multipart functionality
def multipart_download_boto3():

    file_path = os.path.dirname(__file__)+ '/multipart_download_example.pdf'
    file_path1 = os.path.dirname(__file__)
    key = 'multipart-test/multipart_download_example.pdf'

    s3_resource.Object(bucket_name, key).download_file(file_path,
                            Config=config,
                            Callback=ProgressPercentage(file_path1)
                            )

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

if __name__ == '__main__':
 multipart_upload_boto3()
 multipart_download_boto3()
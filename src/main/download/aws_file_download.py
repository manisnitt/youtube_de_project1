import boto3
import traceback
import os
from src.main.utility.logging_config import *

class S3FileDownloader:
    def __init__(self,s3_client, bucket_name, local_directory):
        self.bucket_name = bucket_name
        self.local_directory = local_directory
        self.s3_client = s3_client

    def download_files(self, list_files):
        logger.info("Running download files for these files %s",list_files)
        for key in list_files:
            file_name = os.path.basename(key)
            logger.info("File name %s ",file_name)
            download_file_path = os.path.join(self.local_directory, file_name)
            try:
                self.s3_client.download_file(self.bucket_name,key,download_file_path)
            except Exception as e:
                error_message = f"Error downloading file '{key}': {str(e)}"
                traceback_message = traceback.format_exc()
                print(error_message)
                print(traceback_message)
                raise e


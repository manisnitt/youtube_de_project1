import boto3
import traceback
from src.main.utility.logging_config import *

class S3Reader:

    def list_files(self, s3_client, bucket_name,folder_path):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=folder_path)
            if 'Contents' in response:
                logger.info("Total files available in folder '%s' of bucket '%s': %s", folder_path, bucket_name, response)
                files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents'] if
                         not obj['Key'].endswith('/')]
                return files
            else:
                return []
        except Exception as e:
            error_message = f"Error listing files: {e}"
            traceback_message = traceback.format_exc()
            logger.error("Got this error : %s",error_message)
            print(traceback_message)
            raise


################### Directory will also be available if you use this ###########

    # def list_files(self, bucket_name):
    #     try:
    #         response = self.s3_client.list_objects_v2(Bucket=bucket_name)
    #         if 'Contents' in response:
    #             files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents']]
    #             return files
    #         else:
    #             return []
    #     except Exception as e:
    #         print(f"Error listing files: {e}")
    #         return []

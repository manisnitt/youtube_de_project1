import traceback
from src.main.utility.logging_config import *


def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        for obj in response.get('Contents', []):
            source_key = obj['Key']
            destination_key = destination_prefix + source_key[len(source_prefix):]

            s3_client.copy_object(Bucket=bucket_name,
                                  CopySource={'Bucket': bucket_name,
                                              'Key': source_key}, Key=destination_key)

            s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        return f"Data Moved succesfully from {source_prefix} to {destination_prefix}"
    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e


def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix,file_name=None):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        if file_name is None:
            for obj in response.get('Contents', []):
                source_key = obj['Key']
                destination_key = destination_prefix + source_key[len(source_prefix):]

                s3_client.copy_object(Bucket=bucket_name,
                                      CopySource={'Bucket': bucket_name,
                                                  'Key': source_key}, Key=destination_key)

                s3_client.delete_object(Bucket=bucket_name, Key=source_key)
            # return f"Data Moved succesfully from {source_prefix} to {destination_prefix}"
        else:
            for obj in response.get('Contents', []):
                source_key = obj['Key']

                if source_key.endswith(file_name):
                    destination_key = destination_prefix + source_key[len(source_prefix):]

                    s3_client.copy_object(Bucket=bucket_name,
                                          CopySource={'Bucket': bucket_name,
                                                      'Key': source_key}, Key=destination_key)

                    s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                    logger.info(f"Moved file: {source_key} to {destination_key}")
                # else:
                #     logger.info(f"Skipped file: {source_key} as it doesn't match the filename criteria")

        return f"Data Moved successfully from {source_prefix} to {destination_prefix}"
    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e


def move_local_to_local():
    pass

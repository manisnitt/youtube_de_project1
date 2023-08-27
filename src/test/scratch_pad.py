import shutil
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.spark_session import *
import os
from resources.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *

s3_client_provider = S3ClientProvider(decrypt(config.aws_access_key), decrypt(config.aws_secret_key))
s3_client = s3_client_provider.get_client()
# spark = spark_session()
# input("Press enter")

# import datetime
# current_epoch = int(datetime.datetime.now().timestamp())*1000
# print(current_epoch)
# local_folder_path = "C:\\Users\\nikita\\Documents\\data_engineering\\spark_data\\customer_data_mart"
# s3_prefix = f"sales_partitioned_data_mart"
# for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
#     for file in files:
#         print(file)
#         local_file_path = os.path.join(root, file)
#         s3_key = f"{s3_prefix}/{file}"
#         s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

# for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
#     for file in files:
#         print(file)
#         local_file_path = os.path.join(root, file)
#         relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
#         s3_key = f"{s3_prefix}/{relative_file_path}"
#         s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

# source_prefix = 'sales_data/'
# destination_prefix = 'sales_data_processed/'
# response = s3_client.list_objects_v2(Bucket=config.bucket_name, Prefix=source_prefix)
#
# for obj in response.get('Contents', []):
#     source_key = obj['Key']
#     destination_key = destination_prefix + source_key[len(source_prefix):]
#
#     s3_client.copy_object(Bucket=config.bucket_name,
#                           CopySource={'Bucket': config.bucket_name,
#                             'Key': source_key}, Key=destination_key)
#
#     s3_client.delete_object(Bucket=config.bucket_name, Key=source_key)

# import shutil
# delete_file_path = "C:\\Users\\nikita\\Documents\\data_engineering\\spark_data\\sales_partition_data\\"
#
# files_to_delete = [os.path.join(delete_file_path, filename) for filename in os.listdir(delete_file_path)]
#
# for item in files_to_delete:
#     if os.path.isfile(item):
#         os.remove(item)
#         print(f"Deleted file: {item}")
#     elif os.path.isdir(item):
#         shutil.rmtree(item)
#         print(f"Deleted folder: {item}")
#
#
# print("All files inside the folder have been deleted.")


# csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
# connection = get_mysql_connection()
# cursor = connection.cursor()
#
# total_csv_files = []
# if csv_files:
#     for file in csv_files:
#         total_csv_files.append(file)
#         print("1",file)
#     print(str(total_csv_files)[1:-1])
#     statement = f"select distinct file_name from youtube_project.product_staging_table " \
#                 f"where file_name in ({str(total_csv_files)[1:-1]}) and status='I' "
#     print(statement)
#     cursor.execute(statement)
#     data = cursor.fetchall()
#     print(data)
#     if data:
#         print("Your last run has failed please check")
#     else:
#         print("Your last run was succesfull")
#
#
# else:
#     print("Nofile")


# error_files = ["C:\\Users\\nikita\\Documents\\data_engineering\\spark_data\\file_from_s3\\sales_data.csv"]
# error_folder_local_path = config.error_folder_path_local
# for file_path in error_files:
#     if os.path.exists(file_path):
#         file_name = os.path.basename(file_path)
#         destination_path = os.path.join(error_folder_local_path, file_name)
#
#         shutil.move(file_path, destination_path)
#         print(f"Moved '{file_name}' to '{destination_path}'.")
#     else:
#         print(f"'{file_path}' does not exist.")

import boto3
import traceback
from src.main.utility.logging_config import *



def list_files( s3_client, bucket_name,folder_path):
    print("inside list file")
    try:
        print(folder_path)
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
folder_path = "sales_data/"
# list_files(s3_client,config.bucket_name,folder_path)
s3_absolute_file_path = list_files(s3_client, config.bucket_name,folder_path)
logger.info("Absolute path on s3 bucket for csv file %s ",s3_absolute_file_path)

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


# try:
#     response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
#     if 'Contents' in response:
#         logger.info("Total files available in folder '%s' of bucket '%s': %s", folder_path, bucket_name, response)
#         files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents'] if
#                  not obj['Key'].endswith('/')]
#         return files
#     else:
#         return []
#
# try:
#     response = s3_client.list_objects_v2(Bucket=bucket_name)
#     if 'Contents' in response:
#         logger.info("Total file available in bucket %s", response)
#         files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents'] if
#                  not obj['Key'].endswith('/')]
#         return files
#     else:
#         return []
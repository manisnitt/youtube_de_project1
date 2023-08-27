import boto3

class S3Deleter:
    def __init__(self, aws_access_key, aws_secret_key):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

    def delete_file(self, bucket_name, file_name):
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=file_name)
            print(f"File '{file_name}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting file: {e}")

    def delete_bucket(self, bucket_name):
        try:
            self.s3_client.delete_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting bucket: {e}")

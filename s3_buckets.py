import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class S3BucketManager:
    def __init__(self, endpoint_url, access_key, secret_key, bucket_name):
        """
        Initialize the S3 bucket manager with connection details.
        """
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self.bucket_name = bucket_name

    def list_objects(self):
        """
        List all objects in the bucket.
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in response:
                print("Objects in bucket:")
                for obj in response['Contents']:
                    print(f"- {obj['Key']}")
            else:
                print("Bucket is empty.")
        except Exception as e:
            print(f"Error listing objects: {e}")

    def upload_file(self, file_path, object_name=None):
        """
        Upload a file to the bucket.
        :param file_path: Path to the local file to be uploaded.
        :param object_name: Name to assign to the file in the bucket.
        """
        try:
            if object_name is None:
                object_name = file_path.split('/')[-1]
            self.s3.upload_file(file_path, self.bucket_name, object_name)
            print(f"File {file_path} uploaded as {object_name}.")
        except FileNotFoundError:
            print(f"The file {file_path} was not found.")
        except NoCredentialsError:
            print("Credentials not available.")
        except Exception as e:
            print(f"Error uploading file: {e}")

    def download_file(self, object_name, file_path):
        """
        Download a file from the bucket.
        :param object_name: Name of the file in the bucket to download.
        :param file_path: Path to save the downloaded file locally.
        """
        try:
            self.s3.download_file(self.bucket_name, object_name, file_path)
            print(f"File {object_name} downloaded to {file_path}.")
        except Exception as e:
            print(f"Error downloading file: {e}")

# Usage example:
if __name__ == "__main__":
    # Replace with your S3-compatible service details
    endpoint = "https://your-s3-endpoint.com"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    bucket_name = "your-bucket-name"

    # Initialize the S3 manager
    s3_manager = S3BucketManager(endpoint, access_key, secret_key, bucket_name)

    # List objects
    s3_manager.list_objects()

    # Upload a file
    #s3_manager.upload_file("path/to/local/file.txt", "uploaded_file.txt")

    # Download a file
    #s3_manager.download_file("uploaded_file.txt", "path/to/download/file.txt")

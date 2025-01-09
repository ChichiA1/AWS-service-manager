import os
from typing import Union
from datetime import datetime
import logging

import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv


class S3Connection:
    def __init__(self) -> None:
        """
        Initializes the S3 client using AWS credentials from environment variables.

        This method loads AWS access keys (access_key, secret_key) from environment variables
        using `load_dotenv()`. If the credentials are not set, a ValueError is raised. If
        there is an issue creating the S3 client, it raises a ValueError with the error message.

        :return: None
        """
        load_dotenv()
        access_key = os.getenv("access_key")
        secret_key = os.getenv("secret_access_key")
        region_name = os.getenv("region_name", "us-east-1")  # Default to 'us-east-1' if not set)

        if not access_key or not secret_key:
            raise ValueError("AWS credentials are not set in the environment variables.")

        try:
            # Create an S3 client using the provided credentials
            self.client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                                       region_name=region_name)
        except Exception as e:
            raise ValueError(f"Failed to create S3 client: {e}")

    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a new S3 bucket in the specified region.

        :param bucket_name: The name of the bucket to create
        :return: True if the bucket was created, False if there was an error
        """
        # Check if the bucket already exists
        list_of_buckets = self.client.list_buckets().get("Buckets")

        # If the bucket name already exists, modify the name to make it unique
        if bucket_name in [bucket.get("Name") for bucket in list_of_buckets]:
            name = str(datetime.now()).split()[0]
            name = "".join(name.split("-"))
            bucket_name = f"{bucket_name}-{name}"

        # Print the new bucket name for clarity
        print(f"Bucket name: {bucket_name}")

        # Get the AWS region
        region = os.getenv("AWS_REGION", "us-east-1")

        try:
            # Create the bucket with location constraint if the region is not us-east-1
            if region == "us-east-1":
                self.client.create_bucket(Bucket=bucket_name)
            else:
                self.client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"Bucket {bucket_name} created successfully in region {region}.")
            return True
        except Exception as e:
            print(f"Error creating bucket: {str(e)}")
            return False

    def get_all_buckets(self)->list:
        """
        list all S3 bucket.

        :param: none
        :return: a list
        """
        all_buckets = []
        for items in self.client.list_buckets().get("Buckets"):
            all_buckets.append(items.get("Name"))

        return all_buckets

    def delete_bucket(self, bucket_name: str)->bool:
        """
        Delete an S3 bucket.

        :param bucket_name: The S3 bucket name
        :return: True if the bucket was deleted, else False
        """

        try:
            self.client.delete_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} deleted successfully.")
            return True
        except Exception as e:
            print(f"Error deleting bucket: {str(e)}")
        return False

    def read_file(self, key: str, bucket_name: str) -> Union[pd.DataFrame, None]:
        """Reads a CSV file from an S3 bucket and returns it as a pandas DataFrame."""
        try:
            # Fetch the object from S3
            response = self.client.get_object(Bucket=bucket_name, Key=key)

            # Check if the response status code indicates success (HTTP 200)
            status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            if status_code != 200:
                raise ValueError(f"Failed to retrieve object. Status code: {status_code}")

            # Read the CSV file from the response body
            df = pd.read_csv(response['Body'])
            return df

        except ClientError as e:
            # Specific error handling for AWS S3 client errors
            print(f"S3 ClientError: {e}")
        except ValueError as e:
            # Handle case where response status code is not 200
            print(f"ValueError: {e}")
        except Exception as e:
            # Generic exception handling for any other errors
            print(f"An unexpected error occurred: {e}")

        return None  # Return None if an error occurs

    def delete_file(slef, bucket_name: str, object_name:str)->bool:
        """
        Delete a file from an S3 bucket.

        :param bucket_name: The S3 bucket name
        :param object_name: The file to delete
        :return: True if the file was deleted, else False
        """

        try:
            self.client.delete_object(Bucket=bucket_name, Key=object_name)
            print(f"File {object_name} deleted from {bucket_name}")
            return True
        except Exception as e:
            print(f"Error deleting file: {str(e)}")
        return False

    def get_object_metadata(self, bucket_name: str, key: str) -> Union[dict, None]:
        """
        Get metadata for an object in S3.

        :param bucket_name: The S3 bucket name
        :param key: The object in the bucket
        :return: Metadata dictionary or None if error occurred
        """
        self.client = get_client("s3")

        try:
            response = self.client.head_object(Bucket=bucket_name, Key=key)
            print(f"Metadata for {key}: {response}")
            return response
        except Exception as e:
            print(f"Error getting metadata: {str(e)}")
        return None

    def copy_object(self, source_bucket: str, source_object: str, destination_bucket: str, destination_object: str)\
            -> bool:
        """
        Copy an object from one S3 bucket to another.

        :param source_bucket: The source bucket name
        :param source_object: The object in the source bucket
        :param destination_bucket: The destination bucket name
        :param destination_object: The name of the object in the destination bucket
        :return: True if the object was copied, else False
        """
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_object}
            self.client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_object)
            print(f"Copied {source_object} from {source_bucket} to {destination_bucket}/{destination_object}")
            return True
        except Exception as e:
            print(f"Error copying object: {str(e)}")
        return False


    def upload_to_s3(self, data: Union[str, pd.DataFrame], bucket_name: str, key: str, is_dataframe: bool = False) -> bool:
        """
        General method to upload either a file or a DataFrame to S3.

        If the file (or object) already exists in the S3 bucket, the method returns `True`.
        If the file does not exist, it uploads the file or DataFrame to the S3 bucket and returns `False`.

        Params:
            data (Union[str, pd.DataFrame]): The input data, which can either be:
                - a local file path (str) when uploading a file, or
                - a pandas DataFrame (pd.DataFrame) when uploading data as CSV.
            bucket_name (str): The name of the S3 bucket where the file or DataFrame should be uploaded.
            key (str): The key (path) for the object in the S3 bucket.
            is_dataframe (bool): Flag indicating whether the `data` is a DataFrame. Defaults to False (indicating `data` is a file).

        Returns:
            bool: `True` if the file already exists in the bucket, `False` if the file is uploaded.

        Raises:
            Exception: If there is an error during the S3 interaction (e.g., invalid credentials, network issues).
        """
        try:
            # Check if the object exists in the bucket
            self.client.head_object(Bucket=bucket_name, Key=key)
            logging.info(f"File {key} already exists in bucket {bucket_name}.")
            return True  # File exists
        except Exception as e:
            logging.info(f"File {key} not found, uploading a new file.")
            if is_dataframe:
                # Handle DataFrame upload
                csv_buffer = StringIO()
                data.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                self.client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
                logging.info(f"DataFrame uploaded as {key} to bucket {bucket_name}.")
            else:
                # Handle file upload
                self.client.upload_file(data, bucket_name, key)  # `data` is a file path here
                logging.info(f"File {data} uploaded as {key} to bucket {bucket_name}.")
            return False  # File does not exist or uploaded


    def download_file(self, bucket_name: str, key: str, file_path: str)->bool:
        """
        Download a file from an S3 bucket.

        :param bucket_name: The S3 bucket name
        :param key: The name of the file in the bucket
        :param file_path: Local path to save the file
        :return: True if the file was downloaded, else False
        """
        try:
            self.client.download_file(bucket_name, key, file_path)
            print(f"File downloaded successfully from {bucket_name}/{key} to {file_path}")
            return True
        except Exception as e:
            print(f"Error downloading file: {str(e)}")
        return False

    def list_files(self, bucket_name: str)->list:
        """
        List all files in an S3 bucket.

        :param bucket_name: The S3 bucket name
        :return: List of files in the bucket
        """

        try:
            response = self.client.list_objects_v2(Bucket=bucket_name)
            files = [obj['Key'] for obj in response.get('Contents', [])]
            return files
        except Exception as e:
            print(f"Error listing files: {str(e)}")
        return []

    def delete_file(self, bucket_name: str, key: str)->bool:
        """
        Delete a file from an S3 bucket.

        :param bucket_name: The S3 bucket name
        :param key: The file to delete
        :return: True if the file was deleted, else False
        """
        try:
            self.client.delete_object(Bucket=bucket_name, Key=key)
            print(f"File {key} deleted from {bucket_name}")
            return True
        except Exception as e:
            print(f"Error deleting file: {str(e)}")
        return False

    def write_df(self, df: pd.DataFrame, bucket_name: str, key: str) -> bool:
        """
        Uploads a DataFrame as a CSV file to an S3 bucket.

        If the file already exists in the S3 bucket, the method logs a message and returns `True`.
        If the file does not exist, it uploads the DataFrame as a new file to the S3 bucket and returns `False`.

        Params:
            df (pd.DataFrame): The pandas DataFrame to upload.
            bucket_name (str): The name of the S3 bucket.
            key (str): The key (file path) for the object in the S3 bucket.

        Returns:
            bool: `True` if the file already exists, `False` if the file was uploaded.

        Raises:
            ClientError: If there is an issue interacting with S3 (e.g., permission errors).
        """
        try:
            # Check if the object exists in the bucket
            self.client.head_object(Bucket=bucket_name, Key=key)
            logging.info(f"File {key} already exists in bucket {bucket_name}.")
            return True  # File exists
        except ClientError as e:
            # If the error code is 'NoSuchKey', it means the file does not exist
            if e.response['Error']['Code'] == 'NoSuchKey':
                logging.info(f"File {key} not found, uploading a new file.")
            else:
                logging.error(f"Error checking file {key}: {e}")
                raise  # Re-raise the error if it wasn't a "NoSuchKey" error

            # Create an in-memory buffer for the CSV file
            csv_buffer = StringIO()
            # Copy the DataFrame to the in-memory buffer
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)  # Rewind the buffer to the start before uploading

            # Upload the CSV to S3
            try:
                self.client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
                logging.info(f"File {key} successfully uploaded to bucket {bucket_name}.")
                return False  # File does not exist, and now uploaded
            finally:
                # Ensure that the buffer is closed to release resources
                csv_buffer.close()

class RedShift:
    pass


if __name__ == "__main__":
    # instantiate the class
    conn = S3Connection()
    # create s3 bucket
    conn.create_bucket("test-bucket-docs-latest")
    # delete s3 bucket
    conn.delete_bucket("test-bucket-docs-latest")
    # create s3 bucket
    conn.create_bucket("test-bucket-docs-latest")
    # upload dataset
    conn.upload_to_s3("test doc dataset.csv", "test-bucket-docs-latest", "test-bucket-docs2025/test doc dataset.csv")
    delete file
    conn.delete_file("test-bucket-docs-latest", "test-bucket-docs-latest/test doc dataset.csv")
    # upload dataset
    conn.upload_to_s3("test doc dataset.csv", "test-bucket-docs-latest", "test-bucket-docs-latest/test doc dataset.csv")
    # download dataset
    conn.download_file("test-bucket-docs-latest", "test-bucket-docs-latest/test doc dataset.csv", "./test doc dataset.csv")
    # read dataset and put this in a dataframe
    test_dataset = conn.read_file("test-bucket-docs-latest/test doc dataset.csv", "test-bucket-docs-latest")
    # upload dataFrame
    conn.write_df(test_dataset, "test-bucket-docs-latest", "test-bucket-docs-latest/test doc dataset.csv")
import pandas as pd
from dotenv import load_dotenv
import os
import boto3


class S3Connection:
    @classmethod
    def create_conn(cls, bucket):
        load_dotenv()
        client = boto3.client('s3', aws_access_key_id=os.getenv("access_key"),
                              aws_secret_access_key=os.getenv("secret_access_key"))
        return cls(bucket=bucket, conn=client)

    def __init__(self, bucket, conn):
        self.bucket = bucket
        self.client = conn

    def get_all_buckets(self):
        all_buckets = []
        for items in self.client.list_buckets().get("Buckets"):
            all_buckets.append(items.get("Name"))

        return all_buckets

    def read_file(self, key):
        #all_buckets = self.get_all_buckets()
        # assert bucket in all_buckets, f"The {bucket} does not exists"
        try:
            response = self.client.get_object(Bucket=self.bucket,
                                              Key=key)
            status = response.get('ResponseMetadata', {}).get('HTTPStatusCode', {})
            print(f"Successfully pulled the data: Status - {status}")
            df = pd.read_csv(response.get("Body"))
            print(df.head())
        except Exception as e:
            print(e)
            # print(f"Unable to pull the data: status - {status}")


    def upload_file(self):
        pass

    def write_df(self):
        pass


class RedShift:
    pass


if __name__ == "__main__":





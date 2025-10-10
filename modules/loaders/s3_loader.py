import boto3
import os
import pandas as pd
from io import BytesIO
from modules.utils.logger import setup_logger

class S3Loader:
    """Upload de DataFrame e bytes para S3"""
    def __init__(self, bucket: str = None):
        self.bucket = bucket or os.getenv("S3_BUCKET")
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.logger = setup_logger(self.__class__.__name__)
        self.logger.info(f"S3Loader initialized for bucket: {self.bucket}")

    def upload_dataframe(self, df: pd.DataFrame, key: str):
        """Upload DataFrame as Parquet to S3"""
        try:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            out_buffer.seek(0)
            self.s3_client.upload_fileobj(out_buffer, self.bucket, key)
            self.logger.info(f"DataFrame uploaded to s3://{self.bucket}/{key} ({len(df)} records)")
        except Exception as e:
            self.logger.error(f"Failed to upload DataFrame to S3: {e}")
            raise

    def upload_bytes(self, data: bytes, key: str):
        """Upload raw bytes to S3"""
        try:
            buffer = BytesIO(data)
            self.s3_client.upload_fileobj(buffer, self.bucket, key)
            self.logger.info(f"Bytes uploaded to s3://{self.bucket}/{key} ({len(data)} bytes)")
        except Exception as e:
            self.logger.error(f"Failed to upload bytes to S3: {e}")
            raise

    def download_dataframe(self, key: str) -> pd.DataFrame:
        """Download Parquet from S3 as DataFrame"""
        try:
            buffer = BytesIO()
            self.s3_client.download_fileobj(self.bucket, key, buffer)
            buffer.seek(0)
            df = pd.read_parquet(buffer)
            self.logger.info(f"DataFrame downloaded from s3://{self.bucket}/{key} ({len(df)} records)")
            return df
        except Exception as e:
            self.logger.error(f"Failed to download DataFrame from S3: {e}")
            raise

    def list_objects(self, prefix: str = "") -> list:
        """List objects in S3 bucket with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if 'Contents' in response:
                objects = [obj['Key'] for obj in response['Contents']]
                self.logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
                return objects
            else:
                self.logger.info(f"No objects found with prefix '{prefix}'")
                return []
        except Exception as e:
            self.logger.error(f"Failed to list objects from S3: {e}")
            raise
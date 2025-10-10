# modules/storage/s3_uploader.py
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import io

class S3Uploader:
    def __init__(self, bucket: str = None):
        """
        Inicializa o uploader S3.
        
        Args:
            bucket: nome do bucket padrão. Se None, usa AWS_BUCKET do .env
        """
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket = bucket or os.getenv("AWS_BUCKET")
        
        self.session = boto3.session.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        self.s3_client = self.session.client("s3")

        if not self.bucket:
            raise ValueError("Bucket S3 não definido. Passe no construtor ou no .env com AWS_BUCKET.")

    def upload_dataframe(self, df, key: str):
        """
        Faz upload de um DataFrame para o S3 como Parquet.

        Args:
            df: pandas DataFrame a ser enviado
            key: caminho completo no S3 (ex: "processed/iptu_unificado.parquet")
        """
        try:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
            parquet_buffer.seek(0)

            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=parquet_buffer.getvalue()
            )
            print(f"[INFO] Upload Parquet concluído: s3://{self.bucket}/{key}")
        except (BotoCoreError, ClientError) as e:
            print(f"[ERROR] Falha ao enviar Parquet para S3: {e}")
            raise

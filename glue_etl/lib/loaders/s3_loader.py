# lib/loaders/s3_loader.py
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import boto3
import re
from lib.utils.logger import Logger
import uuid

logger = Logger()

class S3Loader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_parquet(self, df: DataFrame, path: str, final_name: str) -> DataFrame:
        """
        Escreve um único arquivo .parquet em S3 com nome específico.
        """
        
        # Pasta temporária
        temp_folder = f"{path}_temp_{uuid.uuid4()}/"
        logger.info(f"Writing to temp folder: {temp_folder}")

        # Escreve em temp com coalesce(1)
        df.coalesce(1).write.mode("overwrite").parquet(temp_folder)

        # Renomeia o arquivo part-*.parquet para o nome final
        s3 = boto3.client('s3')
        bucket, prefix = self._split_s3_path(path)
        temp_bucket, temp_prefix = self._split_s3_path(temp_folder)

        # Lista arquivos na pasta temp
        response = s3.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
        
        # Encontra o arquivo .parquet (ignora _SUCCESS e outros)
        parquet_file = None
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet'):
                parquet_file = key
                logger.info(f"Found parquet file: {key}")
                break

        if not parquet_file:
            all_files = [obj['Key'] for obj in response.get('Contents', [])]
            raise RuntimeError(f"No .parquet file found. Files: {all_files}")

        # Copia para o nome final
        final_key = f"{prefix}{final_name}.parquet"
        logger.info(f"Copying to final location: s3://{bucket}/{final_key}")
        
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': temp_bucket, 'Key': parquet_file},
            Key=final_key
        )

        # Remove pasta temporária
        logger.info("Cleaning up temp folder")
        for obj in response.get('Contents', []):
            s3.delete_object(Bucket=temp_bucket, Key=obj['Key'])

        logger.info(f"Successfully wrote: s3://{bucket}/{final_key}")
        return df
    
    def read_parquet(self, path: str) -> DataFrame:
        """
        Lê um arquivo ou diretório parquet do S3.
        """
        
        logger.info(f"Reading parquet from: {path}")
        df = self.spark.read.parquet(path)
        logger.info(f"Successfully read parquet with {df.count()} rows")
        return df
    
    def _split_s3_path(self, s3_path: str):
        """
        Converte um caminho S3 (s3://bucket/key/...) em (bucket, key)
        """
        match = re.match(r's3://([^/]+)/(.+)', s3_path)
        if not match:
            raise ValueError(f"Caminho S3 inválido: {s3_path}")
        return match.group(1), match.group(2)

    def write_csv_singlefile(self, df: DataFrame, path: str, final_name: str) -> DataFrame:
        """
        Escreve um único arquivo .csv em S3 com nome específico.
        """
        
        # Pasta temporária
        temp_folder = f"{path}_temp_{uuid.uuid4()}/"
        logger.info(f"Writing CSV to temp folder: {temp_folder}")

        # Escreve em temp com coalesce(1) e header
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_folder)

        # Renomeia o arquivo part-*.csv para o nome final
        s3 = boto3.client('s3')
        bucket, prefix = self._split_s3_path(path)
        temp_bucket, temp_prefix = self._split_s3_path(temp_folder)

        # Lista arquivos na pasta temp
        response = s3.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
        
        # Encontra o arquivo .csv (ignora _SUCCESS e outros)
        csv_file = None
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                csv_file = key
                logger.info(f"Found CSV file: {key}")
                break

        if not csv_file:
            all_files = [obj['Key'] for obj in response.get('Contents', [])]
            raise RuntimeError(f"No .csv file found. Files: {all_files}")

        # Copia para o nome final
        final_key = f"{prefix}{final_name}.csv"
        logger.info(f"Copying to final location: s3://{bucket}/{final_key}")
        
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': temp_bucket, 'Key': csv_file},
            Key=final_key
        )

        # Remove pasta temporária
        logger.info("Cleaning up temp folder")
        for obj in response.get('Contents', []):
            s3.delete_object(Bucket=temp_bucket, Key=obj['Key'])

        logger.info(f"Successfully wrote: s3://{bucket}/{final_key}")
        return df

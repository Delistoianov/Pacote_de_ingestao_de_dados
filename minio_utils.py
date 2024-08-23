from minio import Minio
from minio.error import S3Error
import pandas as pd
import os

def save_csv_as_parquet_to_minio():
    client = Minio(
        "localhost:9000",  
        access_key="minioadmin",  
        secret_key="minioadmin",  
        secure=False  
    )

    csv_file_path = os.path.join("..", "Data", "tmp", "sku_price.csv")
    parquet_file_path = os.path.join("..", "Data", "tmp", "sku_price.parquet")
    bucket_name = "test-bucket"
    parquet_file_name = "sku_price.parquet"

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso.")
        else:
            print(f"Bucket '{bucket_name}' já existe.")

        df = pd.read_csv(csv_file_path)
        print(f"Arquivo CSV '{csv_file_path}' lido com sucesso.")

        df.to_parquet(parquet_file_path, index=False)
        print(f"Arquivo convertido para Parquet: '{parquet_file_path}'")

        client.fput_object(bucket_name, parquet_file_name, parquet_file_path)
        print(f"Arquivo Parquet '{parquet_file_name}' enviado com sucesso para o bucket '{bucket_name}'.")

    except S3Error as e:
        print(f"Erro ao interagir com o MinIO: {e}")
    except Exception as e:
        print(f"Erro geral: {e}")

if __name__ == "__main__":
    save_csv_as_parquet_to_minio()

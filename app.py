import os
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd

script_dir = os.path.dirname(__file__)
create_table_sql = os.path.join(script_dir, 'sql/create_table.sql')
create_view_sql = os.path.join(script_dir, 'sql/create_view.sql')

create_bucket_if_not_exists("raw-data")

execute_sql_script(create_table_sql)


def handle():
    try:
        df = pd.read_csv('Data/tmp/sku_price.csv', encoding='latin1')
        
        if df.empty:
            print("O DataFrame lido está vazio.")
            return

        filename = process_data(df)
        
        upload_file('raw-data', filename)

        downloaded_filename = f"downloaded_{filename}"
        download_file('raw-data', filename, downloaded_filename)

        df_parquet = pd.read_parquet(downloaded_filename)
        
        df_prepared = prepare_dataframe_for_insert(df_parquet)

        if df_prepared.empty:
            print("O DataFrame preparado está vazio.")
            return

        client = get_client()
        insert_dataframe(client, 'working_data', df_prepared)
        
        print("Processamento concluído com sucesso.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

execute_sql_script(create_view_sql)

if __name__ == "__main__":
    handle()

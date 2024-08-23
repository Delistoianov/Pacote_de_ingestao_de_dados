import pytest
import pandas as pd
from unittest.mock import patch, mock_open, MagicMock
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
import os

@patch('clickhouse_connect.get_client')
def test_get_client(mock_get_client):
    mock_get_client.return_value = 'mock_client'

    client = get_client()

    mock_get_client.assert_called_once_with(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=os.getenv('CLICKHOUSE_PORT')
    )
    
    assert client == 'mock_client'

@patch('builtins.open', new_callable=mock_open, read_data='SELECT 1')
@patch('data_pipeline.clickhouse_client.get_client')
def test_execute_sql_script(mock_get_client, mock_open_file):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    client = execute_sql_script('dummy_path.sql')

    mock_open_file.assert_called_once_with('dummy_path.sql', 'r')

    mock_client.command.assert_called_once_with('SELECT 1')

    assert client == mock_client

@patch('data_pipeline.clickhouse_client.get_client')
def test_insert_dataframe(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})

    insert_dataframe(mock_client, 'dummy_table', df)

    mock_client.insert_df.assert_called_once_with('dummy_table', df)

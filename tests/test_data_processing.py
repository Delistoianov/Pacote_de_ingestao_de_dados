import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import patch, MagicMock
from datetime import datetime
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert

@patch('data_pipeline.data_processing.pq.write_table')
@patch('data_pipeline.data_processing.datetime')
def test_process_data(mock_datetime, mock_write_table):
    mock_datetime.now.return_value = datetime(2023, 8, 23, 15, 45, 0)
    mock_datetime.strftime = datetime.strftime
    
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    
    filename = process_data(df)
    
    expected_filename = "raw_data_20230823154500.parquet"
    
    assert filename == expected_filename
    
    mock_write_table.assert_called_once()
    args, kwargs = mock_write_table.call_args
    table_written = args[0]
    assert isinstance(table_written, pa.Table)

    table_expected = pa.Table.from_pandas(df)
    assert table_written.equals(table_expected)

@patch('data_pipeline.data_processing.datetime')
def test_prepare_dataframe_for_insert(mock_datetime):
    mock_datetime.now.return_value = datetime(2023, 8, 23, 15, 45, 0)

    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})

    df_prepared = prepare_dataframe_for_insert(df)

    assert 'data_ingestao' in df_prepared.columns
    assert 'dado_linha' in df_prepared.columns
    assert 'tag' in df_prepared.columns

    assert all(df_prepared['data_ingestao'] == datetime(2023, 8, 23, 15, 45, 0))
    assert all(df_prepared['tag'] == 'example_tag')

    expected_json_0 = '{"col1":1,"col2":"a","data_ingestao":1692805500000}'
    expected_json_1 = '{"col1":2,"col2":"b","data_ingestao":1692805500000}'
    assert df_prepared['dado_linha'].iloc[0] == expected_json_0
    assert df_prepared['dado_linha'].iloc[1] == expected_json_1

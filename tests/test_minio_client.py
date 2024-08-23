import pytest
from unittest.mock import patch, MagicMock
import os
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file

# Testando a função create_bucket_if_not_exists
@patch('data_pipeline.minio_client.minio_client')
def test_create_bucket_if_not_exists(mock_minio_client):
    bucket_name = "test-bucket"

    # Caso o bucket não exista
    mock_minio_client.bucket_exists.return_value = False

    create_bucket_if_not_exists(bucket_name)

    # Verifica se a função make_bucket foi chamada
    mock_minio_client.make_bucket.assert_called_once_with(bucket_name)

    # Reseta o mock
    mock_minio_client.reset_mock()

    # Caso o bucket já exista
    mock_minio_client.bucket_exists.return_value = True

    create_bucket_if_not_exists(bucket_name)

    # Verifica se a função make_bucket NÃO foi chamada
    mock_minio_client.make_bucket.assert_not_called()

# Testando a função upload_file
@patch('data_pipeline.minio_client.minio_client')
def test_upload_file(mock_minio_client):
    bucket_name = "test-bucket"
    file_path = "/path/to/file.txt"
    file_name = os.path.basename(file_path)

    upload_file(bucket_name, file_path)

    # Verifica se a função fput_object foi chamada corretamente
    mock_minio_client.fput_object.assert_called_once_with(bucket_name, file_name, file_path)

# Testando a função download_file
@patch('data_pipeline.minio_client.minio_client')
def test_download_file(mock_minio_client):
    bucket_name = "test-bucket"
    file_name = "file.txt"
    local_file_path = "/path/to/local_file.txt"

    download_file(bucket_name, file_name, local_file_path)

    # Verifica se a função fget_object foi chamada corretamente
    mock_minio_client.fget_object.assert_called_once_with(bucket_name, file_name, local_file_path)

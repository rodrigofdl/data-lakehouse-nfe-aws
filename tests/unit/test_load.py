# Imports Third-Party Libraries
import pytest
import pandas as pd
import pyarrow as pa

# Imports Local Libraries
import pipeline.load


@pytest.fixture
def mock_s3_fs(mocker):
    """
    Fixture to simulate S3Filesystem with customizable behavior in each test.
    """
    mock_s3_fs_class = mocker.patch("pipeline.load.S3FileSystem")
    mock_fs_instance = mocker.MagicMock()
    mock_s3_fs_class.return_value = mock_fs_instance
    return mock_fs_instance


@pytest.mark.unit
def test_save_partitioned_with_existing_partition(mocker, mock_s3_fs):
    """
    Tests if the function removes an existing partition before saving new data.
    """
    # Arrange
    mock_write_dataset = mocker.patch("pipeline.load.ds.write_dataset")
    mock_logger_info = mocker.patch("pipeline.load.logger.info")

    mock_s3_fs.isdir.return_value = True  # Simulates that the partition exists

    df = pd.DataFrame({"produto": ["A"], "valor": [100], "ano": [2024], "mes": [6]})
    s3_base_path = "s3://meu-bucket/dados"
    partition_path = f"{s3_base_path}/ano=2024/mes=6"

    # Act
    pipeline.load.save_parquet_partitioned(df, s3_base_path)

    # Assert
    mock_s3_fs.isdir.assert_called_once_with(partition_path)
    mock_s3_fs.delete_dir_contents.assert_called_once_with(partition_path)
    mock_logger_info.assert_called_once_with(
        f"Partição existente limpa: {partition_path}"
    )

    mock_write_dataset.assert_called_once()

    assert mock_write_dataset.call_count == 1
    call_args = mock_write_dataset.call_args.kwargs
    assert call_args["base_dir"] == s3_base_path
    assert call_args["partitioning"] == ["ano", "mes"]
    assert isinstance(call_args["data"], pa.Table)


@pytest.mark.unit
def test_save_partitioned_with_new_partition(mocker, mock_s3_fs):
    """
    Tests whether the function does not try to delete a partition that does not exist.
    """
    # Arrange
    mock_write_dataset = mocker.patch("pipeline.load.ds.write_dataset")
    mock_s3_fs.isdir.return_value = False  # Simulates that the partition does not exist

    df = pd.DataFrame({"ano": [2024], "mes": [7]})
    s3_base_path = "s3://meu-bucket/dados"

    # Act
    pipeline.load.save_parquet_partitioned(df, s3_base_path)

    # Assert
    mock_s3_fs.delete_dir_contents.assert_not_called()
    mock_write_dataset.assert_called_once()


@pytest.mark.unit
def test_save_parquet_with_path_invalid(mocker):
    """
    Tests if the function is a log of error and closes the execution if Path S3 is invalid.
    """
    # Arrange
    mock_logger_error = mocker.patch("pipeline.load.logger.error")

    df = pd.DataFrame({"ano": [2024], "mes": [1]})
    invalid_path = "   "  # Invalid path with only spaces

    # Act & Assert
    with pytest.raises(SystemExit) as e:
        pipeline.load.save_parquet_partitioned(df, invalid_path)

    assert e.type == SystemExit
    assert e.value.code == 1
    mock_logger_error.assert_called_once_with(
        "s3_base_path não encontrado. Certifique-se de que o arquivo .env está configurado corretamente."
    )

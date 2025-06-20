import pytest
import pandas as pd
import pyarrow as pa

# Imports Local Libraries
from pipeline import load
from pipeline.load import MissingS3PathError, LoadError


@pytest.fixture
def mock_s3_fs(mocker):
    """
    Fixture to simulate S3Filesystem with customizable behavior in each test.
    """
    mock_s3_fs_class = mocker.patch("load.S3FileSystem")
    mock_fs_instance = mocker.MagicMock()
    mock_s3_fs_class.return_value = mock_fs_instance
    return mock_fs_instance


@pytest.mark.unit
def test_save_partitioned_with_existing_partition(mocker, mock_s3_fs):
    """
    Tests if the function removes an existing partition before saving new data.
    """
    # Arrange
    mock_write_dataset = mocker.patch("load.ds.write_dataset")
    mock_logger_info = mocker.patch("load.logger.info")

    mock_s3_fs.isdir.return_value = True  # Simulates that the partition exists

    df = pd.DataFrame({"produto": ["A"], "valor": [100], "ano": [2024], "mes": [6]})
    s3_base_path = "s3://meu-bucket/dados"
    expected_partition_path = f"{s3_base_path}/ano=2024/mes=6"

    # Act
    load.save_parquet_partitioned(df, s3_base_path)

    # Assert
    mock_s3_fs.isdir.assert_called_once_with(expected_partition_path)
    mock_s3_fs.delete_dir_contents.assert_called_once_with(expected_partition_path)
    mock_write_dataset.assert_called_once()

    # Check write_dataset call params
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
    mock_write_dataset = mocker.patch("load.ds.write_dataset")
    mock_s3_fs.isdir.return_value = False  # Simulates that the partition does not exist

    df = pd.DataFrame({"ano": [2024], "mes": [7]})
    s3_base_path = "s3://meu-bucket/dados"

    # Act
    load.save_parquet_partitioned(df, s3_base_path)

    # Assert
    mock_s3_fs.delete_dir_contents.assert_not_called()
    mock_write_dataset.assert_called_once()


@pytest.mark.unit
def test_save_parquet_partitioned_empty_s3_base_path():
    """
    Test if save_parquet_partitioned raises MissingS3PathError when s3_base_path is empty.
    """
    # Arrange
    df = pd.DataFrame({"ano": [2024], "mes": [7]})
    invalid_path = "   "  # Simulates empty string or string with spaces

    # Act & Assert
    with pytest.raises(
        MissingS3PathError, match="Parâmetro s3_base_path ausente ou vazio"
    ):
        load.save_parquet_partitioned(df, invalid_path)


@pytest.mark.unit
def test_save_parquet_partitioned_raises_load_error_on_write_failure(mocker):
    """
    Test if save_parquet_partitioned raises LoadError when write_dataset fails.
    """
    # Arrange
    mock_write_dataset = mocker.patch("pipeline.load.ds.write_dataset")

    mock_write_dataset.side_effect = Exception("Falha de gravação no S3")

    df = pd.DataFrame({"ano": [2024], "mes": [7]})
    s3_base_path = "s3://fake-bucket/data"

    # Act & Assert
    with pytest.raises(LoadError, match="Falha ao carregar os dados no S3"):
        load.save_parquet_partitioned(df, s3_base_path)

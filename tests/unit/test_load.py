# Imports Third-Party Libraries
import pytest
import pandas as pd
import pyarrow as pa

# Imports Local Libraries
import pipeline.load


@pytest.mark.unit
def test_save_partitioned_with_existing_partition(mocker):
    """
    Checks if the function deletes the old partition and writes the new.
    """
    # Arrange: Mocking the necessary components
    # Mock the write_dataset function
    mock_write_dataset = mocker.patch("pipeline.load.ds.write_dataset")
    # Mock the S3FileSystem class
    mock_s3_fs_class = mocker.patch("pipeline.load.S3FileSystem")
    # Creates a stuntman for S3FileSystem
    mock_fs_instance = mocker.MagicMock()
    # Simulates that the partition already exists
    mock_fs_instance.isdir.return_value = True
    # Makes it S3FileSystem() Return our stunt
    mock_s3_fs_class.return_value = mock_fs_instance
    # Mock the logger to check if the info method is called
    mock_logger_info = mocker.patch("pipeline.load.logger.info")

    # Arrange: Input data
    df = pd.DataFrame({"produto": ["A"], "valor": [100], "ano": [2024], "mes": [6]})
    s3_base_path = "s3://meu-bucket/dados"
    partition_path = f"{s3_base_path}/ano=2024/mes=6"

    # Act: Call the function save_parquet_partitioned
    pipeline.load.save_parquet_partitioned(df, s3_base_path)

    # Assert:
    # Ensure S3FileSystem was instantiated
    mock_s3_fs_class.assert_called_once()
    # Check if the isdir method was called with the correct partition path
    mock_fs_instance.isdir.assert_called_once_with(partition_path)
    # Check if the delete_dir_contents method was called with the correct partition path
    mock_fs_instance.delete_dir_contents.assert_called_once_with(partition_path)
    # Check if the logger info method was called with the correct message
    mock_logger_info.assert_called_once_with(
        f"Partição existente limpa: {partition_path}"
    )
    # Ensure write_dataset was called
    mock_write_dataset.assert_called_once()

    # Assert: Check if the write_dataset was called with the correct parameters
    kwargs = mock_write_dataset.call_args
    assert kwargs["base_dir"] == s3_base_path
    assert kwargs["partitioning"] == ["ano", "mes"]
    assert isinstance(kwargs["data"], pa.Table)


@pytest.mark.unit
def test_save_partitioned_with_new_partition(mocker):
    """
    Checks if the function does not try to delete a partition that does not exist.
    """
    # Arrange: Mocking the necessary components
    mock_write_dataset = mocker.patch("pipeline.load.ds.write_dataset")
    mock_s3_fs_class = mocker.patch("pipeline.load.S3FileSystem")
    mock_fs_instance = mocker.MagicMock()
    mock_fs_instance.isdir.return_value = (
        False  # Simulates that the partition does not exist
    )
    mock_s3_fs_class.return_value = mock_fs_instance

    # Arrange: Input data
    df = pd.DataFrame({"ano": [2024], "mes": [7]})
    s3_base_path = "s3://meu-bucket/dados"

    # Act: Call the function save_parquet_partitioned
    pipeline.load.save_parquet_partitioned(df, s3_base_path)

    # Assert: Verify the deleting function was not called
    mock_fs_instance.delete_dir_contents.assert_not_called()

    # Assert: Checks if the writing function has been called
    mock_write_dataset.assert_called_once()


@pytest.mark.unit
def test_save_parquet_with_path_invalid(mocker):
    """
    Check if the function ends and logs on an error with an empty Path S3.
    """
    # Arrange: Mocking the logger to check if the error method is called
    mock_logger_error = mocker.patch("pipeline.load.logger.error")

    # Arrange: Input data
    df = pd.DataFrame({"ano": [2024], "mes": [1]})

    # Act: Call the function save_parquet_partitioned with an invalid path
    with pytest.raises(SystemExit) as e:
        pipeline.load.save_parquet_partitioned(df, "   ")

    # Assert: Check if the function raises SystemExit with code 1
    assert e.type == SystemExit
    assert e.value.code == 1

    # Assert: Check if the correct error message was logged in
    mock_logger_error.assert_called_once_with(
        "s3_base_path não encontrado. Certifique-se de que o arquivo .env está configurado corretamente."
    )

# Imports Standard Library
import sys
import logging
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# Imports Third-Party Libraries
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem

# Logging System Configuration: Displays at the terminal and saves in file
logger = logging.getLogger(__name__)


def save_parquet_partitioned(df: pd.DataFrame, s3_base_path: str) -> None:
    """
    Saves Dataframe in Parquet format 'ano' and 'mes' in S3.

    Parameters:
        df (pd.DataFrame): Dataframe Treaty.
        s3_base_path (str): Base path on S3 (ex: 's3://meu-bucket-dados/raw/notas_fiscais_partitioned')
    """
    # Checks if the base path has been defined
    if not s3_base_path or s3_base_path.strip() == "":
        logger.error(
            "s3_base_path não encontrado. Certifique-se de que o arquivo .env está configurado corretamente."
        )
        sys.exit(1)

    # Checks if the DataFrame has the required columns
    fs = S3FileSystem()

    # Exclude existing partitions that are in dataframe (avoids duplicity)
    years_months = df["ano", "mes"].dropna().drop_duplicates()

    # Check if the base path exists, if yes, delete its contents
    for _, row in years_months.iterrows():
        partition_path = f"{s3_base_path}/ano={int(row['ano'])}/mes={int(row['mes'])}"
        if fs.isdir(partition_path):
            fs.delete_dir_contents(partition_path)
            logger.info(f"Partição existente limpa: {partition_path}")

    # Convert DataFrame to PyArrow Table and write to S3
    table = pa.Table.from_pandas(df)
    ds.write_dataset(
        data=table,
        base_dir=s3_base_path,
        format="parquet",
        partitioning=["ano", "mes"],
        filesystem=fs,
    )

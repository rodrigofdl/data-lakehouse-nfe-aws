from __future__ import annotations
import os
import logging
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pandas as pd

# Logging System Configuration: Displays at the terminal and saves in file
logger = logging.getLogger(__name__)


class MissingS3PathError(Exception):
    """Raised when the S3 path is missing or empty."""


class LoadError(Exception):
    """Raised when an error occurs during the data load to S3."""


def save_parquet_partitioned(
    df: pd.DataFrame, s3_base_path: Optional[str] = None
) -> None:
    """
    Saves Dataframe in Parquet format partitioned by 'ano' and 'mes' in S3.

    Parameters:
        df (pd.DataFrame): Dataframe Treaty.
        s3_base_path (str): Base path on S3 (ex: 's3://meu-bucket-dados/raw/notas_fiscais_partitioned')

    Raises:
        MissingS3PathError: If s3_base_path is missing.
        LoadError: If any error occurs during the save process.
    """
    s3_base_path = s3_base_path or os.getenv("S3_BASE_PATH")

    if not s3_base_path or s3_base_path.strip() == "":
        logger.error(
            "s3_base_path não encontrado. Certifique-se de que o arquivo .env está configurado corretamente."
        )
        raise MissingS3PathError("Parâmetro s3_base_path ausente ou vazio.")

    if df.empty:
        logger.warning("DataFrame vazio. Nenhum dado será gravado no S3.")
        return

    try:
        s3_fs = S3FileSystem()

        # Exclude existing partitions that are in dataframe (avoids duplicity)
        years_months = df[["ano", "mes"]].dropna().drop_duplicates()

        # Check if the base path exists, if yes, delete its contents
        for _, row in years_months.iterrows():
            partition_path = (
                f"{s3_base_path}/ano={int(row['ano'])}/mes={int(row['mes'])}"
            )
            if s3_fs.isdir(partition_path):
                s3_fs.delete_dir_contents(partition_path)
                logger.info(f"Partição existente limpa: {partition_path}")

        # Convert DataFrame to PyArrow Table and write to S3
        table = pa.Table.from_pandas(df)

        ds.write_dataset(
            data=table,
            base_dir=s3_base_path,
            format="parquet",
            partitioning=["ano", "mes"],
            filesystem=s3_fs,
        )

        logger.info(f"Data gravada com sucesso no S3 em: {s3_base_path}")

    except Exception as e:
        logger.error(f"Erro ao salvar o DataFrame particionado no S3: {e}")
        raise LoadError(f"Falha ao carregar os dados no S3: {e}")


if __name__ == "__main__":
    # Example of load execution manual
    import pandas as pd

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Mocked DataFrame example
    example_df = pd.DataFrame(
        {
            "id": [1],
            "ano": [2025],
            "mes": [6],
            "valorNotaFiscal": [1234.56],
            "dataEmissao": [pd.to_datetime("2025-06-01")],
            "dataTipoEventoMaisRecente": [pd.to_datetime("2025-06-01 10:00:00")],
            "codigoOrgaoSuperiorDestinatario": ["001"],
            "orgaoSuperiorDestinatario": ["Secretaria"],
            "codigoOrgaoDestinatario": ["002"],
            "orgaoDestinatario": ["Departamento"],
            "nomeFornecedor": ["Fornecedor X"],
            "cnpjFornecedor": ["12345678000100"],
            "municipioFornecedor": ["São Paulo"],
            "chaveNotaFiscal": ["ABC123"],
            "tipoEventoMaisRecente": ["Evento1"],
            "numero": [100],
            "serie": [1],
        }
    )

    try:
        # Example of path S3 fictitious
        save_parquet_partitioned(
            df=example_df, s3_base_path="s3://meu-bucket/raw/notas_fiscais_partitioned"
        )
    except MissingS3PathError as e:
        print(f"Erro de configuração: {e}")
    except LoadError as e:
        print(f"Erro durante o load: {e}")

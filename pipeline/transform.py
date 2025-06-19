import logging
import pandas as pd

# Logging System Configuration: Displays at the terminal and saves in file
logger = logging.getLogger(__name__)


class DataTransformationError(Exception):
    """Custom exception for data transformation errors."""


def prepare_dataframe(all_nfe: list[dict]) -> pd.DataFrame:
    """
    Converts the list of NFE to a dataframe,
    perform types and add columns 'ano' and 'mes'.

    Parameters:
        all_nfe (list[dict]): List of NFE raw.

    Returns:
        pd.DataFrame: DataFrame treated.

    Raises:
        DataTransformationError: If data type conversion fails.
    """
    logger.info("Preparando o DataFrame a partir dos dados das NFEs.")

    if not all_nfe:
        logger.warning("Lista de NFE recebida está vazia. Retornando DataFrame vazio.")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(all_nfe)

        # Correct ValorNotafiscal: Remove the thousands separator, change comma to point, convert to float
        df["valorNotaFiscal"] = (
            df["valorNotaFiscal"]
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

        # Convert date columns
        df["dataEmissao"] = pd.to_datetime(
            df["dataEmissao"], format="%d/%m/%Y", errors="coerce"
        )
        df["dataTipoEventoMaisRecente"] = pd.to_datetime(
            df["dataTipoEventoMaisRecente"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
        )

        # Convert remaining columns to proper types
        df = df.astype(
            {
                "id": "int64",
                "codigoOrgaoSuperiorDestinatario": "string",
                "orgaoSuperiorDestinatario": "string",
                "codigoOrgaoDestinatario": "string",
                "orgaoDestinatario": "string",
                "nomeFornecedor": "string",
                "cnpjFornecedor": "string",
                "municipioFornecedor": "string",
                "chaveNotaFiscal": "string",
                "tipoEventoMaisRecente": "string",
                "numero": "int64",
                "serie": "int64",
            }
        )

        # Extraction of 'ano' and 'mês' columns of the dataEmissao
        df["ano"] = df["dataEmissao"].dt.year
        df["mes"] = df["dataEmissao"].dt.month

        logger.info(
            f"DataFrame com {len(df)} linhas e {len(df.columns)} colunas preparado."
        )

        return df

    except Exception as e:
        logger.error(f"Erro na transformação dos dados: {e}")
        raise DataTransformationError(f"Falha na transformação dos dados: {e}")


if __name__ == "__main__":
    # Example of manual transformation execution
    import json

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # NFe mocked list example
    example_nfe = [
        {
            "id": 1,
            "codigoOrgaoSuperiorDestinatario": "001",
            "orgaoSuperiorDestinatario": "Secretaria",
            "codigoOrgaoDestinatario": "002",
            "orgaoDestinatario": "Departamento",
            "nomeFornecedor": "Fornecedor X",
            "cnpjFornecedor": "12345678000100",
            "municipioFornecedor": "São Paulo",
            "chaveNotaFiscal": "ABC123",
            "tipoEventoMaisRecente": "Evento1",
            "numero": 100,
            "serie": 1,
            "dataEmissao": "01/01/2025",
            "dataTipoEventoMaisRecente": "01/01/2025 10:00:00",
            "valorNotaFiscal": "1.234,56",
        }
    ]

    try:
        df = prepare_dataframe(all_nfe=example_nfe)
        print(df.head())
    except DataTransformationError as e:
        print(f"Erro durante a transformação: {e}")

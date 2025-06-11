# Imports Standard Library
import logging

# Imports Third-Party Libraries
import pandas as pd

# Logging System Configuration: Displays at the terminal and saves in file
logger = logging.getLogger(__name__)


def prepare_dataframe(all_nfe: list[dict]) -> pd.DataFrame:
    """
    Converts the list of NFE to a dataframe,
    perform types and add columns 'ano' and 'mes'.

    Parameters:
        all_nfe (list[dict]): List of NFE raw.

    Returns:
        pd.DataFrame: DataFrame treated.
    """
    logger.info("Preparando o DataFrame a partir dos dados das NFEs.")

    df = pd.DataFrame(all_nfe)

    # Correct ValorNotafiscal: Remove the thousands separator, change comma to point and convert to float
    df["valorNotaFiscal"] = (
        df["valorNotaFiscal"]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    # Convert date columns to datetime
    df["dataEmissao"] = pd.to_datetime(
        df["dataEmissao"], format="%d/%m/%Y", errors="coerce"
    )
    df["dataTipoEventoMaisRecente"] = pd.to_datetime(
        df["dataTipoEventoMaisRecente"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )

    # Converts the remaining types
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
    
    return df

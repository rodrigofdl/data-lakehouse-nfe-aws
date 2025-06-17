# Imports Third-Party Libraries
import pandas as pd
import pytest

# Imports Local Libraries
import pipeline.transform


@pytest.fixture
def mock_nfe_data():
    """
    Pytest fixture to provide mock data for testing the DataFrame preparation.
    """
    return {
        "id": 1,
        "codigoOrgaoSuperiorDestinatario": "001",
        "orgaoSuperiorDestinatario": "Secretaria X",
        "codigoOrgaoDestinatario": "002",
        "orgaoDestinatario": "Departamento Y",
        "nomeFornecedor": "Empresa A",
        "cnpjFornecedor": "12345678000199",
        "municipioFornecedor": "São Paulo",
        "chaveNotaFiscal": "ABC123",
        "tipoEventoMaisRecente": "Emitida",
        "numero": 100,
        "serie": 1,
        "valorNotaFiscal": "1.234,56",
        "dataEmissao": "15/06/2024",
        "dataTipoEventoMaisRecente": "16/06/2024 10:30:00",
    }


def test_prepare_dataframe_valid_data():
    """
    Test the prepare_dataframe function with valid mock data.
    """
    # Arrange: Use the mock data fixture
    input_data = [mock_nfe_data]

    # Act: Call the function to prepare the dataframe
    df = pipeline.transform.prepare_dataframe(input_data)

    # Assert: Check if the dataframe is created correctly
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1
    assert "ano" in df.columns
    assert "mes" in df.columns
    assert df.loc[0, "ano"] == 2024
    assert df.loc[0, "mes"] == 6
    assert df.loc[0, "valorNotaFiscal"] == 1234.56
    assert pd.api.types.is_datetime64_any_dtype(df["dataEmissao"])
    assert df["id"].dtype == "int64"
    assert df["codigoOrgaoSuperiorDestinatario"].dtype.name == "string"


def test_prepare_dataframe_invalid_valorNotaFiscal():
    """
    Test the prepare_dataframe function with an invalid valorNotaFiscal format.
    """
    # Arrange: Use the mock data fixture and modify valorNotaFiscal
    broken_data = mock_nfe_data.copy()
    broken_data["valorNotaFiscal"] = "invalid_value"

    # Act & Assert: Check if ValueError is raised when trying to prepare the dataframe
    with pytest.raises(ValueError):
        pipeline.transform.prepare_dataframe([broken_data])


def test_prepare_dataframe_invalid_dates():
    """
    Test the prepare_dataframe function with invalid date formats.
    """
    # Arrange: Use the mock data fixture and modify date fields
    broken_data = mock_nfe_data.copy()
    broken_data["dataEmissao"] = "99/99/9999"
    broken_data["dataTipoEventoMaisRecente"] = "99/99/9999 99:99:99"

    # Act: Call the function to prepare the dataframe
    df = pipeline.transform.prepare_dataframe([broken_data])

    # Assert: Check if the dates are converted to NaT (Not a Time)
    assert pd.isna(df.loc[0, "dataEmissao"])
    assert pd.isna(df.loc[0, "dataTipoEventoMaisRecente"])
    assert pd.isna(df.loc[0, "ano"])
    assert pd.isna(df.loc[0, "mes"])

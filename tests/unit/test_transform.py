# Imports Third-Party Libraries
import pandas as pd
import pytest

# Imports Local Libraries
import pipeline.transform


@pytest.fixture
def mock_nfe_data():
    """
    Fixture that provides a fictitious record of Nota Fiscal Eletrônica (NFe) for the tests.

    Returns:
        dict: A dictionary representing a NFe record with examples.
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


@pytest.mark.unit
def test_prepare_dataframe_valid_data(mock_nfe_data):
    """
    Tests if the function prepares_dataframe converts and correctly a valid NFe record.

    Tested scenarios:
        - Type conversion (int, string, float, datetime)
        - Column generation 'ano' and 'mes'
        - Correct treatment of numerical fields and dates
    """
    # Arrange
    input_data = [mock_nfe_data]

    # Act
    df = pipeline.transform.prepare_dataframe(input_data)

    # Assert
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


@pytest.mark.unit
def test_prepare_dataframe_invalid_valorNotaFiscal(mock_nfe_data):
    """
    Tests if the function prepare_dataframe spear ValueError when the field valorNotaFiscal has an invalid format.
    """
    # Arrange
    broken_data = mock_nfe_data.copy()
    broken_data["valorNotaFiscal"] = "invalid_value"

    # Act & Assert
    with pytest.raises(ValueError):
        pipeline.transform.prepare_dataframe([broken_data])


@pytest.mark.unit
def test_prepare_dataframe_invalid_dates(mock_nfe_data):
    """
    Tests if the function prepare_dataframe converts invalid dates into NaT (Not a Time).

    Tested scenarios:
        - dataEmissao invalid
        - dataTipoEventoMaisRecente invalid
        - Derivation of 'ano' and 'mes' must result in NaN
    """
    # Arrange
    broken_data = mock_nfe_data.copy()
    broken_data["dataEmissao"] = "99/99/9999"
    broken_data["dataTipoEventoMaisRecente"] = "99/99/9999 99:99:99"

    # Act
    df = pipeline.transform.prepare_dataframe([broken_data])

    # Assert
    assert pd.isna(df.loc[0, "dataEmissao"])
    assert pd.isna(df.loc[0, "dataTipoEventoMaisRecente"])
    assert pd.isna(df.loc[0, "ano"])
    assert pd.isna(df.loc[0, "mes"])

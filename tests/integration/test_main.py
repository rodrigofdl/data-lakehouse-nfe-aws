import pytest
import pandas as pd
from pipeline import main


def test_run_pipeline_happy_path(mocker, caplog):
    """
    Tests the full pipeline flow (extract → transform → load) with mocks on external premises.
    """

    # Arrange
    mocker.patch("logging.FileHandler", autospec=True)

    mock_nfe_data = [
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
            "dataEmissao": "01/01/2024",
            "dataTipoEventoMaisRecente": "01/01/2024 10:00:00",
            "valorNotaFiscal": "1.234,56",
        }
    ]
    mocker.patch("pipeline.extract.get_nfe_data", return_value=mock_nfe_data)

    mock_df = pd.DataFrame(
        {
            "id": [1],
            "ano": [2024],
            "mes": [1],
            "valorNotaFiscal": [1234.56],
            "dataEmissao": [pd.to_datetime("2024-01-01")],
        }
    )
    mocker.patch("pipeline.transform.prepare_dataframe", return_value=mock_df)

    mock_save = mocker.patch("pipeline.load.save_parquet_partitioned")

    # Act
    with caplog.at_level("INFO"):
        main.run_pipeline()

    # Assert
    main.extract.get_nfe_data.assert_called_once_with(
        organ_code="36000", year_emission=2024
    )
    main.transform.prepare_dataframe.assert_called_once_with(all_nfe=mock_nfe_data)
    mock_save.assert_called_once_with(df=mock_df)

    # Assert logs
    log_text = caplog.text
    assert "Iniciando o pipeline..." in log_text
    assert "Pipeline concluído com sucesso." in log_text
    assert "Iniciando a coleta de NFE" in log_text
    assert "Preparando o DataFrame" in log_text

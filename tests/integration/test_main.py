import pandas as pd
import pytest

from pipeline import main
from pipeline.ingestion import MissingAPIConfigError
from pipeline.load import LoadError
from pipeline.transform import DataTransformationError


@pytest.mark.integration
def test_run_pipeline_sucess(mocker, caplog):
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
            "codigoOrgaoDestinatario": "36000",
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
    mock_extract = mocker.patch(
        "pipeline.extract.get_nfe_data", return_value=mock_nfe_data
    )

    mock_df = pd.DataFrame(
        {
            "id": [1],
            "ano": [2024],
            "mes": [1],
            "valorNotaFiscal": [1234.56],
            "dataEmissao": [pd.to_datetime("2024-01-01")],
        }
    )
    mock_transform = mocker.patch(
        "pipeline.transform.prepare_dataframe", return_value=mock_df
    )
    mock_save = mocker.patch("pipeline.load.save_parquet_partitioned")

    # Act
    with caplog.at_level("INFO"):
        main.run_pipeline()

    # Assert
    mock_extract.assert_called_once_with(organ_code="36000", year_emission=2024)
    mock_transform.assert_called_once_with(all_nfe=mock_nfe_data)
    mock_save.assert_called_once_with(df=mock_df)
    assert "Iniciando o pipeline..." in caplog.text
    assert "Pipeline concluído com sucesso." in caplog.text


@pytest.mark.integration
def test_run_pipeline_empty_extract(mocker, caplog):
    """
    Tests pipeline behavior when extract returns empty list.
    """

    # Arrange
    mocker.patch("logging.FileHandler", autospec=True)

    mock_extract = mocker.patch("pipeline.extract.get_nfe_data", return_value=[])

    # Act
    with caplog.at_level("WARNING"):
        main.run_pipeline()

    # Assert
    mock_extract.assert_called_once_with(organ_code="36000", year_emission=2024)
    assert (
        "Nenhuma NFe foi encontrada para o filtro informado. Pipeline encerrado."
        in caplog.text
    )


@pytest.mark.integration
def test_run_pipeline_empty_transform(mocker, caplog):
    """
    Test the case where transform returns empty dataframe.
    """

    # Arrange
    mocker.patch("logging.FileHandler", autospec=True)

    mock_nfe_data = [{"dummy": "data"}]
    mock_extract = mocker.patch(
        "pipeline.extract.get_nfe_data", return_value=mock_nfe_data
    )
    mock_transform = mocker.patch(
        "pipeline.transform.prepare_dataframe", return_value=pd.DataFrame()
    )

    # Act
    with caplog.at_level("WARNING"):
        main.run_pipeline()

    # Assert
    mock_extract.assert_called_once_with(organ_code="36000", year_emission=2024)
    mock_transform.assert_called_once_with(all_nfe=mock_nfe_data)
    assert (
        "DataFrame resultante da transformação está vazio. Pipeline encerrado."
        in caplog.text
    )


@pytest.mark.integration
def test_run_pipeline_missing_api_config_error(mocker, caplog):
    """
    Tests the case where Extract launches MissingapiconFigerror.
    """

    # Arrange
    mocker.patch("logging.FileHandler", autospec=True)
    mocker.patch(
        "pipeline.extract.get_nfe_data",
        side_effect=MissingAPIConfigError("API_URL não encontrada"),
    )

    # Act
    with caplog.at_level("ERROR"):
        main.run_pipeline()

    # Assert
    assert "Erro de configuração da API: API_URL não encontrada" in caplog.text


@pytest.mark.integration
def test_run_pipeline_transformation_error(mocker, caplog):
    """
    Tests the case where transform launches DataTransformationError.
    """

    # Arrange
    mocker.patch("logging.FileHandler", autospec=True)

    mock_nfe_data = [{"dummy": "data"}]
    mock_extract = mocker.patch(
        "pipeline.extract.get_nfe_data", return_value=mock_nfe_data
    )

    mocker.patch(
        "pipeline.transform.prepare_dataframe",
        side_effect=DataTransformationError("Erro durante a transformação"),
    )

    # Act
    with caplog.at_level("ERROR"):
        main.run_pipeline()

    # Assert
    mock_extract.assert_called_once_with(organ_code="36000", year_emission=2024)
    assert "Erro durante a transformação" in caplog.text


@pytest.mark.integration
def test_run_pipeline_load_error(mocker, caplog):
    """
    Test the case where load launches LoadError.
    """

    # Arrange
    mocker.patch("logging.FileHandler", autospec=True)

    mock_nfe_data = [{"dummy": "data"}]
    mock_extract = mocker.patch(
        "pipeline.extract.get_nfe_data", return_value=mock_nfe_data
    )

    mock_df = pd.DataFrame({"dummy_col": [1]})
    mock_transform = mocker.patch(
        "pipeline.transform.prepare_dataframe", return_value=mock_df
    )

    mocker.patch(
        "pipeline.load.save_parquet_partitioned",
        side_effect=LoadError("Erro durante o carregamento para o S3"),
    )

    # Act
    with caplog.at_level("ERROR"):
        main.run_pipeline()

    # Assert
    mock_extract.assert_called_once_with(organ_code="36000", year_emission=2024)
    mock_transform.assert_called_once_with(all_nfe=mock_nfe_data)
    assert "Erro durante o carregamento para o S3" in caplog.text


@pytest.mark.integration
def test_run_pipeline_generic_exception(mocker, caplog):
    """
    Tests the case of unexpected generic exception.
    """

    # Arrange
    mocker.patch("logging.FileHandler", autospec=True)

    mocker.patch(
        "pipeline.extract.get_nfe_data",
        side_effect=Exception("Erro genérico inesperado"),
    )

    # Act
    with caplog.at_level("ERROR"):
        main.run_pipeline()

    # Assert
    assert "Erro inesperado no pipeline: Erro genérico inesperado" in caplog.text

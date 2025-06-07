import pytest
import requests
import src.ingestion_api


@pytest.fixture
def mock_nfe_data():
    """
    Pytest fixture to provide mock data for testing.
    """
    return {
        "nfe_2025": [
            {"dataEmissao": "01/01/2025", "id": 1},
            {"dataEmissao": "15/02/2025", "id": 2},
        ],
        "nfe_2024": [
            {"dataEmissao": "31/12/2024", "id": 3},
        ],
        "mixed_nfe": [
            {"dataEmissao": "01/01/2025", "id": 1},
            {"dataEmissao": "15/02/2025", "id": 2},
            {"dataEmissao": "31/12/2024", "id": 3},
        ],
    }


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    """
    Pytest fixture to set the API key environment variable with a fictional value for testing.
    """
    monkeypatch.setenv("API_KEY", "test_api_key")


@pytest.mark.unit
def test_request_nfe_success(mocker):
    """
    Tests if the `request_nfe' function returns the data correctly when the API responds successfully (status 200).
    """
    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = [{"data": "example"}]

    result = src.ingestion_api.request_nfe(organ_code="36000", page_number=1)

    assert result == [{"data": "example"}]


def test_request_nfe_failure(mocker):
    """
    Tests if the `request_nfe' function tries again and throws exception after request failures.
    """
    mock_get = mocker.patch("requests.get")
    mock_get.side_effect = requests.exceptions.RequestException("API error")

    with pytest.raises(requests.exceptions.RequestException) as exc_info:
        src.ingestion_api.request_nfe(organ_code="36000", page_number=1)

    assert mock_get.call_count == 3
    assert "API error" in str(exc_info.value)


def test_filter_nfe_per_year_sucess(mock_nfe_data):
    """
    Tests if the `filter_nfe_per_year` function correctly filters data by year.
    """
    filtered_data = src.ingestion_api.filter_nfe_per_year(
        api_response=mock_nfe_data["mixed_nfe"], year_emission=2025
    )

    assert len(filtered_data) == 2
    assert filtered_data == mock_nfe_data["nfe_2025"]


def test_filter_nfe_per_year_no_match(mock_nfe_data):
    """
    Tests if the `filter_nfe_per_year` function returns an empty list when no data matches the specified year.
    """
    filtered_data = src.ingestion_api.filter_nfe_per_year(
        api_response=mock_nfe_data["mixed_nfe"], year_emission=2100
    )

    assert filtered_data == []


def test_get_nfe_data_success(mocker, mock_nfe_data):
    """
    Tests the complete flow of API data with multiple pages,
    returning only data with an issuance year equal to the specified year.
    """
    simulated_api_response = [mock_nfe_data["mixed_nfe"], mock_nfe_data["nfe_2024"], []]
    mock_request_nfe = mocker.patch(
        "src.ingestion_api.request_nfe", side_effect=simulated_api_response
    )

    result = src.ingestion_api.get_nfe_data(organ_code="36000", year_emission=2025)

    assert len(result) == 2
    assert result == mock_nfe_data["nfe_2025"]
    assert mock_request_nfe.call_count == 3


def test_get_nfe_data_empty_first_page(mocker):
    """
    Tests if the `get_nfe_data` function returns an empty list when the first page of the API response is empty.
    """
    mock_request_nfe = mocker.patch("src.ingestion_api.request_nfe", return_value=[])

    result = src.ingestion_api.get_nfe_data(organ_code="36000", year_emission=2025)

    assert result == []
    assert mock_request_nfe.call_count == 1


def test_get_nfe_data_reaches_max_pages(mocker, mock_nfe_data):
    """
    Tests if the `get_nfe_data` function stops seeking after reaching the pages limit (max_pages).
    """
    mock_request_nfe = mocker.patch(
        "src.ingestion_api.request_nfe", return_value=mock_nfe_data["nfe_2025"]
    )

    result = src.ingestion_api.get_nfe_data(
        organ_code="36000", year_emission=2025, max_pages=2
    )

    assert len(result) == 4
    assert mock_request_nfe.call_count == 2


def test_get_nfe_data_api_exception(mocker, mock_nfe_data):
    """
    Tests if the `get_nfe_data` function handles API exceptions correctly,
    """
    simulated_api_response = [
        mock_nfe_data["nfe_2025"],
        Exception("API error"),
    ]
    mock_request = mocker.patch(
        "src.ingestion_api.request_nfe", side_effect=simulated_api_response
    )

    result = src.ingestion_api.get_nfe_data(organ_code="36000", year_emission=2025)

    assert len(result) == 2
    assert result == mock_nfe_data["nfe_2025"]
    assert mock_request.call_count == 2

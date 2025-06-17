# Imports Third-Party Libraries
import pytest
import requests
from tenacity import RetryError

# Imports Local Libraries
import pipeline.extract


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


@pytest.mark.unit
def test_request_nfe_success(mocker):
    """
    Tests if the `request_nfe' function returns the data correctly when the API responds successfully (status 200).
    """
    # Arrange: Mock the requests.get method to simulate a successful API response
    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = [{"data": "example"}]

    # Act: Call the function with test parameters
    result = pipeline.extract.request_nfe(
        organ_code="36000", page_number=1, api_key="test_key"
    )

    # Assert: Check if the result is as expected
    assert result == [{"data": "example"}]


@pytest.mark.unit
def test_request_nfe_failure(mocker):
    """
    Tests if the `request_nfe' function tries again and throws exception after request failures.
    """
    # Arrange: Mock the requests.get method to simulate a failed API response
    mock_get = mocker.patch("requests.get")
    mock_get.side_effect = requests.exceptions.RequestException("API error")

    # Act: Call the function with test parameters and expect it to retry
    with pytest.raises(RetryError) as exc_info:
        pipeline.extract.request_nfe(
            organ_code="36000", page_number=1, api_key="test_key"
        )

    # Assert: Check if the exception is raised and the request was retried
    assert isinstance(
        exc_info.value.last_attempt.exception(), requests.exceptions.RequestException
    )
    assert mock_get.call_count == 3


@pytest.mark.unit
def test_filter_nfe_per_year_sucess(mock_nfe_data):
    """
    Tests if the `filter_nfe_per_year` function correctly filters data by year.
    """
    # Arrange & Act: Call the function with the mock data and year 2025
    filtered_data = pipeline.extract.filter_nfe_per_year(
        api_response=mock_nfe_data["mixed_nfe"], year_emission=2025
    )

    # Assert: Check if the filtered data contains only entries from 2025
    assert len(filtered_data) == 2
    assert filtered_data == mock_nfe_data["nfe_2025"]


@pytest.mark.unit
def test_filter_nfe_per_year_no_match(mock_nfe_data):
    """
    Tests if the `filter_nfe_per_year` function returns an empty list when no data matches the specified year.
    """
    # Arrange & Act: Call the function with the mock data and a year that does not match any entry
    filtered_data = pipeline.extract.filter_nfe_per_year(
        api_response=mock_nfe_data["mixed_nfe"], year_emission=2100
    )

    # Assert: Check if the filtered data is empty
    assert filtered_data == []


@pytest.mark.unit
def test_get_nfe_data_success(mocker, mock_nfe_data):
    """
    Tests the complete flow of API data with multiple pages,
    returning only data with an issuance year equal to the specified year.
    """
    # Arrange: Mock the request_nfe function to simulate API responses
    simulated_api_response = [mock_nfe_data["mixed_nfe"], mock_nfe_data["nfe_2024"], []]
    mock_request_nfe = mocker.patch(
        "pipeline.extract.request_nfe", side_effect=simulated_api_response
    )

    # Act: Call the get_nfe_data function with test parameters
    result = pipeline.extract.get_nfe_data(organ_code="36000", year_emission=2025)

    # Assert: Check if the result is as expected
    assert len(result) == 2
    assert result == mock_nfe_data["nfe_2025"]
    assert mock_request_nfe.call_count == 3


@pytest.mark.unit
def test_get_nfe_data_empty_first_page(mocker):
    """
    Tests if the `get_nfe_data` function returns an empty list when the first page of the API response is empty.
    """
    # Arrange: Mock the request_nfe function to return an empty list for the first page
    mock_request_nfe = mocker.patch("pipeline.extract.request_nfe", return_value=[])

    # Act: Call the get_nfe_data function with test parameters
    result = pipeline.extract.get_nfe_data(organ_code="36000", year_emission=2025)

    # Assert: Check if the result is an empty list and the request was made once
    assert result == []
    assert mock_request_nfe.call_count == 1


@pytest.mark.unit
def test_get_nfe_data_reaches_max_pages(mocker, mock_nfe_data):
    """
    Tests if the `get_nfe_data` function stops seeking after reaching the pages limit (max_pages).
    """
    # Arrange: Mock the request_nfe function to return data for two pages
    mock_request_nfe = mocker.patch(
        "pipeline.extract.request_nfe", return_value=mock_nfe_data["nfe_2025"]
    )

    # Act: Call the get_nfe_data function with a limit of 2 pages
    result = pipeline.extract.get_nfe_data(
        organ_code="36000", year_emission=2025, max_pages=2
    )

    # Assert: Check if the result contains the expected number of entries and the request was made twice
    assert len(result) == 4
    assert mock_request_nfe.call_count == 2


@pytest.mark.unit
def test_get_nfe_data_api_exception(mocker, mock_nfe_data):
    """
    Tests if the `get_nfe_data` function handles API exceptions correctly.
    """
    # Arrange: Mock the request_nfe function to simulate an API error on the second call
    simulated_api_response = [
        mock_nfe_data["nfe_2025"],
        Exception("API error"),
    ]
    mock_request = mocker.patch(
        "pipeline.extract.request_nfe", side_effect=simulated_api_response
    )

    # Act: Call the get_nfe_data function with test parameters
    result = pipeline.extract.get_nfe_data(organ_code="36000", year_emission=2025)

    # Assert: Check if the result contains only the first page of data and the request was made twice
    assert len(result) == 2
    assert result == mock_nfe_data["nfe_2025"]
    assert mock_request.call_count == 2

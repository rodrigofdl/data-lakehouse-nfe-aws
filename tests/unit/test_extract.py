# Imports Third-Party Libraries
import pytest
import requests
from tenacity import RetryError

# Imports Local Libraries
import pipeline.extract


@pytest.fixture
def mock_nfe_api_response():
    """
    Fixture that provides different examples of response from API of NFe for the tests.

    Returns:
        dict: Dictionary containing NFe lists separated per year or mixture of years.
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
    # Arrange
    mock_get = mocker.patch("requests.get")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = [{"data": "example"}]

    # Act
    result = pipeline.extract.request_nfe(
        organ_code="36000", page_number=1, api_key="test_key"
    )

    # Assert
    assert result == [{"data": "example"}]


@pytest.mark.unit
def test_request_nfe_failure(mocker):
    """
    Tests if the `request_nfe' function tries again and throws exception after request failures.
    """
    # Arrange
    mock_get = mocker.patch("requests.get")
    mock_get.side_effect = requests.exceptions.RequestException("API error")

    # Act & Assert
    with pytest.raises(RetryError) as exc_info:
        pipeline.extract.request_nfe(
            organ_code="36000", page_number=1, api_key="test_key"
        )

    # Assert
    assert isinstance(
        exc_info.value.last_attempt.exception(), requests.exceptions.RequestException
    )
    assert mock_get.call_count == 3


@pytest.mark.unit
def test_filter_nfe_per_year_sucess(mock_nfe_api_response):
    """
    Tests if the `filter_nfe_per_year` function correctly filters data by year.
    """
    # Arrange & Act
    filtered_data = pipeline.extract.filter_nfe_per_year(
        api_response=mock_nfe_api_response["mixed_nfe"], year_emission=2025
    )

    # Assert
    assert len(filtered_data) == 2
    assert filtered_data == mock_nfe_api_response["nfe_2025"]


@pytest.mark.unit
def test_filter_nfe_per_year_no_match(mock_nfe_api_response):
    """
    Tests if the `filter_nfe_per_year` function returns an empty list when no data matches the specified year.
    """
    # Arrange & Act
    filtered_data = pipeline.extract.filter_nfe_per_year(
        api_response=mock_nfe_api_response["mixed_nfe"], year_emission=2100
    )

    # Assert
    assert filtered_data == []


@pytest.mark.unit
def test_get_nfe_data_success(mocker, mock_nfe_api_response):
    """
    Tests the complete flow of the function `get_nfe_data`, simulating multiple API pages.

    Scenario:
        - The API returns three answers (2 with data, 1 empty indicating the end of the pagination)
        - The end result should contain only the records of the year specified.
    """
    # Arrange
    simulated_api_response = [
        mock_nfe_api_response["mixed_nfe"],
        mock_nfe_api_response["nfe_2024"],
        [],  # Simulates the end of the pagess
    ]
    mock_request_nfe = mocker.patch(
        "pipeline.extract.request_nfe", side_effect=simulated_api_response
    )

    # Act
    result = pipeline.extract.get_nfe_data(organ_code="36000", year_emission=2025)

    # Assert
    assert len(result) == 2
    assert result == mock_nfe_api_response["nfe_2025"]
    assert mock_request_nfe.call_count == 3


@pytest.mark.unit
def test_get_nfe_data_empty_first_page(mocker):
    """
    Tests if the `get_nfe_data` function returns an empty list when the first page of the API response is empty.
    """
    # Arrange
    mock_request_nfe = mocker.patch("pipeline.extract.request_nfe", return_value=[])

    # Act
    result = pipeline.extract.get_nfe_data(organ_code="36000", year_emission=2025)

    # Assert
    assert result == []
    assert mock_request_nfe.call_count == 1


@pytest.mark.unit
def test_get_nfe_data_reaches_max_pages(mocker, mock_nfe_api_response):
    """
    Tests if the `get_nfe_data` function stops seeking after reaching the pages limit (max_pages).
    """
    # Arrange
    mock_request_nfe = mocker.patch(
        "pipeline.extract.request_nfe", return_value=mock_nfe_api_response["nfe_2025"]
    )

    # Act
    result = pipeline.extract.get_nfe_data(
        organ_code="36000", year_emission=2025, max_pages=2
    )

    # Assert
    assert len(result) == 4  # Two pages with two records each
    assert mock_request_nfe.call_count == 2


@pytest.mark.unit
def test_get_nfe_data_api_exception(mocker, mock_nfe_api_response):
    """
    Tests if the function `get_nfe_data` continues collecting the data to the page with API failure
    and interrupts after the exception.

    Scenario:
        - First page returns data
        - Second Page generates exception
        - You must return only data from the first page
    """
    # Arrange
    simulated_api_response = [
        mock_nfe_api_response["nfe_2025"],
        Exception("API error"),
    ]
    mock_request = mocker.patch(
        "pipeline.extract.request_nfe", side_effect=simulated_api_response
    )

    # Act
    result = pipeline.extract.get_nfe_data(organ_code="36000", year_emission=2025)

    # Assert
    assert len(result) == 2
    assert result == mock_nfe_api_response["nfe_2025"]
    assert mock_request.call_count == 2

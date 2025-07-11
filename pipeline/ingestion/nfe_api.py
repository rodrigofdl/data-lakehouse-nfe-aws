from datetime import datetime
from typing import Optional

import requests
from config import API_KEY, API_URL
from logger import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def request_nfe(
    organ_code: str,
    page_number: int,
    api_url: Optional[str] = API_URL,
    api_key: Optional[str] = API_KEY,
) -> list[dict]:
    """
    Request a page of NFe from the API of Portal da Transparência.

    Args:
        organ_code (str): Public agency code.
        page_number (int): Page number to request.
        api_url (str): Base URL of the API.
        api_key (str): API key.

    Returns:
        list[dict]: List of invoice records.

    Raises:
        MissingAPIConfigError: If API_URL or API_KEY is missing.
    """
    headers = {"accept": "*/*", "chave-api-dados": api_key}
    params = {"codigoOrgao": organ_code, "pagina": page_number}

    logger.debug(f"Requisitando página {page_number} do órgão {organ_code}.")
    response = requests.get(api_url, params=params, headers=headers)  # type: ignore
    response.raise_for_status()
    return response.json()


def filter_nfe_per_year(api_response: list[dict], year_emission: int) -> list[dict]:
    """
    Filters NFe by year of emission.

    Args:
        api_response (list[dict]): List of invoice records.
        year_emission (int): Year to filter by.

    Returns:
        list[dict]: Filtered list containing only records from the given year.
    """
    logger.debug(f"Filtrando {len(api_response)} registros para o ano {year_emission}.")
    return [
        nfe
        for nfe in api_response
        if "dataEmissao" in nfe
        and datetime.strptime(nfe["dataEmissao"], "%d/%m/%Y").year == year_emission
    ]

from datetime import datetime
from typing import Optional

import requests
from logger import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from utils import API_KEY, API_URL


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


def get_nfe_data(
    organ_code: str,
    year_emission: int,
    page_number: int = 1,
    max_pages: int = 3000,
) -> list[dict]:
    """
    Collects all NFe records from a public agency filtered by year.

    Args:
        organ_code (str): Public agency code.
        year_emission (int): Year of emission to filter.
        max_pages (int): Maximum number of pages to request.

    Returns:
        list[dict]: List of filtered invoice records.
    """
    data: list[dict] = []

    logger.info(f"Iniciando coleta para órgão {organ_code} no ano {year_emission}.")

    while True:
        if page_number > max_pages:
            logger.warning(
                "Limite de páginas atingido. Pode haver dados faltando.\n"
                "Verifique se é necessário aumentar esse limite ou revisar o filtro."
            )
            break

        response = request_nfe(organ_code, page_number)
        if not response:
            logger.info(f"Página {page_number} vazia. Encerrando coleta.")
            break
        filtered = filter_nfe_per_year(response, year_emission)
        logger.debug(
            f"Página {page_number}: {len(filtered)} registros do ano {year_emission}."  # noqa: E501
        )
        data.extend(filtered)
        page_number += 1

    logger.info(f"Total coletado: {len(data)} registros.")
    return data

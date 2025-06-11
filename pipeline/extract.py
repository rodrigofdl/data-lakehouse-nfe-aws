# Imports Standard Library
import os
import sys
import logging
from datetime import datetime

# Imports Third-Party Libraries
from dotenv import load_dotenv
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Logging System Configuration: Displays at the terminal and saves in file
logger = logging.getLogger(__name__)

# URL of the API of Portal da Transparência
API_URL = "https://api.portaldatransparencia.gov.br/api-de-dados/notas-fiscais"

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def request_nfe(organ_code: str, page_number: int) -> list[dict]:
    """
    Request a page of invoices from the API of Portal da Transparência.

    Parameters:
        organ_code (str): Public agency code;
        page_number (int): Page number to be requested.

    Returns:
        list[dict]: Page Invoice Records List.
    """
    headers = {"accept": "*/*", "chave-api-dados": API_KEY}
    parameters = {"codigoOrgao": organ_code, "pagina": page_number}

    response = requests.get(API_URL, params=parameters, headers=headers)
    response.raise_for_status()  # Launches exception if the answer is error (4xx ou 5xx)
    return response.json()


def filter_nfe_per_year(api_response: list[dict], year_emission: int) -> list[dict]:
    """
    Filters the invoices records that belong to the specified year.

    Parameters:
        api_response (list[dict]): List of invoices received from the API;
        year_emission (int): Desired year for the filter.

    Returns:
        list[dict]: List of invoices issued in the specified year.
    """
    return [
        nfe
        for nfe in api_response
        if "dataEmissao" in nfe
        and datetime.strptime(nfe["dataEmissao"], "%d/%m/%Y").year == year_emission
    ]


def get_nfe_data(
    organ_code: str, year_emission: int, max_pages: int = 1000
) -> list[dict]:
    """
    Holds the collection of all invoices of an agency for a given year.

    Parameters:
        organ_code (str): Public agency code;
        year_emission (int): Year of invoices to be collected;
        max_pages (int): Maximum number of pages to be consulted (default: 1000).

    Returns:
        list[dict]: Consolidated List of Invoices of the Desired Year.
    """
    # Check if Api_Key has been loaded correctly
    if not API_KEY or API_KEY.strip() == "":
        logger.error(
            "API_KEY não encontrada. Certifique-se de que o arquivo .env está configurado corretamente."
        )
        sys.exit(1) # Exit the program with an error code

    all_nfe: list[dict] = []
    page_number = 1
    logger.info(
        f"Iniciando a coleta de NFE para o órgão {organ_code} no ano {year_emission}."
    )

    while True:
        if page_number > max_pages:
            logger.warning(
                "Limite de páginas atingido. Pode haver dados faltando.\n"
                "Verifique se é necessário aumentar esse limite ou revisar o filtro."
            )
            break

        try:
            # Requirement of an API page
            api_response = request_nfe(organ_code=organ_code, page_number=page_number)

            # If the answer is empty, the loop finishes
            if not api_response:
                logger.info(
                    f"Nenhum dado retornado na página {page_number}. Finalizando coleta."
                )
                break

            # Filters only the Invoices of the Desired Year
            filtered_nfe = filter_nfe_per_year(
                api_response=api_response, year_emission=year_emission
            )
            all_nfe.extend(filtered_nfe)

            logger.info(
                f"Página {page_number} - {len(filtered_nfe)} registros de {year_emission} encontrados."
            )

            page_number += 1

        except Exception as e:
            logger.error(f"Erro ao requisitar a página {page_number}: {e}")
            break

    return all_nfe

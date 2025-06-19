import os
import logging
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Logging System Configuration: Displays at the terminal and saves in file
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


class MissingAPIConfigError(Exception):
    """Custom exception for missing API configuration."""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def request_nfe(
    organ_code: str,
    page_number: int,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> list[dict]:
    """
    Request a page of invoices from the API of Portal da Transparência.

    Parameters:
        organ_code (str): Public agency code;
        page_number (int): Page number to be requested.

    Returns:
        list[dict]: Page Invoice Records List.

    Raises:
        MissingAPIConfigError: If API_URL or API_KEY is not found.
    """
    api_url = api_url or os.getenv("API_URL")
    api_key = api_key or os.getenv("API_KEY")

    if not api_url or api_url.strip() == "":
        logger.error(
            "API_URL não encontrada. Certifique-se de que o arquivo .env está configurado corretamente."
        )
        raise MissingAPIConfigError("API_URL ausente.")

    if not api_key or api_key.strip() == "":
        logger.error(
            "API_KEY não encontrada. Certifique-se de que o arquivo .env está configurado corretamente."
        )
        raise MissingAPIConfigError("API_KEY ausente.")

    headers = {"accept": "*/*", "chave-api-dados": api_key}
    parameters = {"codigoOrgao": organ_code, "pagina": page_number}

    response = requests.get(api_url, params=parameters, headers=headers)
    response.raise_for_status()

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
    organ_code: str, year_emission: int, max_pages: int = 400
) -> list[dict]:
    """
    Collects all invoices of an agency for a given year.

    Parameters:
        organ_code (str): Public agency code;
        year_emission (int): Year of invoices to be collected;
        max_pages (int): Maximum number of pages to be consulted (default: 400).

    Returns:
        list[dict]: Consolidated List of Invoices of the desired Year.
    """
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
            api_response = request_nfe(organ_code=organ_code, page_number=page_number)

            if not api_response:
                logger.info(
                    f"Nenhum dado retornado na página {page_number}. Finalizando coleta."
                )
                break

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

    logger.info(
        f"{len(all_nfe)} NFe para o órgão {organ_code} no ano {year_emission} coletadas."
    )

    return all_nfe


if __name__ == "__main__":
    # Example usage of the module
    import json

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        organ_code = "36000"
        year_emission = 2024
        data = get_nfe_data(organ_code=organ_code, year_emission=year_emission)
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except MissingAPIConfigError as config_error:
        print(f"Configuração de API ausente: {config_error}")
    except Exception as general_error:
        print(f"Erro inesperado durante a execução: {general_error}")

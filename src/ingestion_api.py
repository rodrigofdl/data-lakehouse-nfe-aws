import os
import sys
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ingestion.log", mode="w"),
    ],
)
logger = logging.getLogger(__name__)

API_URL = "https://api.portaldatransparencia.gov.br/api-de-dados/notas-fiscais"
load_dotenv()
API_KEY = os.getenv("API_KEY")

if not API_KEY or API_KEY.strip() == "":
    logger.error(
        "API_KEY não encontrada. Certifique-se de que o arquivo .env está configurado corretamente."
    )
    sys.exit(1)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def request_nfe(organ_code: str, page_number: int) -> list[dict]:
    headers = {"accept": "*/*", "chave-api-dados": API_KEY}
    parameters = {"codigoOrgao": organ_code, "pagina": page_number}

    response = requests.get(API_URL, params=parameters, headers=headers)
    response.raise_for_status()
    return response.json()


def filter_nfe_per_year(api_response: list[dict], year_emission: int) -> list[dict]:
    return [
        nfe
        for nfe in api_response
        if "dataEmissao" in nfe
        and datetime.strptime(nfe["dataEmissao"], "%d/%m/%Y").year == year_emission
    ]


def get_nfe_data(
    organ_code: str, year_emission: int, max_pages: int = 1000
) -> list[dict]:
    all_nfe: list[dict] = []
    page_number = 1
    logger.info(
        f"Iniciando a coleta de NFE para o órgão {organ_code} no ano {year_emission}."
    )

    while True:
        if page_number > max_pages:
            logger.warning(
                "Limite de páginas atingido. Pode haver dados faltando."
                f"Verifique se é necessário aumentar esse limite ou revisar o filtro."
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
                f"Página {page_number} - {len(filtered_nfe)} registros de {year_emission} encontrados"
            )

            page_number += 1

        except Exception as e:
            logger.error(f"Erro ao requisitar a página {page_number}: {e}")
            break

    return all_nfe


if __name__ == "__main__":
    organ_code = "36000"
    year_emission = 2025

    nfe_data = get_nfe_data(organ_code=organ_code, year_emission=year_emission)

    if not nfe_data:
        logger.info(
            "Nenhuma nota fiscal foi encontrada ou um erro ocorreu durante a busca."
        )
        sys.exit(0)

    logger.info(
        f"Processamento concluído. {len(nfe_data)} notas fiscais de {year_emission} foram obtidas."
    )

from pipeline.ingestion.nfe_api import filter_nfe_per_year, request_nfe
from logger import logger


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

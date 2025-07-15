from ingestion.logger import logger
from ingestion.nfe_ingestion import run_ingestion


def airflow_run_ingestion(**kwargs):
    """
    Trigger the ingestion pipeline from an Airflow task using predefined parameters.

    Parameters are expected in the task context under 'params'.

    Returns:
        str: S3 path of the saved file, if ingestion succeeds.

    Raises:
        EnvironmentError: When required environment variables are missing or invalid.
        Exception: For any unexpected error during execution.
    """
    params = kwargs.get("params", {})

    organ_code = params.get("organ_code", "36000")
    year_emission = params.get("year_emission", 2024)
    page_number = params.get("page_number", 1)
    max_pages = params.get("max_pages", 1)
    
    try:
        final_s3_key = run_ingestion(
            organ_code,
            year_emission,
            page_number,
            max_pages,
        )
        if not final_s3_key:
            logger.info("Extração concluída, mas nenhum dado foi salvo no S3.")
            return

        logger.info(f"Arquivo salvo em: {final_s3_key}")
        return final_s3_key

    except EnvironmentError as e:
        logger.error(f"Erro de configuração: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        raise

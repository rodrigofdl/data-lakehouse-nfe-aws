from ingestion.logger import logger
from ingestion.nfe_ingestion import run_ingestion


def main():
    """
    Input point for manual execution/local tests.
    """
    try:
        final_s3_key = run_ingestion(
            organ_code="36000",
            year_emission=2024,
            page_number=1,
            max_pages=1,
        )
        if not final_s3_key:
            logger.info("Extração concluída, mas nenhum dado foi salvo no S3.")
            return

        logger.info(f"Arquivo salvo em: {final_s3_key}")

    except EnvironmentError as e:
        logger.error(f"Erro de configuração: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")


if __name__ == "__main__":
    main()

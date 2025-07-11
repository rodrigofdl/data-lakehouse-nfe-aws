from ingestion.nfe_ingestion import run_ingestion
from ingestion.logger import logger


def main():
    """
    Input point for manual execution/local tests.
    """
    try:
        final_s3_key = run_ingestion(
            organ_code="36000",
            year_emission=2024,
            page_number=1
            max_pages=1,
        )
        if not final_s3_key:
            print("Extração concluída, mas nenhum dado foi salvo no S3.")
            return

        print(f"Arquivo salvo em: {final_s3_key}")

    except EnvironmentError as e:
        logger.error(f"Erro de configuração: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")


if __name__ == "__main__":
    main()

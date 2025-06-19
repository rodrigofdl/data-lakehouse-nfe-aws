import os
import logging
from dotenv import load_dotenv

# Imports Local Modules
from pipeline import extract, transform, load
from pipeline.extract import APIConfigurationError
from pipeline.transform import DataTransformationError
from pipeline.load import MissingS3PathError, LoadError

# Logging Configuration: Displays at the terminal and saves in file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline/logs/pipeline.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def run_pipeline():
    try:
        # Input parameters for collection
        organ_code = "36000"
        year_emission = 2024

        logger.info("Iniciando o pipeline...")

        # Extract
        nfe_data = extract.get_nfe_data(
            organ_code=organ_code, year_emission=year_emission
        )

        if not nfe_data:
            logger.warning(
                "Nenhuma NFe foi encontrada para o filtro informado. Encerrando a busca."
            )
            return

        # Transformação
        df = transform.prepare_dataframe(all_nfe=nfe_data)

        if df.empty:
            logger.warning(
                "DataFrame resultante da transformação está vazio. Pipeline encerrado."
            )
            return

        # Save the data to S3 in Parquet format with partitioning
        load.save_parquet_partitioned(df=df)

        logger.info("Pipeline concluído com sucesso.")

    except APIConfigurationError as e:
        logger.error(f"Erro de configuração da API: {e}")

    except DataTransformationError as e:
        logger.error(f"Erro durante a transformação de dados: {e}")

    except MissingS3PathError as e:
        logger.error(f"Erro de configuração do caminho S3: {e}")

    except LoadError as e:
        logger.error(f"Erro durante o carregamento para o S3: {e}")

    except Exception as e:
        logger.exception(f"Erro inesperado no pipeline: {e}")


if __name__ == "__main__":
    run_pipeline()

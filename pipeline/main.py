# Imports Standard Library
import sys
import logging

# Imports Local Modules
import extract
import transform

# Logging System Configuration: Displays at the terminal and saves in file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline/logs/pipeline.log", mode="w", encoding='utf-8'),
    ],
)
logger = logging.getLogger(__name__)


def main():
    # Input parameters for collection
    organ_code = "36000"
    year_emission = 2024

    logger.info("Iniciando o pipeline...")

    # Run the collection
    nfe_data = extract.get_nfe_data(organ_code=organ_code, year_emission=year_emission)

    # Check if the collection returned data
    if not nfe_data:
        logger.warning(
            "Nenhuma nota fiscal foi encontrada ou ocorreu um erro durante a busca."
        )
        sys.exit(0)

    logger.info(f"{len(nfe_data)} notas fiscais de {year_emission} coletadas.")

    # Prepare the DataFrame
    df = transform.prepare_dataframe(nfe_data)
    logger.info(
        f"DataFrame com {len(df)} linhas e {len(df.columns)} colunas preparado."
    )

    # Aqui você pode chamar a função de armazenamento se desejar, ex:
    # save_parquet_partitioned(df, path="data/nfe", partition_cols=["ano", "mes"])

    # logger.info("Pipeline concluído com sucesso.")


if __name__ == "__main__":
    main()

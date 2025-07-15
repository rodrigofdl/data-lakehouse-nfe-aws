from typing import Optional

from logger import logger
from utils import S3_BUCKET_NAME, build_s3_key

from pipeline.ingestion.nfe_api import get_nfe_data
from pipeline.ingestion.nfe_storage import upload_to_s3


def run_ingestion(
    organ_code: str,
    year_emission: int,
    page_number: int = 17000,
    max_pages: int = 3000,
    timestamp: Optional[str] = None,
) -> str:
    """
    Orchestrates the extraction and upload process to S3.

    Args:
        organ_code (str): Agency code.
        year_emission (int): Year to filter invoices.
        max_pages (int): Limit of pages to request.
        timestamp (Optional[str]): Custom timestamp for file naming.

    Returns:
        str: S3 key of uploaded file, or empty string if no data.

    Raises:
        MissingAPIConfigError: If API config is missing.
        S3UploadError: If upload to S3 fails.
    """
    data = get_nfe_data(organ_code, year_emission, page_number, max_pages)

    if not data:
        logger.info("Nenhum dado coletado.")
        return ""

    s3_key = build_s3_key(organ_code, year_emission, timestamp)
    upload_to_s3(data, s3_key)

    logger.info(f"Dados salvos em: s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key

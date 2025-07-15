import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def validate_env_vars() -> None:
    """
    Validates the required environment variables.

    Raises:
        EnvironmentError: If any required environment variable is missing or blank.
    """
    missing_vars = []

    required_vars = {
        "API_URL": API_URL,
        "API_KEY": API_KEY,
        "S3_BUCKET_NAME": S3_BUCKET_NAME,
        "AWS_REGION_NAME": AWS_REGION_NAME,
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    }

    for name, value in required_vars.items():
        if not value or not value.strip():
            missing_vars.append(name)

    if missing_vars:
        raise EnvironmentError(
            f"Variáveis de ambiente ausentes ou inválidas: {', '.join(missing_vars)}"
        )


def build_s3_key(
    organ_code: str, year_emission: int, timestamp: Optional[str] = None
) -> str:
    """
    Constructs an S3 key path for storing invoice data.

    Args:
        organ_code (str): Public agency code.
        year_emission (int): Year of invoice emission.
        timestamp (Optional[str]): Optional timestamp. Uses current time if None.

    Returns:
        str: The constructed S3 key.
    """
    # Ex: raw/nfe/36000/2024/nfe_data_36000_2024_20250703_152250.json
    current_datetime = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"nfe_data_{organ_code}_{year_emission}_{current_datetime}.json"
    return f"raw/nfe/{organ_code}/{year_emission}/{filename}"

import os
from typing import cast

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

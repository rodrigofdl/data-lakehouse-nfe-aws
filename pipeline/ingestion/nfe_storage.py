import json
from typing import Optional

import boto3
from utils import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION_NAME,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET_NAME,
)
from logger import logger


def upload_to_s3(
    data: list[dict],
    key: str,
    bucket_name: Optional[str] = S3_BUCKET_NAME,
    region_name: Optional[str] = AWS_REGION_NAME,
    aws_access_key_id: Optional[str] = AWS_ACCESS_KEY_ID,
    aws_secret_access_key: Optional[str] = AWS_SECRET_ACCESS_KEY,
) -> None:
    """
    Uploads data to an S3 bucket in JSON format.

    Args:
        data (list[dict]): The data to upload.
        key (str): S3 object key.
        bucket_name (Optional[str]): Bucket name.
        region_name (Optional[str]): AWS region.
        aws_access_key_id (Optional[str]): AWS access key ID.
        aws_secret_access_key (Optional[str]): AWS secret access key.

    Raises:
        S3UploadError: If upload fails or credentials are missing.
    """
    s3 = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    json_data = json.dumps(data, ensure_ascii=False, indent=2)
    s3.put_object(Bucket=bucket_name, Key=key, Body=json_data)
    logger.info(f"Upload para S3 concluído: s3://{bucket_name}/{key}")

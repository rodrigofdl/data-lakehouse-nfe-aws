from datetime import datetime
from typing import Optional


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

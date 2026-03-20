from typing import Dict, List

import pandas as pd
from fastapi import HTTPException


def normalize_column_name(value: str) -> str:
    return str(value).strip()


def validate_mapped_columns_exist(
    df: pd.DataFrame,
    field_mapping: Dict[str, str],
    custom_fields: Dict[str, str],
) -> None:
    available_columns = {normalize_column_name(col) for col in df.columns.tolist()}

    for target_field, source_column in field_mapping.items():
        if normalize_column_name(source_column) not in available_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Mapped column '{source_column}' for field '{target_field}' does not exist in file."
            )

    for custom_field, source_column in custom_fields.items():
        if normalize_column_name(source_column) not in available_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Mapped custom column '{source_column}' for field '{custom_field}' does not exist in file."
            )


def check_duplicate_target_fields(field_mapping: Dict[str, str], custom_fields: Dict[str, str]) -> None:
    duplicate_keys = set(field_mapping.keys()) & set(custom_fields.keys())
    if duplicate_keys:
        raise HTTPException(
            status_code=400,
            detail=f"Duplicate target field names found across field_mapping and custom_fields: {sorted(duplicate_keys)}"
        )


def build_mapped_dataframe(
    df: pd.DataFrame,
    field_mapping: Dict[str, str],
    custom_fields: Dict[str, str],
) -> pd.DataFrame:
    """
    Build a standardized dataframe using:
    - standard field mapping
    - custom fields mapping
    """
    check_duplicate_target_fields(field_mapping, custom_fields)
    validate_mapped_columns_exist(df, field_mapping, custom_fields)

    output_data = {}

    # Standard mapped fields
    for target_field, source_column in field_mapping.items():
        output_data[target_field] = df[source_column]

    # Custom mapped fields
    for custom_field, source_column in custom_fields.items():
        output_data[custom_field] = df[source_column]

    output_df = pd.DataFrame(output_data)

    # Ensure all column names are stripped
    output_df.columns = [normalize_column_name(col) for col in output_df.columns.tolist()]
    return output_df


def list_output_columns(field_mapping: Dict[str, str], custom_fields: Dict[str, str]) -> List[str]:
    return list(field_mapping.keys()) + list(custom_fields.keys())
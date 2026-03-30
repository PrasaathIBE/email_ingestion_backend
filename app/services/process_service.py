from typing import Any, Dict, Tuple, Union

import pandas as pd
from fastapi import HTTPException

from app.core.config import PREVIEW_ROW_LIMIT
from app.schemas.file_schemas import ProcessBlobFileRequest, ProcessFileRequest
from app.services.blob_service import read_blob_to_dataframe
from app.services.dedupe_service import deduplicate_by_email
from app.services.email_service import (
    add_normalized_email_column,
    drop_empty_email_rows,
    drop_invalid_email_rows,
    fill_domain_if_missing,
    split_email_rows,
)
from app.services.file_service import get_file_path
from app.services.mapping_service import build_mapped_dataframe
from app.services.parser_service import read_file_to_dataframe


def convert_nan_to_none(records: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    cleaned = []
    for row in records:
        cleaned_row = {}
        for key, value in row.items():
            if pd.isna(value):
                cleaned_row[key] = None
            else:
                cleaned_row[key] = value
        cleaned.append(cleaned_row)
    return cleaned


def build_processed_dataframe_from_df(
    df: pd.DataFrame,
    field_mapping: Dict[str, str],
    custom_fields: Dict[str, str],
    options: Any,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file/sheet contains no rows.")

    total_rows_input = len(df)

    mapped_df = build_mapped_dataframe(
        df=df,
        field_mapping=field_mapping,
        custom_fields=custom_fields,
    )

    if "email" not in mapped_df.columns:
        raise HTTPException(status_code=400, detail="Mapped output does not contain required 'email' column.")

    if options.split_multi_emails:
        mapped_df = split_email_rows(mapped_df, email_column="email")

    rows_after_split = len(mapped_df)

    mapped_df = add_normalized_email_column(mapped_df, email_column="email")

    mapped_df, empty_email_removed = drop_empty_email_rows(mapped_df, email_column="email")

    invalid_email_removed = 0
    if options.drop_invalid_emails:
        mapped_df, invalid_email_removed = drop_invalid_email_rows(mapped_df, email_column="email")

    if options.derive_domain_if_missing:
        mapped_df = fill_domain_if_missing(
            mapped_df,
            email_column="email",
            domain_column="email_domain",
        )

    if options.deduplicate_by != "email":
        raise HTTPException(status_code=400, detail="Currently only deduplicate_by='email' is supported.")

    mapped_df, duplicates_removed = deduplicate_by_email(mapped_df, email_column="email")

    summary = {
        "total_rows_input": total_rows_input,
        "rows_after_split": rows_after_split,
        "empty_email_removed": empty_email_removed,
        "invalid_email_removed": invalid_email_removed,
        "duplicates_removed": duplicates_removed,
        "final_unique_emails": len(mapped_df),
    }

    return mapped_df, summary


def build_processed_dataframe(
    payload: Union[ProcessFileRequest, ProcessBlobFileRequest]
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if isinstance(payload, ProcessFileRequest):
        file_path = get_file_path(payload.file_id)
        df = read_file_to_dataframe(file_path=file_path, sheet_name=payload.sheet_name)
    elif isinstance(payload, ProcessBlobFileRequest):
        df = read_blob_to_dataframe(
            file_url=payload.file_url,
            filename=payload.filename,
            sheet_name=payload.sheet_name,
        )
    else:
        raise HTTPException(status_code=400, detail="Unsupported payload type for processing.")

    return build_processed_dataframe_from_df(
        df=df,
        field_mapping=payload.field_mapping,
        custom_fields=payload.custom_fields,
        options=payload.options,
    )


def process_uploaded_file(payload: Union[ProcessFileRequest, ProcessBlobFileRequest]) -> Dict[str, Any]:
    processed_df, summary = build_processed_dataframe(payload)

    all_records = convert_nan_to_none(
        processed_df.to_dict(orient="records")
    )

    preview_records = all_records[:PREVIEW_ROW_LIMIT]

    return {
        "status": "success",
        "summary": summary,
        "preview": preview_records,
    }


def build_full_processed_payload(payload: Union[ProcessFileRequest, ProcessBlobFileRequest]) -> Dict[str, Any]:
    processed_df, summary = build_processed_dataframe(payload)

    all_records = convert_nan_to_none(
        processed_df.to_dict(orient="records")
    )

    preview_records = all_records[:PREVIEW_ROW_LIMIT]

    return {
        "status": "success",
        "summary": summary,
        "preview": preview_records,
        "data": all_records,
    }
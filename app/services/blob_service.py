import io
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
from fastapi import HTTPException


def download_blob_bytes(file_url: str) -> bytes:
    try:
        response = httpx.get(file_url, timeout=300.0)
        response.raise_for_status()
        return response.content
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to download blob file: {exc}") from exc


def get_file_type_from_filename(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {suffix}")
    return suffix.replace(".", "")


def get_excel_sheets_from_bytes(file_bytes: bytes, filename: str) -> list[str]:
    suffix = Path(filename).suffix.lower()

    if suffix not in {".xlsx", ".xls"}:
        return []

    try:
        content = io.BytesIO(file_bytes)
        excel_file = pd.ExcelFile(content)
        return excel_file.sheet_names
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unable to read Excel sheets from blob: {exc}") from exc


def read_blob_to_dataframe(file_url: str, filename: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    file_bytes = download_blob_bytes(file_url)
    suffix = Path(filename).suffix.lower()
    content = io.BytesIO(file_bytes)

    try:
        if suffix == ".csv":
            return pd.read_csv(content)

        if suffix in {".xlsx", ".xls"}:
            if sheet_name:
                return pd.read_excel(content, sheet_name=sheet_name)
            return pd.read_excel(content)

        raise HTTPException(status_code=400, detail=f"Unsupported blob file type: {suffix}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to parse blob file: {exc}") from exc


def get_blob_columns(file_url: str, filename: str, sheet_name: Optional[str] = None) -> dict:
    df = read_blob_to_dataframe(
        file_url=file_url,
        filename=filename,
        sheet_name=sheet_name,
    )

    columns = [str(col).strip() for col in df.columns.tolist()]

    return {
        "status": "success",
        "filename": filename,
        "sheet_name": sheet_name,
        "columns": columns,
    }


def build_blob_file_metadata(file_url: str, filename: str) -> dict:
    file_bytes = download_blob_bytes(file_url)
    file_type = get_file_type_from_filename(filename)
    sheets = get_excel_sheets_from_bytes(file_bytes, filename)

    return {
        "status": "success",
        "filename": filename,
        "file_type": file_type,
        "sheets": sheets,
        "has_multiple_sheets": len(sheets) > 1,
    }
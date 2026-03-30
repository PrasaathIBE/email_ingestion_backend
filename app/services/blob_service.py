import io
import uuid
from pathlib import Path

import httpx
import pandas as pd
from fastapi import HTTPException

from app.services.parser_service import is_excel_file


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


def build_blob_file_metadata(file_url: str, filename: str) -> dict:
    file_bytes = download_blob_bytes(file_url)
    file_type = get_file_type_from_filename(filename)
    sheets = get_excel_sheets_from_bytes(file_bytes, filename)
    file_id = str(uuid.uuid4())

    return {
        "status": "success",
        "file_id": file_id,
        "filename": filename,
        "file_type": file_type,
        "sheets": sheets,
        "has_multiple_sheets": len(sheets) > 1,
    }
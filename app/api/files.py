from typing import Optional

from fastapi import APIRouter, File, Query, UploadFile

from app.schemas.file_schemas import (
    BlobColumnsRequest,
    BlobColumnsResponse,
    ColumnsResponse,
    FileUploadResponse,
    ForwardProcessedResponse,
    ProcessBlobFileRequest,
    ProcessBlobRequest,
    ProcessBlobResponse,
    ProcessFileRequest,
    ProcessFileResponse,
    SheetsResponse,
)
from app.services.blob_service import build_blob_file_metadata, get_blob_columns
from app.services.file_service import get_file_path, save_uploaded_file
from app.services.forward_service import forward_processed_payload
from app.services.parser_service import get_columns_from_file, get_excel_sheets, is_excel_file
from app.services.process_service import process_uploaded_file

router = APIRouter(prefix="/files", tags=["Files"])


@router.post("/upload", response_model=FileUploadResponse)
def upload_file(file: UploadFile = File(...)):
    saved = save_uploaded_file(file)

    file_path = get_file_path(saved["file_id"])
    sheets = get_excel_sheets(file_path) if is_excel_file(file_path) else []

    return FileUploadResponse(
        file_id=saved["file_id"],
        filename=saved["filename"],
        file_type=saved["file_type"],
        sheets=sheets,
        has_multiple_sheets=len(sheets) > 1,
    )


@router.get("/{file_id}/sheets", response_model=SheetsResponse)
def list_sheets(file_id: str):
    file_path = get_file_path(file_id)
    sheets = get_excel_sheets(file_path) if is_excel_file(file_path) else []

    return SheetsResponse(
        file_id=file_id,
        sheets=sheets,
        has_multiple_sheets=len(sheets) > 1,
    )


@router.get("/{file_id}/columns", response_model=ColumnsResponse)
def list_columns(
    file_id: str,
    sheet_name: Optional[str] = Query(default=None, description="Excel sheet name if needed"),
):
    file_path = get_file_path(file_id)
    columns = get_columns_from_file(file_path=file_path, sheet_name=sheet_name)

    return ColumnsResponse(
        file_id=file_id,
        sheet_name=sheet_name,
        columns=columns,
    )


@router.post("/process", response_model=ProcessFileResponse)
def process_file(payload: ProcessFileRequest):
    result = process_uploaded_file(payload)
    return ProcessFileResponse(**result)


@router.post("/forward-processed", response_model=ForwardProcessedResponse)
def forward_processed_file(payload: ProcessFileRequest):
    result = forward_processed_payload(payload)
    return ForwardProcessedResponse(**result)


@router.post("/process-blob", response_model=ProcessBlobResponse)
def process_blob(payload: ProcessBlobRequest):
    result = build_blob_file_metadata(
        file_url=payload.file_url,
        filename=payload.filename,
    )
    return ProcessBlobResponse(**result)


@router.post("/blob-columns", response_model=BlobColumnsResponse)
def blob_columns(payload: BlobColumnsRequest):
    result = get_blob_columns(
        file_url=payload.file_url,
        filename=payload.filename,
        sheet_name=payload.sheet_name,
    )
    return BlobColumnsResponse(**result)


@router.post("/blob-process", response_model=ProcessFileResponse)
def blob_process(payload: ProcessBlobFileRequest):
    result = process_uploaded_file(payload)
    return ProcessFileResponse(**result)


@router.post("/blob-forward-processed", response_model=ForwardProcessedResponse)
def blob_forward_processed(payload: ProcessBlobFileRequest):
    result = forward_processed_payload(payload)
    return ForwardProcessedResponse(**result)
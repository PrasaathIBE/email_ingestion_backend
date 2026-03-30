from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


class FileUploadResponse(BaseModel):
    status: str = "success"
    file_id: str
    filename: str
    file_type: str
    sheets: List[str] = Field(default_factory=list)
    has_multiple_sheets: bool = False


class SheetsResponse(BaseModel):
    status: str = "success"
    file_id: str
    sheets: List[str] = Field(default_factory=list)
    has_multiple_sheets: bool = False


class ColumnsResponse(BaseModel):
    status: str = "success"
    file_id: str
    sheet_name: Optional[str] = None
    columns: List[str] = Field(default_factory=list)


class ProcessOptions(BaseModel):
    split_multi_emails: bool = True
    deduplicate_by: str = "email"
    derive_domain_if_missing: bool = True
    drop_invalid_emails: bool = False


class ProcessFileRequest(BaseModel):
    file_id: str
    sheet_name: Optional[str] = None
    field_mapping: Dict[str, str] = Field(default_factory=dict)
    custom_fields: Dict[str, str] = Field(default_factory=dict)
    options: ProcessOptions = Field(default_factory=ProcessOptions)

    @model_validator(mode="after")
    def validate_email_mapping(self):
        email_column = self.field_mapping.get("email")
        if not email_column or not str(email_column).strip():
            raise ValueError("field_mapping must include a valid 'email' column mapping.")
        return self


class ProcessSummary(BaseModel):
    total_rows_input: int
    rows_after_split: int
    empty_email_removed: int
    invalid_email_removed: int
    duplicates_removed: int
    final_unique_emails: int


class ProcessFileResponse(BaseModel):
    status: str = "success"
    summary: ProcessSummary
    preview: List[Dict[str, Any]] = Field(default_factory=list)


class ForwardProcessedResponse(BaseModel):
    status: str = "success"
    records_sent: int
    target_status_code: int
    target_response: Any
    message: str


class ErrorResponse(BaseModel):
    status: str = "error"
    message: str

class ProcessBlobRequest(BaseModel):
    file_url: str
    filename: str


class ProcessBlobResponse(BaseModel):
    status: str = "success"
    file_id: str
    filename: str
    file_type: str
    sheets: List[str] = Field(default_factory=list)
    has_multiple_sheets: bool = False
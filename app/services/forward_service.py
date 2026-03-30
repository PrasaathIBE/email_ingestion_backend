import requests
from fastapi import HTTPException

from app.core.config import INGESTION_URL
from app.schemas.file_schemas import ProcessBlobFileRequest, ProcessFileRequest
from app.services.process_service import build_full_processed_payload


def forward_processed_payload(payload: ProcessFileRequest | ProcessBlobFileRequest) -> dict:
    full_payload = build_full_processed_payload(payload)
    records_sent = len(full_payload.get("data", []))

    try:
        response = requests.post(
            INGESTION_URL,
            json=full_payload,
            timeout=300,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect to ingestion URL: {exc}"
        ) from exc

    try:
        target_response = response.json()
    except ValueError:
        target_response = response.text

    return {
        "status": "success",
        "records_sent": records_sent,
        "target_status_code": response.status_code,
        "target_response": target_response,
        "message": "Processed payload forwarded successfully",
    }
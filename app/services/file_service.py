import shutil
import uuid
from pathlib import Path

from fastapi import HTTPException, UploadFile

from app.core.config import ALLOWED_EXTENSIONS, UPLOAD_DIR


def get_file_extension(filename: str) -> str:
    return Path(filename).suffix.lower()


def validate_file_extension(filename: str) -> str:
    extension = get_file_extension(filename)
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{extension}'. Allowed: {sorted(ALLOWED_EXTENSIONS)}"
        )
    return extension


def build_stored_filename(file_id: str, original_filename: str) -> str:
    extension = get_file_extension(original_filename)
    return f"{file_id}{extension}"


def save_uploaded_file(upload_file: UploadFile) -> dict:
    if not upload_file.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename.")

    extension = validate_file_extension(upload_file.filename)
    file_id = str(uuid.uuid4())
    stored_filename = build_stored_filename(file_id, upload_file.filename)
    save_path = UPLOAD_DIR / stored_filename

    try:
        with save_path.open("wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {exc}") from exc
    finally:
        upload_file.file.close()

    return {
        "file_id": file_id,
        "filename": upload_file.filename,
        "file_type": extension.replace(".", ""),
        "stored_filename": stored_filename,
        "file_path": str(save_path),
    }


def get_file_path(file_id: str) -> Path:
    matching_files = list(UPLOAD_DIR.glob(f"{file_id}.*"))
    if not matching_files:
        raise HTTPException(status_code=404, detail="File not found for the given file_id.")
    return matching_files[0]
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = BASE_DIR / "storage"
UPLOAD_DIR = STORAGE_DIR / "uploads"
PROCESSED_DIR = STORAGE_DIR / "processed"

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
PREVIEW_ROW_LIMIT = 20

# Ensure directories exist
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

INGESTION_URL = "https://email-management-database.vercel.app/ingest"
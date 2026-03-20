from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.files import router as files_router


app = FastAPI(
    title="Email Ingestion Backend",
    version="1.0.0",
    description="Backend APIs for upload, sheet selection, column listing, and file processing.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(files_router)


@app.get("/")
def root():
    return {
        "status": "success",
        "message": "Email Ingestion Backend is running"
    }


@app.get("/health")
def health_check():
    return {
        "status": "success",
        "message": "healthy"
    }
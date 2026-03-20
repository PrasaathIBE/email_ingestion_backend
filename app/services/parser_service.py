from pathlib import Path
from typing import List, Optional

import pandas as pd
from fastapi import HTTPException


def is_excel_file(file_path: Path) -> bool:
    return file_path.suffix.lower() in {".xlsx", ".xls"}


def is_csv_file(file_path: Path) -> bool:
    return file_path.suffix.lower() == ".csv"


def get_excel_sheets(file_path: Path) -> List[str]:
    if not is_excel_file(file_path):
        raise HTTPException(status_code=400, detail="Provided file is not an Excel file.")

    try:
        excel_file = pd.ExcelFile(file_path)
        return excel_file.sheet_names
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unable to read Excel sheets: {exc}") from exc


def read_file_to_dataframe(file_path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    try:
        if is_csv_file(file_path):
            return pd.read_csv(file_path)

        if is_excel_file(file_path):
            if sheet_name:
                return pd.read_excel(file_path, sheet_name=sheet_name)
            return pd.read_excel(file_path)

        raise HTTPException(status_code=400, detail="Unsupported file format.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid sheet name or file read option: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read file: {exc}") from exc


def get_columns_from_file(file_path: Path, sheet_name: Optional[str] = None) -> List[str]:
    df = read_file_to_dataframe(file_path=file_path, sheet_name=sheet_name)
    return [str(col).strip() for col in df.columns.tolist()]
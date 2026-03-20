import re
from typing import List

import pandas as pd


EMAIL_REGEX = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


def clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_email(value) -> str:
    return clean_text(value).lower()


def extract_emails_from_text(value) -> List[str]:
    text = clean_text(value)
    if not text:
        return []
    matches = EMAIL_REGEX.findall(text)
    return [normalize_email(match) for match in matches if normalize_email(match)]


def split_email_rows(df: pd.DataFrame, email_column: str = "email") -> pd.DataFrame:
    """
    Split rows where email column may contain multiple emails.
    Creates one row per extracted email.
    """
    records = []

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        extracted_emails = extract_emails_from_text(row_dict.get(email_column, ""))

        if not extracted_emails:
            row_copy = dict(row_dict)
            row_copy[email_column] = ""
            records.append(row_copy)
            continue

        for email in extracted_emails:
            row_copy = dict(row_dict)
            row_copy[email_column] = email
            records.append(row_copy)

    return pd.DataFrame(records)


def add_normalized_email_column(df: pd.DataFrame, email_column: str = "email") -> pd.DataFrame:
    df = df.copy()
    df[email_column] = df[email_column].apply(normalize_email)
    return df


def derive_domain_from_email(email: str) -> str:
    email = normalize_email(email)
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].strip()


def fill_domain_if_missing(
    df: pd.DataFrame,
    email_column: str = "email",
    domain_column: str = "domain",
) -> pd.DataFrame:
    df = df.copy()

    if domain_column not in df.columns:
        df[domain_column] = ""

    df[domain_column] = df[domain_column].fillna("").astype(str).str.strip()

    missing_mask = df[domain_column] == ""
    df.loc[missing_mask, domain_column] = df.loc[missing_mask, email_column].apply(derive_domain_from_email)

    return df


def is_valid_email(email: str) -> bool:
    email = normalize_email(email)
    if not email:
        return False
    return bool(EMAIL_REGEX.fullmatch(email))


def drop_empty_email_rows(df: pd.DataFrame, email_column: str = "email") -> tuple[pd.DataFrame, int]:
    df = df.copy()
    before = len(df)
    df[email_column] = df[email_column].apply(normalize_email)
    df = df[df[email_column] != ""].copy()
    removed = before - len(df)
    return df, removed


def drop_invalid_email_rows(df: pd.DataFrame, email_column: str = "email") -> tuple[pd.DataFrame, int]:
    df = df.copy()
    before = len(df)
    df = df[df[email_column].apply(is_valid_email)].copy()
    removed = before - len(df)
    return df, removed
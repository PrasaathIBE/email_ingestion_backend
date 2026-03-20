import pandas as pd


def deduplicate_by_email(df: pd.DataFrame, email_column: str = "email") -> tuple[pd.DataFrame, int]:
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=[email_column], keep="first").copy()
    removed = before - len(df)
    return df, removed
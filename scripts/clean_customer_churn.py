# ==========================================
# File: clean_customer_churn.py
# Purpose: Clean & preprocess Customer Churn data (in memory, no file output)
# ==========================================

import pandas as pd

RAW_EXCEL_PATH = "/opt/airflow/data/raw/Customer_Churn.xlsx"


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load data from Excel file
    """
    print("[CLEAN] Dang load du lieu tu Excel...")
    df = pd.read_excel(file_path)
    print(f"[CLEAN] Da load xong: {df.shape[0]} dong, {df.shape[1]} cot")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and preprocess Customer Churn dataset
    """
    print("[CLEAN] Dang clean du lieu...")

    # 1. Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # 2. Remove duplicate customers
    if "customerid" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset="customerid")
        print(f"[CLEAN] Da loai {before - len(df)} khach hang trung lap")

    # 3. Handle TotalCharges (often stored as text)
    if "totalcharges" in df.columns:
        df["totalcharges"] = pd.to_numeric(df["totalcharges"], errors="coerce")

    # 4. Handle missing values
    print("[CLEAN] Dang xu ly gia tri thieu...")

    # Numeric columns → fill with median
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    # Categorical columns → fill with mode
    categorical_cols = df.select_dtypes(include=["object"]).columns
    for col in categorical_cols:
        df[col] = df[col].fillna(df[col].mode()[0])

    # 5. Normalize text values (Yes/No → lowercase)
    yes_no_cols = [
        col for col in df.columns
        if df[col].dtype == "object" and df[col].nunique() <= 3
    ]

    for col in yes_no_cols:
        df[col] = df[col].str.strip().str.lower()

    # 6. Validate business logic
    if "tenure" in df.columns:
        df = df[df["tenure"] >= 0]

    if "monthlycharges" in df.columns:
        df = df[df["monthlycharges"] >= 0]

    print("[CLEAN] Hoan thanh clean du lieu")
    return df


def get_cleaned_data(file_path: str = RAW_EXCEL_PATH) -> pd.DataFrame:
    """
    Load Excel, clean, and return DataFrame (no file output).
    Dùng bởi DAG: task clean trả về df này qua XCom cho task load.
    """
    df_raw = load_data(file_path)
    return clean_data(df_raw)


if __name__ == "__main__":
    get_cleaned_data()

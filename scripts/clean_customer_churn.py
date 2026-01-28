# ==========================================
# File: clean_customer_churn.py
# Purpose: Clean & preprocess Customer Churn data
# Author: Light ✨
# ==========================================

import pandas as pd
import os


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load data from Excel file
    """
    print("📥 Loading raw data from Excel...")
    df = pd.read_excel(file_path)
    print(f"✅ Raw data loaded: {df.shape[0]} rows, {df.shape[1]} columns")
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and preprocess Customer Churn dataset
    """
    print("🧹 Start cleaning data...")

    # 1. Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # 2. Remove duplicate customers
    if "customerid" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset="customerid")
        print(f"🔁 Removed {before - len(df)} duplicate customers")

    # 3. Handle TotalCharges (often stored as text) - Đồng bộ numeric vì phần chính là số tiền
    if "totalcharges" in df.columns:
        df["totalcharges"] = pd.to_numeric(df["totalcharges"], errors="coerce") 

    # 4. Handle missing values 
    print("🩹 Handling missing values...")

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

    print("✨ Data cleaning completed")
    return df


def save_clean_data(df: pd.DataFrame, output_path: str):
    """
    Save cleaned data to CSV
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"💾 Clean data saved to {output_path}")


def main():
    input_file = "/opt/airflow/data/raw/Customer_Churn.xlsx"
    output_file = "/opt/airflow/data/processed/customer_churn_clean.csv"

    df_raw = load_data(input_file)
    df_clean = clean_data(df_raw)
    save_clean_data(df_clean, output_file)


if __name__ == "__main__":
    main()

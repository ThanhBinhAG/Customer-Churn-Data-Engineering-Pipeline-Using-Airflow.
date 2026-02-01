import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =========================
# Load environment variables
# =========================
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# =========================
# Table config
# =========================
TABLE_NAME = "customer_churn"


def get_engine_with_db_creation():
    """
    Đảm bảo database đích tồn tại.
    - Kết nối tới DB hệ thống 'postgres'
    - Nếu chưa có DB_NAME thì tạo
    - Sau đó trả về engine trỏ vào DB_NAME
    """
    base_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}"

    # 1. Kết nối vào DB hệ thống 'postgres' để tạo DB nếu chưa có
    admin_engine = create_engine(f"{base_url}/postgres", isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        print(f"[LOAD] Kiem tra database '{DB_NAME}' co ton tai...")
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_NAME},
        ).scalar()

        if not exists:
            print(f"[LOAD] Tao database '{DB_NAME}'...")
            conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
        else:
            print(f"[LOAD] Database '{DB_NAME}' da ton tai.")

    admin_engine.dispose()

    # 2. Trả về engine trỏ thẳng tới DB_NAME
    return create_engine(f"{base_url}/{DB_NAME}")


def load_to_postgres_from_dataframe(df: pd.DataFrame) -> None:
    """
    Nhan DataFrame da clean tu XCom, ket noi Postgres va load vao bang customer_churn.
    Khong doc tu file CSV.
    """
    print("[LOAD] Nhan du lieu da clean tu buoc truoc...")
    print("[LOAD] Ket noi PostgreSQL va kiem tra database...")
    engine = get_engine_with_db_creation()
    print("[LOAD] Dang load du lieu len PostgreSQL...")
    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
    print("[LOAD] Load du lieu thanh cong.")


if __name__ == "__main__":
    # Khi chay doc lap: can du lieu tu XCom hoac test, o day chi in huong dan
    print("[LOAD] Chay tu DAG: du lieu duoc truyen qua XCom tu task clean.")
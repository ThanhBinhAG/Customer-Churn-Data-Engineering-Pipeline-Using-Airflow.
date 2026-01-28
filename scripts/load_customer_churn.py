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
# File & table config
# =========================
INPUT_FILE = "/opt/airflow/data/processed/customer_churn_clean.csv"
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
        print(f"🔎 Checking if database '{DB_NAME}' exists...")
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_NAME},
        ).scalar()

        if not exists:
            print(f"🆕 Creating database '{DB_NAME}'...")
            # Dùng quoted identifier để giữ đúng tên (kể cả có chữ hoa)
            conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
        else:
            print(f"✅ Database '{DB_NAME}' already exists.")

    admin_engine.dispose()

    # 2. Trả về engine trỏ thẳng tới DB_NAME
    return create_engine(f"{base_url}/{DB_NAME}")


def load_to_postgres():
    print("📥 Reading cleaned data...")
    df = pd.read_csv(INPUT_FILE)

    print("🔌 Ensuring target database exists & connecting to PostgreSQL...")
    engine = get_engine_with_db_creation()

    print("🛢️ Loading data to PostgreSQL...")
    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

    print("✅ Data loaded successfully!")


if __name__ == "__main__":
    load_to_postgres()
    print("💾 Clean data loaded to PostgreSQL!")
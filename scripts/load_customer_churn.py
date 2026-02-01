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
# Cột dùng để gộp dữ liệu khi chạy nhiều lần (trùng customerid -> giữ bản ghi mới nhất)
MERGE_KEY = "customerid"


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


def _table_exists(engine) -> bool:
    """Kiểm tra bảng customer_churn đã tồn tại chưa."""
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :name"
            ),
            {"name": TABLE_NAME},
        ).scalar() is not None


def load_to_postgres_from_dataframe(df: pd.DataFrame) -> None:
    """
    Nhận DataFrame đã clean từ XCom, kết nối Postgres và load vào bảng customer_churn.
    - Lần đầu (bảng chưa có): tạo bảng và insert toàn bộ.
    - Lần sau (bảng đã có): gộp dữ liệu mới với dữ liệu cũ theo MERGE_KEY (customerid),
      trùng key thì giữ bản ghi mới nhất (incremental / merge).
    """
    print("[LOAD] Nhan du lieu da clean tu buoc truoc...")
    print("[LOAD] Ket noi PostgreSQL va kiem tra database...")
    engine = get_engine_with_db_creation()

    if not _table_exists(engine):
        print("[LOAD] Bang chua ton tai. Tao bang va load du lieu lan dau...")
        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
        print(f"[LOAD] Load du lieu thanh cong. So dong: {len(df)}")
        return

    print("[LOAD] Bang da ton tai. Gop du lieu moi voi du lieu cu (giu ban ghi moi nhat theo " + MERGE_KEY + ")...")
    df_existing = pd.read_sql_table(TABLE_NAME, engine, schema="public")
    df_combined = pd.concat([df_existing, df], ignore_index=True)

    if MERGE_KEY not in df_combined.columns:
        # Fallback: không có cột key thì replace toàn bộ
        print("[LOAD] Khong tim thay cot '" + MERGE_KEY + "', thay the toan bo bang du lieu moi.")
        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
        print(f"[LOAD] Load du lieu thanh cong. So dong: {len(df)}")
        return

    df_combined = df_combined.drop_duplicates(subset=[MERGE_KEY], keep="last")
    df_combined.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
    print(f"[LOAD] Gop du lieu thanh cong. Tong so dong sau gop: {len(df_combined)} (them moi/ cap nhat tu lan chay truoc).")


if __name__ == "__main__":
    # Khi chay doc lap: can du lieu tu XCom hoac test, o day chi in huong dan
    print("[LOAD] Chay tu DAG: du lieu duoc truyen qua XCom tu task clean.")
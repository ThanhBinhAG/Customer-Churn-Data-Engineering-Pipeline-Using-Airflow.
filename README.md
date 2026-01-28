## Customer Churn ETL Pipeline (Airflow + Docker)

Pipeline này dùng Apache Airflow để:
- **Clean dữ liệu churn khách hàng** từ file Excel.
- **Load dữ liệu sạch vào PostgreSQL**.
- Tất cả chạy trong **Docker** và có thể theo dõi / quản lý qua **Airflow Web UI**.

---

## 1. Kiến trúc & luồng dữ liệu

- **File DAG chính**: `dags/customer_churn_dag.py`
  - DAG id: `customer_churn_pipeline`
  - Gồm 2 task:
    - `clean_customer_churn_data` → chạy script `scripts/clean_customer_churn.py`
    - `load_customer_churn_to_postgres` → chạy script `scripts/load_customer_churn.py`
  - Thứ tự: `clean_customer_churn_data >> load_customer_churn_to_postgres`

- **Script clean**: `scripts/clean_customer_churn.py`
  - Đọc file Excel raw: `/opt/airflow/data/raw/Customer_Churn.xlsx`
  - Chuẩn hoá cột, xử lý missing values, convert kiểu dữ liệu…
  - Ghi ra file CSV sạch: `/opt/airflow/data/processed/customer_churn_clean.csv`

- **Script load**: `scripts/load_customer_churn.py`
  - Đọc file CSV sạch: `/opt/airflow/data/processed/customer_churn_clean.csv`
  - Kết nối PostgreSQL (thông qua biến môi trường `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME`)
  - Load vào bảng `customer_churn` (replace toàn bộ dữ liệu mỗi lần chạy).

- **Docker / Airflow**:
  - `Dockerfile`: tạo image Airflow custom, cài thêm dependencies từ `requirements.txt`.
  - `docker-compose.yaml`: chạy các service:
    - `postgres_airflow`: Postgres nội bộ cho metadata của Airflow.
    - `airflow-webserver`: giao diện web.
    - `airflow-scheduler`: scheduler.
    - `airflow-init`: init DB & user admin lần đầu.

---

## 2. Cấu trúc thư mục chính

- **`dags/`**: chứa DAG Airflow
  - `customer_churn_dag.py`
- **`scripts/`**:
  - `clean_customer_churn.py` — xử lý/clean dữ liệu.
  - `load_customer_churn.py` — load dữ liệu vào PostgreSQL.
- **`data/`**:
  - `raw/Customer_Churn.xlsx` — dữ liệu gốc (input).
  - `processed/customer_churn_clean.csv` — dữ liệu đã clean (output, được tạo trong container).
- **`logs/`**:
  - Log runtime của Airflow (có thể xoá khi cần dọn dẹp, không ảnh hưởng code).
- **`.env` / `.env.example`**:
  - Cấu hình biến môi trường, đặc biệt là thông tin kết nối DB đích để load dữ liệu.
- **`requirements.txt`**:
  - Thư viện Python để clean & load dữ liệu (pandas, sqlalchemy, psycopg2,…).

> **Lưu ý**: Các file trong `logs/` và `__pycache__/` chỉ là file sinh ra khi chạy, có thể xoá an toàn nếu muốn dọn dẹp repo.

---

## 3. Chuẩn bị môi trường

- **Yêu cầu**:
  - Đã cài `Docker` và `docker-compose`.
  - Cổng `8080` trên máy local đang trống (Airflow Web UI sẽ chạy trên đó).
  - Một instance PostgreSQL đang chạy trên máy bạn (localhost:5432), ví dụ bạn quản lý bằng pgAdmin4.

- **Thiết lập biến môi trường (mặc định dùng Postgres local/pgAdmin4)**:
  1. Tạo file `.env` từ mẫu:
     ```bash
     cp .env.example .env
     ```
  2. `.env.example` được cấu hình để container kết nối tới Postgres local của bạn:
     ```bash
     DB_HOST=host.docker.internal  # container truy cập Postgres local qua host.docker.internal
     DB_PORT=5432
     DB_NAME=CustomerChurn         # database sẽ được script tự tạo nếu chưa có
     DB_USER=postgres              # thay bằng user bạn dùng trong pgAdmin4 nếu khác
     DB_PASSWORD=your_password_here
     ```
  3. Khi DAG chạy:
     - Script `load_customer_churn.py` sẽ:
       - Kết nối tới DB hệ thống `postgres`.
       - Nếu chưa có database `CustomerChurn` thì **tự tạo**.
       - Sau đó tạo/replace bảng `customer_churn` bên trong DB `CustomerChurn`.

- **(Tuỳ chọn) Dùng Postgres nội bộ trong docker-compose**:
  - Nếu bạn muốn tất cả “đóng” trong stack Docker, có thể chuyển `.env` sang:
    ```bash
    DB_HOST=postgres_airflow
    DB_PORT=5432
    DB_NAME=airflow
    DB_USER=airflow
    DB_PASSWORD=airflow
    ```
  - Khi đó:
    - Service `postgres_airflow` sẽ tạo sẵn DB `airflow`.
    - Script sẽ tạo/replace bảng `customer_churn` trong DB `airflow`.

- **Chuẩn bị dữ liệu input**:
  - Đảm bảo file Excel dữ liệu tồn tại ở:
    - `data/raw/Customer_Churn.xlsx` (trên máy host).
  - Thư mục `data/` được mount vào container tại `/opt/airflow/data`, nên script có thể đọc được file này.

---

## 4. Cách chạy pipeline với Docker + Airflow

### 4.1. Build và khởi động stack

Từ thư mục gốc project:

```bash
docker-compose up --build
```

- Lần đầu chạy có thể hơi lâu (build image, init DB).
- Sau khi mọi thứ chạy ổn, Airflow Web UI sẽ ở `http://localhost:8080`.


### 4.2. Đăng nhập Airflow

- Truy cập: `http://localhost:8080`
- User mặc định (từ `docker-compose.yaml`):
  - **Username**: `admin`
  - **Password**: `admin`


### 4.3. Bật DAG và trigger chạy

1. Trong Airflow UI, tìm DAG có id: **`customer_churn_pipeline`**.
2. Gạt ON để **unpause** DAG nếu nó đang bị pause.
3. Nhấn nút **Trigger DAG** (play) để chạy thủ công.
4. Vào tab **Graph** hoặc **Grid** để xem hai task:
   - `clean_customer_churn_data`
   - `load_customer_churn_to_postgres`
5. Nhấp vào từng task → **Logs** để xem chi tiết log (clean data, kết nối Postgres, số hàng insert,…).


### 4.4. Kiểm tra dữ liệu trong PostgreSQL

Sau khi DAG chạy thành công:

- Bảng **`customer_churn`** sẽ được tạo/cập nhật trong database bạn chỉ định.
- Bạn có thể dùng bất cứ client nào (DBeaver, psql, TablePlus, ...) để kết nối tới DB đó và chạy:

```sql
SELECT * FROM customer_churn LIMIT 10;
```

---

## 5. Ghi chú phát triển & tuỳ chỉnh

- **Thay đổi lịch chạy DAG**:
  - Trong `dags/customer_churn_dag.py`, chỉnh tham số `schedule` (hiện tại là `@daily`).
- **Thay đổi logic clean**:
  - Update trong `scripts/clean_customer_churn.py` (chuẩn hoá cột, xử lý missing value, filter thêm điều kiện business,...).
- **Thay đổi schema hoặc table load**:
  - Chỉnh `TABLE_NAME` hoặc logic trong `scripts/load_customer_churn.py`.

- **Dọn dẹp**:
  - Có thể xoá nội dung thư mục `logs/` nếu chỉ cần repo sạch, vì log sẽ được sinh lại khi chạy.
  - File `__pycache__` và `.pyc` không cần thiết trong repo, có thể xoá an toàn.

---

## 6. Tắt & xoá container

- Dừng stack (giữ container & volume):
  ```bash
  docker-compose down
  ```

- Dừng và xoá luôn volume (xoá DB metadata của Airflow, nên cẩn thận):
  ```bash
  docker-compose down -v
  ```

Sau khi chạy lại `docker-compose up --build`, Airflow sẽ init lại từ đầu (tạo user admin, DB metadata mới).

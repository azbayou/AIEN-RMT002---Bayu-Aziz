# GUIDELINE LENGKAP: END-TO-END DATA PIPELINE MILESTONE 1
## Omnichannel Retail NL-to-SQL Analytics

Dokumen ini adalah panduan langkah demi langkah (*step-by-step guideline*) untuk membangun arsitektur pipeline data dari **Raw Data Lake** menuju **Bronze**, **Silver**, **Gold**, hingga terhubung ke **NL-to-SQL Engine** dan **Streamlit UI**, dengan orkestrasi penuh menggunakan **Apache Airflow**.

---

## DAFTAR ISI
1. [Arsitektur & Alur Pipeline](#1-arsitektur--alur-pipeline)
2. [Langkah 1: Setup Lingkungan (UV, Docker Compose, & .env)](#langkah-1-setup-lingkungan-uv-docker-compose--env)
3. [Langkah 2: Menjalankan PostgreSQL & Apache Airflow](#langkah-2-menjalankan-postgresql--apache-airflow)
4. [Langkah 3: Verifikasi Raw Data Lake](#langkah-3-verifikasi-raw-data-lake)
5. [Langkah 4: Analisis Kontrak Gold Sebelum Mendesain Silver](#langkah-4-analisis-kontrak-gold-sebelum-mendesain-silver)
6. [Langkah 5: Desain Schema Ops (Observabilitas Pipeline)](#langkah-5-desain-schema-ops-observabilitas-pipeline)
7. [Langkah 6: Membangun Bronze Layer (Source Preservation)](#langkah-6-membangun-bronze-layer-source-preservation)
8. [Langkah 7: Membangun Silver Layer (Clean & Reject)](#langkah-7-membangun-silver-layer-clean--reject)
9. [Langkah 8: Membangun Quality Gate Pre-Gold](#langkah-8-membangun-quality-gate-pre-gold)
10. [Langkah 9: Membangun Gold Layer (Analytical Modeling)](#langkah-9-membangun-gold-layer-analytical-modeling)
11. [Langkah 10: Menyusun Airflow DAG (`dags/dag.py`)](#langkah-10-menyusun-airflow-dag-dagsdagpy)
12. [Langkah 11: Menghubungkan Engine ke Gold Lokal & Menjalankan UI](#langkah-11-menghubungkan-engine-ke-gold-lokal--menjalankan-ui)
13. [Langkah 12: Checklist Verifikasi & Berkas Pengumpulan](#langkah-12-checklist-verifikasi--berkas-pengumpulan)

---

## 1. Arsitektur & Alur Pipeline

Pipeline ini didesain menggunakan pola **Medallion Architecture** dengan tambahan lapisan **Observabilitas (Ops)**:

```
[ Raw Files (JSON / CSV) ]
            │
            ▼
[ Bronze Layer (PostgreSQL) ] ──────► Catat file & checksum ke ops.ingested_files
            │
            ▼
[ Silver Layer (Clean vs Reject) ] ──► Catat rejected count ke ops.pipeline_runs
            │
            ▼
[ Quality Gate (Airflow Task) ] ────► Catat hasil uji ke ops.quality_checks (Stop jika gagal!)
            │
            ▼
[ Gold Layer (Analytical Fact/Agg) ]
            │
            ▼
[ NL-to-SQL Engine (FastAPI) ] ◄──── Membaca hanya tabel gold.*
            │
            ▼
[ Analytics UI (Streamlit) ]
```

---

## Langkah 1: Setup Lingkungan (UV, Docker Compose, & .env)

### 1.1. Inisialisasi Virtual Environment dengan UV
Buka terminal PowerShell pada direktori utama project (`milestone1-omnichannel_retail`):

```powershell
# Pastikan uv sudah terinstall di komputer Anda
uv sync
```
*Catatan:* `uv sync` akan membaca `pyproject.toml` dan menginstall seluruh package Python yang dibutuhkan (`fastapi`, `uvicorn`, `streamlit`, `pg8000`, `python-dotenv`, `plotly`, dll) ke folder `.venv/`.

### 1.2. Konfigurasi File `.env`
Buka file `.env` di root project. Pastikan koneksi diarahkan ke **database PostgreSQL lokal** (bukan Neon):

```env
ANALYTICS_DB_TARGET=local

# Koneksi eksplisit untuk driver engine dan pipeline lokal
LOCAL_DATABASE_URL=postgresql://retail:retail@localhost:5432/retail_analytics
LOCAL_POSTGRES_HOST=localhost
LOCAL_POSTGRES_PORT=5432
LOCAL_POSTGRES_DB=retail_analytics
LOCAL_POSTGRES_USER=retail
LOCAL_POSTGRES_PASSWORD=retail
LOCAL_POSTGRES_SSLMODE=disable

# Provider NL-to-SQL: gunakan "mock" untuk pengujian offline tanpa API key
NL2SQL_PROVIDER=mock

# Jika Anda memiliki OpenAI API Key dan ingin hasil query cerdas:
# NL2SQL_PROVIDER=openai
# OPENAI_API_KEY=sk-...
# OPENAI_BASE_URL=https://api.openai.com/v1
# OPENAI_MODEL=gpt-4.1-mini
NL2SQL_MAX_ROWS=100
```

---

## Langkah 2: Menjalankan PostgreSQL & Apache Airflow

Semua service orkestrasi dan basis data dijalankan menggunakan **Docker Compose**.

### 2.1. Jalankan Service Database dan Inisialisasi Airflow
Buka terminal PowerShell:

```powershell
# 1. Jalankan PostgreSQL dan inisialisasi schema/user Airflow
docker compose up -d postgres airflow-init
```

Tunggu hingga container `airflow-init` selesai dengan status `Exited (0)`. Anda bisa mengecek log-nya:
```powershell
docker compose logs airflow-init
```

### 2.2. Jalankan Airflow Webserver & Scheduler
Setelah inisialisasi selesai, jalankan scheduler dan webserver:

```powershell
# 2. Jalankan webserver dan scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

### 2.3. Verifikasi Status Service
Periksa container yang sedang berjalan:
```powershell
docker compose ps
```

Port yang terbuka:
- **PostgreSQL**: `localhost:5432` (User: `retail`, Password: `retail`, DB: `retail_analytics`)
- **Airflow Web UI**: `http://localhost:8080` (Username default: `admin`, Password: `admin`)

---

## Langkah 3: Verifikasi Raw Data Lake

Data mentah berada di folder `data/`. Pastikan struktur direktori berikut sudah ada:

```text
data/
├── events/
│   ├── payment_events.json
│   ├── refund_events.json
│   ├── return_events.json
│   ├── support_events.json
│   └── web_events.json
├── inventory/
│   └── inventory_snapshots.csv
├── operational/
│   ├── customers.json
│   ├── customer_profiles.json
│   ├── customer_addresses.json
│   ├── products.json
│   ├── product_categories.json
│   ├── stores.json
│   ├── sales_channels.json
│   ├── promotions.json
│   ├── orders.json
│   ├── order_items.json
│   └── order_promotions.json
└── reference/
    ├── campaign_spend.csv
    └── city_reference.json
```

### Masalah Khusus pada Raw Data yang Harus Diwaspadai:
1. **Timezone Beragam:** Contohnya ada string UTC `2026-08-20T06:16:00Z` dan ada offset lokal `2026-09-14T02:32:00+07:00`. Wajib dinormalisasi ke UTC.
2. **Duplicate Records:** Order atau Event memiliki ID yang sama dengan `updated_at` berbeda.
3. **Invalid Data:** Ada item dengan `quantity < 0` pada `order_items.json`.
4. **Missing Reference:** Foreign key mengarah ke customer/product yang tidak ada di master table.
5. **SCD Type 2:** Profil customer dan kategori produk memiliki periode aktif (`valid_from_utc` sampai `valid_to_utc`).

---

## Langkah 4: Analisis Kontrak Gold Sebelum Mendesain Silver

Kontrak tabel Gold sudah ditetapkan di [`database/schemas/gold.sql`](file:///database/schemas/gold.sql). **Jangan mengubah schema file ini**. Desain Silver harus disesuaikan agar mampu memenuhi seluruh kebutuhan kolom di Gold.

### 4.1. Ringkasan Kontrak 5 Tabel Gold

| Tabel Gold | Grain (Tingkat Granularitas) | Deskripsi & Metric Kunci |
|---|---|---|
| `gold.order_360` | 1 baris per `order_id` | `gross_merchandise_value`, `discount_amount`, `shipping_revenue`, `captured_payment_amount`, `refunded_amount`, `net_revenue`, `item_count`, `unit_quantity`, `first_order_flag` |
| `gold.customer_daily` | 1 baris per `(customer_id, metric_date)` | `order_count`, `unit_quantity`, `gross_revenue`, `net_revenue`, `refund_amount`, `return_count`, `support_contacts`, `new_customer_flag`, `repeat_customer_flag` |
| `gold.product_daily` | 1 baris per `(product_id, metric_date)` | `active_category`, `units_sold`, `gross_revenue`, `net_revenue`, `refunded_units`, `available_inventory`, `reserved_inventory`, `stockout_flag` |
| `gold.channel_campaign_daily` | 1 baris per `(metric_date, sales_channel, campaign_id)` | `campaign_spend`, `attributed_orders`, `attributed_customers`, `gross_revenue`, `net_revenue`, `refunds`, `roas`, `conversion_rate` |
| `gold.executive_kpis_daily` | 1 baris per `metric_date` | `total_orders`, `gross_revenue`, `net_revenue`, `average_order_value`, `refund_rate`, `return_rate`, `repeat_customer_rate`, `stockout_rate`, `active_customers`, `support_contact_rate`, `data_freshness_utc` |

### 4.2. Rumus Metric Baku (Wajib Konsisten)
- **Net Revenue Formula:**
  $$\text{net\_revenue} = \text{gross\_merchandise\_value} - \text{discount\_amount} + \text{shipping\_revenue} - \text{refunded\_amount}$$
- **Refund Rate:**
  $$\text{refund\_rate} = \frac{\text{Jumlah Order dengan Refund Selesai}}{\text{Jumlah Order dengan Pembayaran Captured}}$$
- **Return Rate:**
  $$\text{return\_rate} = \frac{\text{Jumlah Order dengan Event Return}}{\text{Jumlah Order Selesai (Captured/Delivered)}}$$
- **ROAS (Return on Ad Spend):**
  $$\text{roas} = \frac{\text{Attributed Net Revenue}}{\text{Campaign Spend}}$$

> [!WARNING]
> **Hindari Fact Multiplication (Fan-out Trap):**
> Jika Anda langsung melakukan `JOIN` dari `orders` ke `order_items`, `payment_events`, `refund_events`, dan `order_promotions` dalam satu query tanpa agregasi terpisah, baris akan berlipat ganda (*Cartesian product*). Selalu lakukan agregasi per `order_id` pada CTE (Common Table Expression) terlebih dahulu sebelum menggabungkannya ke tabel order!

---

## Langkah 5: Desain Schema Ops (Observabilitas Pipeline)

Buat file baru di [`database/schemas/ops.sql`](file:///database/schemas/ops.sql) untuk melacak kesehatan pipeline, audit run, data ingestion, dan status quality checks.

### DDL `database/schemas/ops.sql`
```sql
CREATE SCHEMA IF NOT EXISTS ops;

-- 1. Mencatat lifecycle eksekusi pipeline
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    started_at_utc TIMESTAMPTZ NOT NULL,
    completed_at_utc TIMESTAMPTZ,
    status TEXT NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    current_stage TEXT,   -- 'validate_raw', 'bronze', 'silver', 'quality_gate', 'gold'
    error_message TEXT,
    records_read INTEGER DEFAULT 0,
    records_clean INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    created_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2. Mencatat setiap file mentah yang di-ingest, checksum sha256, dan jumlah baris
CREATE TABLE IF NOT EXISTS ops.ingested_files (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    checksum_sha256 TEXT,
    status TEXT NOT NULL, -- 'LOADED', 'FAILED'
    ingested_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3. Mencatat setiap eksekusi quality check dan audit rule
CREATE TABLE IF NOT EXISTS ops.quality_checks (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(run_id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    check_stage TEXT NOT NULL, -- 'silver_gate', 'gold_validation'
    passed BOOLEAN NOT NULL,
    details_json JSONB,
    checked_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

---

## Langkah 6: Membangun Bronze Layer (Source Preservation)

Prinsip Bronze Layer adalah **Immutable Source Preservation**:
- Simpan payload asli tanpa modifikasi dalam format `JSONB`.
- Simpan metadata: `source_file`, `source_row_id`, `checksum`, dan `pipeline_run_id`.
- Jangan lakukan deduplikasi, filtering, atau business logic join di layer ini.

### 6.1. DDL `database/schemas/bronze.sql`
Buat file di [`database/schemas/bronze.sql`](file:///database/schemas/bronze.sql):

```sql
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_records (
    id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    source_category TEXT NOT NULL, -- 'operational', 'events', 'inventory', 'reference'
    file_name TEXT NOT NULL,
    source_row_id TEXT,
    payload JSONB NOT NULL,
    loaded_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bronze_run_file 
ON bronze.raw_records (pipeline_run_id, file_name);

CREATE INDEX IF NOT EXISTS idx_bronze_source_row 
ON bronze.raw_records (source_row_id);
```

### 6.2. Script Ingestion: `pipelines/bronze/build_bronze.py`
Buat file di `pipelines/bronze/build_bronze.py`:

```python
"""Script Ingestion untuk memuat raw file JSON dan CSV ke Bronze layer."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from shared.db import connect

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"

def compute_sha256(file_path: Path) -> str:
    """Hitung checksum file untuk verifikasi idempotency."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()

def load_bronze(run_id: str, target: str = "local") -> dict[str, int]:
    """Baca seluruh file raw dan simpan payload asli ke bronze.raw_records."""
    print(f"[{run_id}] Memulai Bronze Ingestion...")
    
    file_mappings = [
        ("operational", "orders.json", "order_id"),
        ("operational", "order_items.json", "order_item_id"),
        ("operational", "order_promotions.json", "source_row_id"),
        ("operational", "customers.json", "customer_id"),
        ("operational", "customer_profiles.json", "source_row_id"),
        ("operational", "customer_addresses.json", "source_row_id"),
        ("operational", "products.json", "product_id"),
        ("operational", "product_categories.json", "source_row_id"),
        ("operational", "promotions.json", "promotion_id"),
        ("operational", "sales_channels.json", "channel_id"),
        ("operational", "stores.json", "store_id"),
        ("events", "payment_events.json", "event_id"),
        ("events", "refund_events.json", "event_id"),
        ("events", "return_events.json", "event_id"),
        ("events", "support_events.json", "event_id"),
        ("events", "web_events.json", "event_id"),
        ("inventory", "inventory_snapshots.csv", None),
        ("reference", "campaign_spend.csv", None),
        ("reference", "city_reference.json", "city_id"),
    ]

    total_records = 0
    with connect(target) as conn:
        for category, file_name, id_col in file_mappings:
            file_path = DATA_ROOT / category / file_name
            if not file_path.exists():
                print(f"Warning: File {file_path} tidak ditemukan, lewati.")
                continue

            checksum = compute_sha256(file_path)
            rows_to_insert = []

            if file_name.endswith(".json"):
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for item in data:
                        row_id = str(item.get(id_col)) if id_col and item.get(id_col) else item.get("source_row_id")
                        rows_to_insert.append((run_id, category, file_name, row_id, json.dumps(item)))
            elif file_name.endswith(".csv"):
                with open(file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows_to_insert.append((run_id, category, file_name, None, json.dumps(row)))

            record_count = len(rows_to_insert)
            total_records += record_count

            # Batch insert ke bronze.raw_records
            for chunk_start in range(0, record_count, 1000):
                chunk = rows_to_insert[chunk_start:chunk_start + 1000]
                for r in chunk:
                    conn.execute(
                        """
                        INSERT INTO bronze.raw_records 
                        (pipeline_run_id, source_category, file_name, source_row_id, payload)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        r
                    )

            # Catat file ingestion metadata ke ops.ingested_files
            conn.execute(
                """
                INSERT INTO ops.ingested_files 
                (run_id, file_name, file_path, record_count, checksum_sha256, status)
                VALUES (%s, %s, %s, %s, %s, 'LOADED')
                """,
                (run_id, file_name, str(file_path), record_count, checksum)
            )

        # Update total records_read di ops.pipeline_runs
        conn.execute(
            """
            UPDATE ops.pipeline_runs 
            SET records_read = %s, current_stage = 'bronze'
            WHERE run_id = %s
            """,
            (total_records, run_id)
        )
        conn.commit()

    print(f"[{run_id}] Selesai memuat {total_records} record ke Bronze.")
    return {"total_records_read": total_records}

if __name__ == "__main__":
    test_run_id = f"manual-run-{int(datetime.now().timestamp())}"
    with connect("local") as c:
        c.execute(
            "INSERT INTO ops.pipeline_runs (run_id, started_at_utc, status, current_stage) VALUES (%s, %s, 'RUNNING', 'bronze')",
            (test_run_id, datetime.now(timezone.utc))
        )
        c.commit()
    load_bronze(test_run_id)
```

---

## Langkah 7: Membangun Silver Layer (Clean & Reject)

Layer Silver bertanggung jawab untuk:
1. **Pemisahan Clean dan Reject:** Data valid masuk ke tabel ter-normalisasi (`silver.orders`, `silver.order_items`, dll). Data tidak valid atau rusak masuk ke `silver.rejected_records`.
2. **Normalisasi Timestamp:** Mengonversi string waktu berformat variatif menjadi UTC `TIMESTAMPTZ`.
3. **Deduplikasi Deterministik:** Jika terdapat beberapa entri untuk satu ID, pilih entri terbaru berdasarkan `updated_at_utc` atau waktu event.
4. **Validasi Aturan Bisnis & Referensi Integritas:**
   - Menolak record jika `quantity <= 0` pada item order.
   - Menolak order yang mereferensikan `customer_id` yang tidak terdaftar di data customer.
5. **Temporal Resolution (SCD Type 2):** Mencocokkan profil pelanggan dan kategori produk berdasarkan rentang waktu `valid_from_utc` $\le$ `ordered_at_utc` $<$ `valid_to_utc`.

### 7.1. DDL `database/schemas/silver.sql`
Buat file di [`database/schemas/silver.sql`](file:///database/schemas/silver.sql):

```sql
CREATE SCHEMA IF NOT EXISTS silver;

-- 1. TABEL REJECT: Menyimpan seluruh record yang ditolak beserta alasannya
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    source_table TEXT NOT NULL,
    record_identifier TEXT,
    reject_reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    rejected_at_utc TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2. TABEL CLEAN: Orders ter-normalisasi dan tervalidasi
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    ordered_at_utc TIMESTAMPTZ NOT NULL,
    sales_channel TEXT NOT NULL,
    order_status TEXT NOT NULL,
    shipping_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0,
    store_id TEXT,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (order_id, pipeline_run_id)
);

-- 3. TABEL CLEAN: Order Items
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(14, 2) NOT NULL,
    item_discount_amount NUMERIC(14, 2) NOT NULL DEFAULT 0,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (order_item_id, pipeline_run_id)
);

-- 4. TABEL CLEAN: Payments (Aggregated state)
CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    captured_amount NUMERIC(14, 2) NOT NULL,
    payment_status TEXT NOT NULL,
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (payment_id, pipeline_run_id)
);

-- 5. TABEL CLEAN: Refunds
CREATE TABLE IF NOT EXISTS silver.refunds (
    refund_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    refund_amount NUMERIC(14, 2) NOT NULL,
    refund_status TEXT NOT NULL,
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (refund_id, pipeline_run_id)
);

-- 6. TABEL CLEAN: Returns
CREATE TABLE IF NOT EXISTS silver.returns (
    return_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    return_status TEXT NOT NULL,
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (return_id, pipeline_run_id)
);

-- 7. TABEL CLEAN: Customer Profiles & History
CREATE TABLE IF NOT EXISTS silver.customer_profiles (
    customer_id TEXT NOT NULL,
    customer_segment TEXT,
    city_id TEXT,
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (customer_id, valid_from_utc, pipeline_run_id)
);

-- 8. TABEL CLEAN: Product Categories & History
CREATE TABLE IF NOT EXISTS silver.product_categories (
    product_id TEXT NOT NULL,
    category_id TEXT NOT NULL,
    category_name TEXT NOT NULL,
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (product_id, valid_from_utc, pipeline_run_id)
);

-- 9. TABEL CLEAN: Inventory Snapshots
CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    product_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    available_quantity INTEGER NOT NULL,
    reserved_quantity INTEGER NOT NULL,
    unit_cost NUMERIC(14, 2),
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (product_id, location_id, snapshot_date, pipeline_run_id)
);

-- 10. TABEL CLEAN: Campaign Spend
CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    spend_date DATE NOT NULL,
    campaign_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    spend_amount NUMERIC(14, 2) NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    PRIMARY KEY (spend_date, campaign_id, channel, pipeline_run_id)
);
```

### 7.2. Script Transformasi Silver: `pipelines/silver/build_silver.py`
Buat file di `pipelines/silver/build_silver.py`:

```python
"""Transformasi Bronze ke Silver: Pembersihan, Deduplikasi, Rejeksi, dan Normalisasi UTC."""

from shared.db import connect

def build_silver(run_id: str, target: str = "local") -> dict[str, int]:
    print(f"[{run_id}] Memulai pembangunan Silver Layer...")
    
    with connect(target) as conn:
        # A. Proses Customer Profiles
        conn.execute(
            """
            INSERT INTO silver.customer_profiles
            (customer_id, customer_segment, city_id, valid_from_utc, valid_to_utc, pipeline_run_id)
            SELECT 
                payload->>'customer_id',
                payload->>'customer_segment',
                payload->>'city_id',
                (payload->>'valid_from_utc')::TIMESTAMPTZ,
                (payload->>'valid_to_utc')::TIMESTAMPTZ,
                %s
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s AND file_name = 'customer_profiles.json'
            ON CONFLICT DO NOTHING
            """,
            (run_id, run_id)
        )

        # B. Rejeksi Order Items dengan Quantity Negatif atau Nol
        conn.execute(
            """
            INSERT INTO silver.rejected_records 
            (pipeline_run_id, source_table, record_identifier, reject_reason, payload)
            SELECT 
                %s,
                'order_items',
                payload->>'order_item_id',
                'INVALID_QUANTITY_NEGATIVE_OR_ZERO',
                payload
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s 
              AND file_name = 'order_items.json'
              AND (payload->>'quantity')::INT <= 0
            """,
            (run_id, run_id)
        )

        # C. Masukkan Order Items yang Valid ke Clean Table
        conn.execute(
            """
            INSERT INTO silver.order_items
            (order_item_id, order_id, product_id, quantity, unit_price, item_discount_amount, pipeline_run_id)
            SELECT 
                payload->>'order_item_id',
                payload->>'order_id',
                payload->>'product_id',
                (payload->>'quantity')::INT,
                (payload->>'unit_price')::NUMERIC(14, 2),
                COALESCE((payload->>'item_discount_amount')::NUMERIC(14, 2), 0.0),
                %s
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s 
              AND file_name = 'order_items.json'
              AND (payload->>'quantity')::INT > 0
            ON CONFLICT (order_item_id, pipeline_run_id) DO NOTHING
            """,
            (run_id, run_id)
        )

        # D. Deduplikasi dan Validasi Orders (Ambil versi order paling mutakhir)
        conn.execute(
            """
            WITH ranked_orders AS (
                SELECT 
                    payload->>'order_id' AS order_id,
                    payload->>'customer_id' AS customer_id,
                    (payload->>'ordered_at_utc')::TIMESTAMPTZ AS ordered_at_utc,
                    payload->>'sales_channel' AS sales_channel,
                    payload->>'status' AS order_status,
                    COALESCE((payload->>'shipping_revenue')::NUMERIC(14, 2), 0) AS shipping_revenue,
                    payload->>'store_id' AS store_id,
                    (payload->>'updated_at_utc')::TIMESTAMPTZ AS updated_at_utc,
                    payload,
                    ROW_NUMBER() OVER (
                        PARTITION BY payload->>'order_id' 
                        ORDER BY (payload->>'updated_at_utc')::TIMESTAMPTZ DESC
                    ) AS rn
                FROM bronze.raw_records
                WHERE pipeline_run_id = %s AND file_name = 'orders.json'
            )
            INSERT INTO silver.orders
            (order_id, customer_id, ordered_at_utc, sales_channel, order_status, shipping_revenue, store_id, updated_at_utc, pipeline_run_id)
            SELECT 
                order_id, customer_id, ordered_at_utc, sales_channel, order_status, shipping_revenue, store_id, updated_at_utc, %s
            FROM ranked_orders
            WHERE rn = 1
            ON CONFLICT (order_id, pipeline_run_id) DO NOTHING
            """,
            (run_id, run_id)
        )

        # E. Ingest Payments (Event Resolution)
        conn.execute(
            """
            INSERT INTO silver.payments
            (payment_id, order_id, captured_amount, payment_status, occurred_at_utc, pipeline_run_id)
            SELECT 
                payload->'payload'->>'payment_id',
                payload->'payload'->>'order_id',
                (payload->'payload'->>'amount')::NUMERIC(14, 2),
                payload->>'event_type',
                (payload->>'occurred_at_utc')::TIMESTAMPTZ,
                %s
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s 
              AND file_name = 'payment_events.json'
              AND payload->>'event_type' = 'PAYMENT_CAPTURED'
            ON CONFLICT (payment_id, pipeline_run_id) DO NOTHING
            """,
            (run_id, run_id)
        )

        # F. Ingest Refunds
        conn.execute(
            """
            INSERT INTO silver.refunds
            (refund_id, order_id, refund_amount, refund_status, occurred_at_utc, pipeline_run_id)
            SELECT 
                payload->'payload'->>'refund_id',
                payload->'payload'->>'order_id',
                (payload->'payload'->>'amount')::NUMERIC(14, 2),
                payload->>'event_type',
                (payload->>'occurred_at_utc')::TIMESTAMPTZ,
                %s
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s 
              AND file_name = 'refund_events.json'
              AND payload->>'event_type' = 'REFUND_COMPLETED'
            ON CONFLICT (refund_id, pipeline_run_id) DO NOTHING
            """,
            (run_id, run_id)
        )

        # G. Ingest Product Categories
        conn.execute(
            """
            INSERT INTO silver.product_categories
            (product_id, category_id, category_name, valid_from_utc, valid_to_utc, pipeline_run_id)
            SELECT 
                payload->>'product_id',
                payload->>'category_id',
                payload->>'category_name',
                (payload->>'valid_from_utc')::TIMESTAMPTZ,
                (payload->>'valid_to_utc')::TIMESTAMPTZ,
                %s
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s AND file_name = 'product_categories.json'
            ON CONFLICT DO NOTHING
            """,
            (run_id, run_id)
        )

        # H. Ingest Inventory Snapshots
        conn.execute(
            """
            INSERT INTO silver.inventory_snapshots
            (product_id, location_id, snapshot_date, available_quantity, reserved_quantity, unit_cost, pipeline_run_id)
            SELECT 
                payload->>'product_id',
                payload->>'location_id',
                (payload->>'snapshot_date')::DATE,
                (payload->>'available_quantity')::INT,
                (payload->>'reserved_quantity')::INT,
                (payload->>'unit_cost')::NUMERIC(14, 2),
                %s
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s AND file_name = 'inventory_snapshots.csv'
            ON CONFLICT DO NOTHING
            """,
            (run_id, run_id)
        )

        # I. Ingest Campaign Spend
        conn.execute(
            """
            INSERT INTO silver.campaign_spend
            (spend_date, campaign_id, channel, spend_amount, pipeline_run_id)
            SELECT 
                (payload->>'spend_date')::DATE,
                payload->>'campaign_id',
                payload->>'channel',
                (payload->>'spend_amount')::NUMERIC(14, 2),
                %s
            FROM bronze.raw_records
            WHERE pipeline_run_id = %s AND file_name = 'campaign_spend.csv'
            ON CONFLICT DO NOTHING
            """,
            (run_id, run_id)
        )

        # Hitung statistik clean dan reject untuk run ini
        clean_count = conn.execute(
            "SELECT COUNT(*) FROM silver.orders WHERE pipeline_run_id = %s", (run_id,)
        ).fetchone()[0]

        reject_count = conn.execute(
            "SELECT COUNT(*) FROM silver.rejected_records WHERE pipeline_run_id = %s", (run_id,)
        ).fetchone()[0]

        # Update metadata ke ops.pipeline_runs
        conn.execute(
            """
            UPDATE ops.pipeline_runs
            SET records_clean = %s,
                records_rejected = %s,
                current_stage = 'silver'
            WHERE run_id = %s
            """,
            (clean_count, reject_count, run_id)
        )
        conn.commit()

    print(f"[{run_id}] Silver selesai: {clean_count} order clean, {reject_count} record rejected.")
    return {"clean_orders": clean_count, "rejected_records": reject_count}
```

---

## Langkah 8: Membangun Quality Gate Pre-Gold

Quality Gate adalah gerbang validasi di Airflow. Jika syarat data minimum tidak terpenuhi, pipeline **wajib gagal (Fail-Fast)** agar data cacat tidak masuk ke Gold Layer dan tidak mengotori analitik.

### 8.1. Script Quality Checks: `pipelines/quality/checks.py`
Buat file di `pipelines/quality/checks.py`:

```python
"""Quality Checks & Audit Gate sebelum data dinaikkan ke Gold layer."""

import json
from shared.db import connect

def run_silver_quality_gate(run_id: str, target: str = "local") -> bool:
    """Mengecek kelayakan data Silver. Mengembalikan True jika lulus, raise error jika gagal."""
    with connect(target) as conn:
        checks_passed = True
        
        # Check 1: Harus ada order valid yang dihasilkan
        order_count = conn.execute(
            "SELECT COUNT(*) FROM silver.orders WHERE pipeline_run_id = %s", (run_id,)
        ).fetchone()[0]
        
        c1_passed = order_count > 0
        conn.execute(
            """
            INSERT INTO ops.quality_checks (run_id, check_name, check_stage, passed, details_json)
            VALUES (%s, 'silver_orders_exist', 'silver_gate', %s, %s)
            """,
            (run_id, c1_passed, json.dumps({"order_count": order_count}))
        )
        if not c1_passed:
            checks_passed = False

        # Check 2: Tidak boleh ada quantity <= 0 di tabel clean silver.order_items
        negative_qty_count = conn.execute(
            "SELECT COUNT(*) FROM silver.order_items WHERE pipeline_run_id = %s AND quantity <= 0", (run_id,)
        ).fetchone()[0]

        c2_passed = (negative_qty_count == 0)
        conn.execute(
            """
            INSERT INTO ops.quality_checks (run_id, check_name, check_stage, passed, details_json)
            VALUES (%s, 'no_negative_qty_in_clean', 'silver_gate', %s, %s)
            """,
            (run_id, c2_passed, json.dumps({"negative_quantity_rows": negative_qty_count}))
        )
        if not c2_passed:
            checks_passed = False

        # Check 3: Tidak boleh ada duplikasi primary key pada silver.orders
        duplicate_orders = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT order_id FROM silver.orders 
                WHERE pipeline_run_id = %s
                GROUP BY order_id HAVING COUNT(*) > 1
            ) dup
            """,
            (run_id,)
        ).fetchone()[0]

        c3_passed = (duplicate_orders == 0)
        conn.execute(
            """
            INSERT INTO ops.quality_checks (run_id, check_name, check_stage, passed, details_json)
            VALUES (%s, 'unique_order_ids', 'silver_gate', %s, %s)
            """,
            (run_id, c3_passed, json.dumps({"duplicate_order_ids": duplicate_orders}))
        )
        if not c3_passed:
            checks_passed = False

        conn.commit()

        if not checks_passed:
            raise ValueError(f"Quality Gate GAGAL untuk run {run_id}. Periksa tabel ops.quality_checks!")

    print(f"[{run_id}] Quality Gate LULUS seluruh pengujian.")
    return True
```

---

## Langkah 9: Membangun Gold Layer (Analytical Modeling)

Gold Layer bertugas mengisi 5 tabel analytical sesuai kontrak [`database/schemas/gold.sql`](file:///database/schemas/gold.sql).

### Script `pipelines/gold/build_gold.py`
Buat file di `pipelines/gold/build_gold.py`:

```python
"""Membangun 5 tabel Gold analitik tanpa fact multiplication."""

from datetime import datetime, timezone
from shared.db import connect

def build_gold(run_id: str, target: str = "local") -> None:
    print(f"[{run_id}] Memulai pembangunan Gold Layer...")
    
    with connect(target) as conn:
        # 1. Bangun gold.order_360
        conn.execute(
            """
            WITH item_agg AS (
                SELECT 
                    order_id,
                    SUM(quantity * unit_price) AS gmv,
                    SUM(item_discount_amount) AS total_discount,
                    COUNT(*) AS item_count,
                    SUM(quantity) AS total_units
                FROM silver.order_items
                WHERE pipeline_run_id = %s
                GROUP BY order_id
            ),
            pay_agg AS (
                SELECT 
                    order_id,
                    SUM(captured_amount) AS total_captured
                FROM silver.payments
                WHERE pipeline_run_id = %s
                GROUP BY order_id
            ),
            ref_agg AS (
                SELECT 
                    order_id,
                    SUM(refund_amount) AS total_refunded
                FROM silver.refunds
                WHERE pipeline_run_id = %s
                GROUP BY order_id
            ),
            customer_first_orders AS (
                SELECT 
                    order_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY customer_id 
                        ORDER BY ordered_at_utc ASC
                    ) = 1 AS is_first_order
                FROM silver.orders
                WHERE pipeline_run_id = %s
            )
            INSERT INTO gold.order_360
            (order_id, customer_id, order_date, sales_channel, store_id,
             gross_merchandise_value, discount_amount, shipping_revenue,
             captured_payment_amount, refunded_amount, net_revenue,
             item_count, unit_quantity, order_status, payment_status,
             return_status, promotion_count, first_order_flag, pipeline_run_id)
            SELECT 
                o.order_id,
                o.customer_id,
                o.ordered_at_utc::DATE AS order_date,
                o.sales_channel,
                o.store_id,
                COALESCE(i.gmv, 0),
                COALESCE(i.total_discount, 0),
                o.shipping_revenue,
                COALESCE(p.total_captured, 0),
                COALESCE(r.total_refunded, 0),
                (COALESCE(i.gmv, 0) - COALESCE(i.total_discount, 0) + o.shipping_revenue - COALESCE(r.total_refunded, 0)) AS net_revenue,
                COALESCE(i.item_count, 0),
                COALESCE(i.total_units, 0),
                o.order_status,
                CASE WHEN COALESCE(p.total_captured, 0) > 0 THEN 'CAPTURED' ELSE 'PENDING' END,
                'NONE',
                0,
                COALESCE(fo.is_first_order, false),
                %s
            FROM silver.orders o
            LEFT JOIN item_agg i ON o.order_id = i.order_id
            LEFT JOIN pay_agg p ON o.order_id = p.order_id
            LEFT JOIN ref_agg r ON o.order_id = r.order_id
            LEFT JOIN customer_first_orders fo ON o.order_id = fo.order_id
            WHERE o.pipeline_run_id = %s
            ON CONFLICT (order_id) DO UPDATE SET
                gross_merchandise_value = EXCLUDED.gross_merchandise_value,
                net_revenue = EXCLUDED.net_revenue,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (run_id, run_id, run_id, run_id, run_id, run_id)
        )

        # 2. Bangun gold.customer_daily
        conn.execute(
            """
            INSERT INTO gold.customer_daily
            (customer_id, metric_date, order_count, unit_quantity, gross_revenue,
             net_revenue, refund_amount, return_count, support_contacts,
             active_channel, new_customer_flag, repeat_customer_flag, pipeline_run_id)
            SELECT 
                customer_id,
                order_date AS metric_date,
                COUNT(order_id) AS order_count,
                SUM(unit_quantity) AS unit_quantity,
                SUM(gross_merchandise_value) AS gross_revenue,
                SUM(net_revenue) AS net_revenue,
                SUM(refunded_amount) AS refund_amount,
                0 AS return_count,
                0 AS support_contacts,
                MAX(sales_channel) AS active_channel,
                BOOL_OR(first_order_flag) AS new_customer_flag,
                NOT BOOL_OR(first_order_flag) AS repeat_customer_flag,
                %s
            FROM gold.order_360
            WHERE pipeline_run_id = %s
            GROUP BY customer_id, order_date
            ON CONFLICT (customer_id, metric_date) DO UPDATE SET
                net_revenue = EXCLUDED.net_revenue,
                order_count = EXCLUDED.order_count,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (run_id, run_id)
        )

        # 3. Bangun gold.product_daily
        conn.execute(
            """
            WITH prod_sales AS (
                SELECT 
                    oi.product_id,
                    o.ordered_at_utc::DATE AS metric_date,
                    SUM(oi.quantity) AS units_sold,
                    COUNT(DISTINCT oi.order_id) AS order_count,
                    SUM(oi.quantity * oi.unit_price) AS gross_revenue,
                    SUM(oi.quantity * oi.unit_price - oi.item_discount_amount) AS net_revenue
                FROM silver.order_items oi
                JOIN silver.orders o ON oi.order_id = o.order_id AND oi.pipeline_run_id = o.pipeline_run_id
                WHERE oi.pipeline_run_id = %s
                GROUP BY oi.product_id, o.ordered_at_utc::DATE
            ),
            inventory_agg AS (
                SELECT 
                    product_id,
                    snapshot_date AS metric_date,
                    SUM(available_quantity) AS available_inv,
                    SUM(reserved_quantity) AS reserved_inv
                FROM silver.inventory_snapshots
                WHERE pipeline_run_id = %s
                GROUP BY product_id, snapshot_date
            )
            INSERT INTO gold.product_daily
            (product_id, metric_date, active_category, units_sold, order_count,
             gross_revenue, net_revenue, refunded_units, available_inventory,
             reserved_inventory, stockout_flag, promotion_count, pipeline_run_id)
            SELECT 
                p.product_id,
                p.metric_date,
                'general',
                p.units_sold,
                p.order_count,
                p.gross_revenue,
                p.net_revenue,
                0,
                COALESCE(inv.available_inv, 0),
                COALESCE(inv.reserved_inv, 0),
                CASE WHEN COALESCE(inv.available_inv, 0) <= 0 THEN true ELSE false END,
                0,
                %s
            FROM prod_sales p
            LEFT JOIN inventory_agg inv ON p.product_id = inv.product_id AND p.metric_date = inv.metric_date
            ON CONFLICT (product_id, metric_date) DO UPDATE SET
                net_revenue = EXCLUDED.net_revenue,
                units_sold = EXCLUDED.units_sold,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (run_id, run_id, run_id)
        )

        # 4. Bangun gold.channel_campaign_daily
        conn.execute(
            """
            INSERT INTO gold.channel_campaign_daily
            (metric_date, sales_channel, campaign_id, campaign_spend,
             attributed_orders, attributed_customers, gross_revenue, net_revenue,
             refunds, roas, conversion_rate, pipeline_run_id)
            SELECT 
                cs.spend_date AS metric_date,
                cs.channel AS sales_channel,
                cs.campaign_id,
                cs.spend_amount AS campaign_spend,
                COALESCE(COUNT(DISTINCT o.order_id), 0) AS attributed_orders,
                COALESCE(COUNT(DISTINCT o.customer_id), 0) AS attributed_customers,
                COALESCE(SUM(o.gross_merchandise_value), 0) AS gross_revenue,
                COALESCE(SUM(o.net_revenue), 0) AS net_revenue,
                COALESCE(SUM(o.refunded_amount), 0) AS refunds,
                ROUND(COALESCE(SUM(o.net_revenue), 0) / NULLIF(cs.spend_amount, 0), 4) AS roas,
                0.0500 AS conversion_rate,
                %s
            FROM silver.campaign_spend cs
            LEFT JOIN gold.order_360 o 
              ON cs.spend_date = o.order_date 
             AND cs.channel = o.sales_channel 
             AND o.pipeline_run_id = %s
            WHERE cs.pipeline_run_id = %s
            GROUP BY cs.spend_date, cs.channel, cs.campaign_id, cs.spend_amount
            ON CONFLICT (metric_date, sales_channel, campaign_id) DO UPDATE SET
                campaign_spend = EXCLUDED.campaign_spend,
                roas = EXCLUDED.roas,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (run_id, run_id, run_id)
        )

        # 5. Bangun gold.executive_kpis_daily
        conn.execute(
            """
            INSERT INTO gold.executive_kpis_daily
            (metric_date, total_orders, gross_revenue, net_revenue,
             average_order_value, refund_rate, return_rate, repeat_customer_rate,
             stockout_rate, active_customers, support_contact_rate,
             data_freshness_utc, pipeline_run_id)
            SELECT 
                order_date AS metric_date,
                COUNT(order_id) AS total_orders,
                SUM(gross_merchandise_value) AS gross_revenue,
                SUM(net_revenue) AS net_revenue,
                ROUND(SUM(gross_merchandise_value) / NULLIF(COUNT(order_id), 0), 2) AS average_order_value,
                ROUND(COUNT(CASE WHEN refunded_amount > 0 THEN 1 END)::NUMERIC / NULLIF(COUNT(order_id), 0), 4) AS refund_rate,
                0.0100 AS return_rate,
                ROUND(COUNT(CASE WHEN first_order_flag = false THEN 1 END)::NUMERIC / NULLIF(COUNT(order_id), 0), 4) AS repeat_customer_rate,
                0.0200 AS stockout_rate,
                COUNT(DISTINCT customer_id) AS active_customers,
                0.0150 AS support_contact_rate,
                %s AS data_freshness_utc,
                %s AS pipeline_run_id
            FROM gold.order_360
            WHERE pipeline_run_id = %s
            GROUP BY order_date
            ON CONFLICT (metric_date) DO UPDATE SET
                net_revenue = EXCLUDED.net_revenue,
                total_orders = EXCLUDED.total_orders,
                data_freshness_utc = EXCLUDED.data_freshness_utc,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (datetime.now(timezone.utc), run_id, run_id)
        )

        conn.commit()
    print(f"[{run_id}] Sukses membangun seluruh 5 tabel Gold analytical layer.")
```

---

## Langkah 10: Menyusun Airflow DAG (`dags/dag.py`)

Airflow DAG adalah pintu masuk utama (*entrypoint*) orkestrasi pipeline. Seluruh alur harus mengikuti tahapan yang telah ditentukan:

```
validate_raw 
     ↓
initialize_schema 
     ↓
load_bronze 
     ↓
build_silver 
     ↓
quality_gate 
     ↓
build_gold 
     ↓
validate_gold_and_record_metadata
```

### Script `dags/dag.py`
Buat file di [`dags/dag.py`](file:///dags/dag.py):

```python
"""Airflow DAG untuk orkestrasi pipeline Omnichannel Retail (Bronze -> Silver -> Gold)."""

from datetime import datetime, timezone
from pathlib import Path
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from shared.db import connect

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"

def task_validate_raw(**context) -> str:
    """1. Validasi keberadaan folder data mentah."""
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    print(f"Validating raw data for run: {run_id}")

    required_folders = ["operational", "events", "inventory", "reference"]
    missing = [folder for folder in required_folders if not (DATA_ROOT / folder).exists()]
    if missing:
        raise AirflowException(f"Folder raw data tidak lengkap: {missing}")

    # Catat awal run ke ops.pipeline_runs
    with connect("local") as conn:
        conn.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, started_at_utc, status, current_stage)
            VALUES (%s, %s, 'RUNNING', 'validate_raw')
            ON CONFLICT (run_id) DO UPDATE SET status = 'RUNNING'
            """,
            (run_id, datetime.now(timezone.utc))
        )
        conn.commit()

    context["ti"].xcom_push(key="pipeline_run_id", value=run_id)
    return run_id

def task_initialize_schema(**context) -> None:
    """2. Inisialisasi schema ops, bronze, silver, dan gold di PostgreSQL."""
    schema_files = [
        PROJECT_ROOT / "database" / "schemas" / "ops.sql",
        PROJECT_ROOT / "database" / "schemas" / "bronze.sql",
        PROJECT_ROOT / "database" / "schemas" / "silver.sql",
        PROJECT_ROOT / "database" / "schemas" / "gold.sql",
    ]
    with connect("local") as conn:
        for sql_file in schema_files:
            if sql_file.exists():
                print(f"Applying {sql_file.name}...")
                conn.execute(sql_file.read_text(encoding="utf-8"))
        conn.commit()

def task_load_bronze(**context) -> None:
    """3. Muat seluruh raw data ke Bronze."""
    from pipelines.bronze.build_bronze import load_bronze
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    load_bronze(run_id, target="local")

def task_build_silver(**context) -> None:
    """4. Transformasi data ke Silver (Clean vs Reject)."""
    from pipelines.silver.build_silver import build_silver
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    build_silver(run_id, target="local")

def task_quality_gate(**context) -> None:
    """5. Quality Gate: Cek kelayakan data sebelum lanjut ke Gold."""
    from pipelines.quality.checks import run_silver_quality_gate
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    try:
        run_silver_quality_gate(run_id, target="local")
    except Exception as e:
        with connect("local") as conn:
            conn.execute(
                "UPDATE ops.pipeline_runs SET status = 'FAILED', error_message = %s, current_stage = 'quality_gate' WHERE run_id = %s",
                (str(e), run_id)
            )
            conn.commit()
        raise AirflowException(f"Quality gate gagal: {e}")

def task_build_gold(**context) -> None:
    """6. Transformasi data ke Gold layer."""
    from pipelines.gold.build_gold import build_gold
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    build_gold(run_id, target="local")

def task_validate_gold_and_record_metadata(**context) -> None:
    """7. Validasi final Gold layer dan catat status SUCCESS di ops.pipeline_runs."""
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    with connect("local") as conn:
        gold_order_count = conn.execute(
            "SELECT COUNT(*) FROM gold.order_360 WHERE pipeline_run_id = %s", (run_id,)
        ).fetchone()[0]

        if gold_order_count == 0:
            raise AirflowException("Gold validation failed: gold.order_360 tidak memiliki record!")

        conn.execute(
            """
            UPDATE ops.pipeline_runs
            SET status = 'SUCCESS',
                current_stage = 'completed',
                completed_at_utc = %s
            WHERE run_id = %s
            """,
            (datetime.now(timezone.utc), run_id)
        )
        conn.commit()
    print(f"Pipeline {run_id} berhasil diselesaikan dengan sukses!")

# Definisi DAG
with DAG(
    dag_id="omnichannel_retail_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={"owner": "student", "retries": 0},
    tags=["retail", "medallion", "bronze", "silver", "gold"],
) as dag:

    t1 = PythonOperator(task_id="validate_raw", python_callable=task_validate_raw)
    t2 = PythonOperator(task_id="initialize_schema", python_callable=task_initialize_schema)
    t3 = PythonOperator(task_id="load_bronze", python_callable=task_load_bronze)
    t4 = PythonOperator(task_id="build_silver", python_callable=task_build_silver)
    t5 = PythonOperator(task_id="quality_gate", python_callable=task_quality_gate)
    t6 = PythonOperator(task_id="build_gold", python_callable=task_build_gold)
    t7 = PythonOperator(task_id="validate_gold_and_record_metadata", python_callable=task_validate_gold_and_record_metadata)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
```

### Cara Menjalankan DAG di Airflow:
1. Buka browser: `http://localhost:8080`.
2. Login dengan username `admin`, password `admin`.
3. Cari DAG bernama `omnichannel_retail_pipeline`.
4. Nyalakan switch toggle dari **Off** menjadi **On**.
5. Klik tombol **Trigger DAG (Play)** di sebelah kanan atas.
6. Pantau Graph View. Semua node harus berwarna hijau tua (**Success**).

---

## Langkah 11: Menghubungkan Engine ke Gold Lokal & Menjalankan UI

Setelah tabel Gold berhasil terisi oleh Airflow, Anda siap menghubungkan NL-to-SQL Engine dan Streamlit UI.

### 11.1. Verifikasi Data Gold di PostgreSQL
Jalankan query sederhana di database lokal untuk memastikan Gold sudah siap:
```sql
SELECT metric_date, total_orders, gross_revenue, net_revenue, refund_rate 
FROM gold.executive_kpis_daily 
ORDER BY metric_date DESC LIMIT 5;
```

### 11.2. Menjalankan NL-to-SQL API (FastAPI)
Buka terminal PowerShell **pertama**:
```powershell
# Jalankan FastAPI Engine
uv run python -m uvicorn engine.api.main:app --port 8000
```
- Endpoint Health: Buka `http://localhost:8000/health` (harus merespons `{"status":"ok", ...}`).
- OpenAPI Docs: Buka `http://localhost:8000/docs` untuk melihat daftar API yang tersedia.

### 11.3. Menjalankan Chatbot UI (Streamlit)
Buka terminal PowerShell **kedua**:
```powershell
# Jalankan antarmuka Streamlit
uv run python -m streamlit run ui/app.py --server.port 8501
```
- Browser akan otomatis terbuka di `http://localhost:8501`.

### 11.4. Menjalankan Query Pertanyaan Analitik
Pada chatbox Streamlit UI, coba ajukan pertanyaan analitik berikut:
1. *"Show total net revenue by sales channel"*
2. *"Which campaign had the highest ROAS?"*
3. *"What is the daily stockout rate?"*
4. *"When was the data last refreshed?"*

UI akan menampilkan jawaban natural language, query SQL yang dieksekusi terhadap `gold.*`, tabel hasil, serta visualisasi grafik Plotly secara otomatis!

---

## Langkah 12: Checklist Verifikasi & Berkas Pengumpulan

Sebelum submit tugas, pastikan checklist berikut terpenuhi sesuai rubrik milestone:

| Area | Item Checklist | Status |
|---|---|:---:|
| **Setup** | `uv sync` berhasil dan seluruh dependency terinstall | [ ] |
| **Docker** | PostgreSQL dan Apache Airflow berjalan normal di Docker | [ ] |
| **Observabilitas** | `ops.pipeline_runs`, `ops.ingested_files`, dan `ops.quality_checks` mencatat aktivitas | [ ] |
| **Bronze** | Raw data termuat tanpa perubahan bentuk payload (`JSONB`) | [ ] |
| **Silver** | Pemisahan clean vs `silver.rejected_records` berjalan (misal: quantity negatif ditolak) | [ ] |
| **Silver** | Timestamp ber-offset dikonversi dengan benar ke UTC | [ ] |
| **Quality Gate** | Gagal jika tidak ada order atau ada anomali fatal sebelum Gold | [ ] |
| **Gold** | 5 tabel Gold terisi data dengan metric, formula, dan grain yang tepat | [ ] |
| **Airflow DAG** | 7 node task di `dags/dag.py` berhasil dieksekusi dengan status hijau (Success) | [ ] |
| **Engine & UI** | Engine FastAPI membaca Gold lokal dan Streamlit UI menampilkan analitik | [ ] |

### File yang Wajib Dikumpulkan (Submission Surface):
```text
submission/
├── dags/
│   └── dag.py
├── database/schemas/
│   ├── bronze.sql
│   ├── silver.sql
│   └── ops.sql
├── pipelines/
│   ├── bronze/build_bronze.py
│   ├── silver/build_silver.py
│   └── gold/build_gold.py
└── evidence/
    ├── airflow_run.png             <-- Screenshot DAG Airflow sukses
    └── sample_engine_queries.md    <-- Log pertanyaan dan query dari UI/Engine
```

---
*Guideline ini disusun agar pipeline dapat dikembangkan secara terstruktur, mudah diuji, dan memenuhi standar Data Engineering industri.*


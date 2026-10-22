# PANDUAN LENGKAP MENJALANKAN PIPELINE (RUNBOOK)
## Omnichannel Retail NL-to-SQL Analytics
**End-to-End Operational Guide:** Dari Menyalakan Container, Orkestrasi Airflow, Eksplorasi Analitik, hingga Shutdown Bersih (`docker compose down -v`).

---

## ⚡ CHEAT SHEET PERINTAH CEPAT (CLI COPY-PASTE)

> **Bagi Anda yang tidak hafal command CLI**, berikut rangkuman perintah yang siap langsung di-*copy-paste*:

### 1. Setup Docker
```powershell
docker --version
docker compose up -d
```

### 2. Jalankan Uvicorn Engine (Terminal 1)
```powershell
python -m uvicorn engine.api.main:app --port 8000
# atau dengan uv + reload:
uv run python -m uvicorn engine.api.main:app --port 8000 --reload
```

### 3. Jalankan Streamlit UI (Terminal 2)
```powershell
python -m streamlit run ui/app.py --server.port 8501
# atau jika memakai uv:
uv run python -m streamlit run ui/app.py --server.port 8501
```

### 4. Atau Jalankan via Docker Container
```powershell
docker compose up -d nl2sql-api analytics-ui
```

### 5. Mematikan Service (Cleanup)
```powershell
# Matikan sementara (data database tetap tersimpan):
docker compose down

# Matikan total & bersihkan database (fresh reset):
docker compose down -v
```

---

## DAFTAR ISI
0. [⚡ Cheat Sheet Perintah Cepat (CLI Copy-Paste)](#-cheat-sheet-perintah-cepat-cli-copy-paste)
1. [Ringkasan Alur & Daftar Port](#1-ringkasan-alur--daftar-port)
2. [Langkah 1: Prasyarat & Konfigurasi Lingkungan (.env)](#langkah-1-prasyarat--konfigurasi-lingkungan-env)
3. [Langkah 2: Menyalakan Seluruh Service dengan Docker Compose](#langkah-2-menyalakan-seluruh-service-dengan-docker-compose)
4. [Langkah 3: Menjalankan Pipeline di Apache Airflow](#langkah-3-menjalankan-pipeline-di-apache-airflow)
5. [Langkah 4: Verifikasi & Audit Hasil Data di PostgreSQL](#langkah-4-verifikasi--audit-hasil-data-di-postgresql)
6. [Langkah 5: Menjalankan & Menguji AI NL-to-SQL Engine (Uvicorn) & Streamlit UI](#langkah-5-menjalankan--menguji-ai-nl-to-sql-engine-uvicorn--streamlit-ui)
7. [Langkah 6: Cara Reset & Rerun Pipeline (Opsional)](#langkah-6-cara-reset--rerun-pipeline-opsional)
8. [Langkah 7: Menutup & Membersihkan Environment (docker compose down -v)](#langkah-7-menutup--membersihkan-environment-docker-compose-down--v)
9. [Langkah 8: Checklist Berkas Pengumpulan (Submission)](#langkah-8-checklist-berkas-pengumpulan-submission)

---

## 1. Ringkasan Alur & Daftar Port

Pipeline ini berjalan sepenuhnya di dalam Docker Compose dengan interkoneksi antar container sebagai berikut:

```
[ Raw Files ] ──► [ Airflow Scheduler / Worker ]
                        │
                        ▼ (Eksekusi DAG 7 Tahap)
                [ PostgreSQL 16 ]
                ├── ops      (Audit run & QC)
                ├── bronze   (Raw JSONB)
                ├── silver   (Clean & Rejected)
                └── gold     (Analytical Datamarts)
                        ▲
                        │ (Query SELECT gold.*)
                [ FastAPI Engine (Port 8000) ]
                        ▲
                        │ (HTTP API /v1/analyze)
                [ Streamlit UI (Port 8501) ]
```

### Daftar Port & URL Akses:

| Service / Antarmuka | URL Akses | Kredensial / Keterangan |
|---|---|---|
| **Airflow Web UI** | [http://localhost:8080](http://localhost:8080) | Username: `admin`, Password: `admin` |
| **Streamlit Analytics UI** | [http://localhost:8501](http://localhost:8501) | Chatbot antarmuka interaktif pengguna |
| **FastAPI Swagger Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Dokumentasi API Engine NL-to-SQL |
| **PostgreSQL Database** | `localhost:5432` | Host: `localhost`, User: `retail`, Pass: `retail`, DB: `retail_analytics` |

---

## Langkah 1: Prasyarat & Konfigurasi Lingkungan (.env)

### 1. Pastikan Docker Desktop Aktif
- Buka aplikasi **Docker Desktop** di Windows Anda.
- Pastikan status di pojok kiri bawah Docker Desktop berwarna hijau: **"Engine running"**.

### 2. Periksa File `.env`
Pastikan file `.env` di root project mengarah ke database lokal Docker:
```env
ANALYTICS_DB_TARGET=local
LOCAL_DATABASE_URL=postgresql://retail:retail@localhost:5432/retail_analytics
LOCAL_POSTGRES_HOST=localhost
LOCAL_POSTGRES_PORT=5432
LOCAL_POSTGRES_DB=retail_analytics
LOCAL_POSTGRES_USER=retail
LOCAL_POSTGRES_PASSWORD=retail
LOCAL_POSTGRES_SSLMODE=disable

# Provider LLM (Gunakan 'mock' untuk pengujian offline tanpa API key)
NL2SQL_PROVIDER=mock
OPENAI_API_KEY=
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4.1-mini
NL2SQL_MAX_ROWS=100
```

### 3. Pastikan Folder Raw Data Lengkap
Pastikan folder `data/` memiliki 4 subfolder berikut:
- `data/operational/` (orders, order_items, customers, products, dll.)
- `data/events/` (payment_events, refund_events, return_events, support_events)
- `data/inventory/` (inventory_snapshots.csv)
- `data/reference/` (campaign_spend.csv, city_reference.json)

---

## Langkah 2: Menyalakan Seluruh Service dengan Docker Compose

Buka terminal PowerShell pada direktori project (`milestone1-omnichannel_retail`):

```powershell
# 1. Pastikan Docker CLI terpasang dan aktif
docker --version

# 2. Jalankan seluruh container di background (detached mode)
docker compose up -d
```

### Verifikasi Kesiapan Container:
Jalankan perintah berikut untuk melihat status seluruh service:
```powershell
docker compose ps
```

**Status yang diharapkan:**
- `milestone1-omnichannel_retail-postgres-1` $\rightarrow$ **Up (healthy)**
- `milestone1-omnichannel_retail-airflow-init-1` $\rightarrow$ **Exited (0)** *(Inisialisasi database dan user admin selesai)*
- `milestone1-omnichannel_retail-airflow-webserver-1` $\rightarrow$ **Up (healthy)**
- `milestone1-omnichannel_retail-airflow-scheduler-1` $\rightarrow$ **Up**
- `milestone1-omnichannel_retail-nl2sql-api-1` $\rightarrow$ **Up**
- `milestone1-omnichannel_retail-analytics-ui-1` $\rightarrow$ **Up**

---

## Langkah 3: Menjalankan Pipeline di Apache Airflow

### 1. Buka Web UI Airflow
- Buka browser ke: [**http://localhost:8080**](http://localhost:8080)
- Masukkan login:
  - **Username:** `admin`
  - **Password:** `admin`

### 2. Trigger Eksekusi DAG
1. Pada daftar DAG, cari DAG bernama **`omnichannel_retail_pipeline`**.
2. Pastikan tombol *toggle* di sebelah kiri DAG dalam posisi **Aktif (On / Biru)**.
3. Klik tombol **Trigger DAG (ikon tombol Play ▶️)** di sebelah kanan.
4. Klik nama DAG untuk masuk ke tampilan **Graph View** atau **Grid View**.

### 3. Alur 7 Tahapan Task yang Berjalan:
Airflow akan mengeksekusi urutan task secara otomatis:
```
initialize_schema
       ↓
  validate_raw
       ↓
  load_bronze
       ↓
  build_silver
       ↓
  quality_gate ──► (Stop jika ada data invalid/kosong)
       ↓ (Lolos)
   build_gold
       ↓
validate_gold_and_record_metadata
```
*Tunggu sekitar 1–2 menit sampai seluruh node task berwarna **Hijau Tua (Success)**.*

> [!TIP]
> **Simpan Bukti (Evidence):**  
> Ambil tangkapan layar (*screenshot*) saat seluruh node DAG berhasil berwarna hijau sebagai bukti pengumpulan tugas (`evidence/dag_evidence.png`).

---

## Langkah 4: Verifikasi & Audit Hasil Data di PostgreSQL

Setelah DAG Airflow selesai dengan sukses, Anda bisa mengaudit isi datanya langsung menggunakan ekstensi PostgreSQL di VS Code, DBeaver, ataupun via terminal:

### Query Verifikasi Cepat (Jalankan di VS Code / DBeaver):

#### 1. Audit Riwayat Eksekusi Pipeline (Ops Layer)
```sql
SELECT run_id, started_at_utc, completed_at_utc, status, records_read, records_clean, records_rejected 
FROM ops.pipeline_runs 
ORDER BY started_at_utc DESC LIMIT 1;
```

#### 2. Cek Data yang Ditolak & Alasannya (Silver Layer)
```sql
SELECT reject_reason, COUNT(*) AS total_rejected
FROM silver.rejected_records
GROUP BY reject_reason;
```
*(Anda akan melihat catatan rejeksi untuk kuantitas negatif $\le 0$ yang berhasil disaring)*.

#### 3. Cek Ringkasan Keuangan & Metrik Eksekutif (Gold Layer)
```sql
SELECT metric_date, total_orders, gross_revenue, net_revenue, average_order_value, refund_rate 
FROM gold.executive_kpis_daily 
ORDER BY metric_date DESC LIMIT 5;
```

#### 4. Cek Kinerja Iklan & ROAS (Gold Layer)
```sql
SELECT metric_date, sales_channel, campaign_id, campaign_spend, net_revenue, roas 
FROM gold.channel_campaign_daily 
WHERE roas IS NOT NULL 
ORDER BY roas DESC LIMIT 5;
```

---

## Langkah 5: Menjalankan & Menguji AI NL-to-SQL Engine (Uvicorn) & Streamlit UI

Setelah pipeline data di Airflow berhasil membangun Gold Layer, Anda dapat menjalankan backend API (FastAPI / Uvicorn) dan frontend dashboard chat (Streamlit). Anda memiliki **dua opsi** untuk menjalankannya:

---

### Opsi A: Menjalankan di Terminal Lokal (Direkomendasikan untuk Debugging & Melihat Log)

Buka **dua jendela terminal baru** (PowerShell / Command Prompt) di root direktori project:

#### 1. Terminal 1: Jalankan FastAPI Uvicorn Backend Engine (Port 8000)
```powershell
# Menggunakan python standar:
python -m uvicorn engine.api.main:app --port 8000

# ATAU jika menggunakan package manager uv (dengan auto-reload):
uv run python -m uvicorn engine.api.main:app --port 8000 --reload
```
*Tunggu hingga terminal menampilkan:*
```text
INFO:     Started server process [...]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

#### 2. Terminal 2: Jalankan Streamlit Frontend Analytics UI (Port 8501)
```powershell
# Menggunakan python standar:
python -m streamlit run ui/app.py --server.port 8501

# ATAU jika menggunakan package manager uv:
uv run python -m streamlit run ui/app.py --server.port 8501
```
*Browser default Anda akan otomatis membuka halaman Streamlit, atau Anda bisa mengaksesnya secara manual di:*  
👉 [**http://localhost:8501**](http://localhost:8501)

---

### Opsi B: Menjalankan via Docker Compose (Semua di Background Container)

Jika Anda tidak ingin membuka banyak terminal lokal dan ingin menjalankan semuanya di dalam container Docker:

```powershell
# Jalankan container backend API dan frontend UI
docker compose up -d nl2sql-api analytics-ui
```

*Untuk melihat log aktivitas real-time container:*
```powershell
docker compose logs -f nl2sql-api analytics-ui
```

> [!WARNING]
> **Peringatan Konflik Port (Port Conflict):**  
> Jangan menjalankan **Opsi A** (Terminal Lokal) dan **Opsi B** (Docker) secara bersamaan! Port `8000` dan `8501` akan bentrok (*Address already in use*).  
> Jika container Docker sedang running dan Anda ingin beralih ke terminal lokal, hentikan container terlebih dahulu dengan:  
> `docker compose stop nl2sql-api analytics-ui`

---

### 3. Verifikasi Kesehatan Service (Health Check & API Docs)

Sebelum menguji pertanyaan analitik di browser, pastikan service berjalan normal:

| Endpoint / URL | Deskripsi | Status yang Diharapkan |
| :--- | :--- | :--- |
| [**http://localhost:8000/health**](http://localhost:8000/health) | API Health Check | `{"status":"ok","db_target":"local","provider":"mock",...}` |
| [**http://localhost:8000/docs**](http://localhost:8000/docs) | Swagger API Documentation | Menampilkan endpoint interaktif `/v1/analyze`, `/query`, `/health`, `/schema` |
| [**http://localhost:8501**](http://localhost:8501) | Streamlit Chat Interface | Dashboard Omnichannel Analytics siap digunakan |

---

### 4. Ajukan Pertanyaan Analitik (Sample Prompts)

Buka [**http://localhost:8501**](http://localhost:8501) di browser Anda dan coba ajukan pertanyaan bisnis berikut pada input chat:

- *"Show total net revenue by sales channel"*
- *"Which campaign had the highest ROAS?"*
- *"What is the daily stockout rate?"*
- *"When was the data last refreshed?"*

**Output yang Dihasilkan UI:**
1. **Natural Language Answer:** Penjelasan langsung dalam bahasa alami menjawab pertanyaan analitik Anda.
2. **Interactive Chart (Plotly):** Grafik interaktif (bar chart, line chart) yang memvisualisasikan tren atau perbandingan.
3. **Data Table:** Cuplikan baris data tabular hasil query.
4. **Show SQL & Provenance (Expander):** Klik expander ini untuk menginspeksi:
   - Query SQL terverifikasi (hanya membaca tabel `gold.*`).
   - Query execution duration (milidetik).
   - Timestamp kapan data Gold layer terakhir kali diperbarui (`ops.pipeline_runs`).

---

## Langkah 6: Cara Reset & Rerun Pipeline (Opsional)

Jika Anda ingin menguji ulang eksekusi pipeline dari kondisi database kosong (*fresh start*):

### 1. Hapus Seluruh Skema Pipeline
Jalankan file `reset_schema.sql` via terminal:
```powershell
Get-Content database/neon/reset_schema.sql | docker compose exec -T postgres psql -U retail -d retail_analytics
```
*(Terminal akan menampilkan `DROP SCHEMA` untuk bronze, silver, ops, dan gold).*

### 2. Eksekusi Ulang di Airflow
- Buka kembali Airflow Web UI di `http://localhost:8080`.
- Klik tombol **Trigger DAG (Play)**.
- Task `initialize_schema` akan otomatis membuat ulang seluruh skema dan tabel, lalu memproses data dari awal.

---

## Langkah 7: Menutup & Membersihkan Environment (`docker compose down -v`)

Setelah seluruh proses selesai, presentasi tuntas, atau sesi pengerjaan berakhir, Anda dapat mematikan seluruh container:

### Pilihan A: Mematikan Sementara (Menyimpan Data di Database)
Gunakan perintah ini jika Anda ingin laptop istirahat tapi data di dalam PostgreSQL tetap tersimpan:
```powershell
docker compose down
```

---

### Pilihan B: Bersih Total Termasuk Volume Database (`docker compose down -v`) ⭐
Gunakan flag **`-v`** (*volumes*) untuk menghapus seluruh container sekaligus **menghapus volume data disk lokal** (`retail_postgres_data` dan `retail_airflow_logs`):

```powershell
docker compose down -v
```

> [!NOTE]
> **Mengapa menggunakan `down -v`?**
> - Mengembalikan kapasitas ruang penyimpanan (*disk space*) laptop Anda.
> - Memastikan tidak ada sisa metadata database atau log Airflow yang tertinggal saat project ingin dijalankan dari nol lagi di kemudian hari.

---

## Langkah 8: Checklist Berkas Pengumpulan (Submission)

Sebelum mengumpulkan tugas milestone ke repository GitHub Anda, pastikan berkas-berkas berikut telah siap:

```text
submission/
├── dags/
│   └── dag.py                                <-- Script DAG Airflow 7 tahap
├── database/schemas/
│   ├── bronze.sql                            <-- DDL tabel Bronze JSONB
│   ├── silver.sql                            <-- DDL tabel Silver (Clean & Reject)
│   └── ops.sql                               <-- DDL tabel observabilitas (runs, files, QC)
├── pipelines/
│   ├── bronze/build_bronze.py                <-- Ingestion Bronze & Checksum SHA-256
│   ├── silver/build_silver.py                <-- Normalisasi UTC, SCD2, & Deduplikasi
│   └── gold/build_gold.py                    <-- Pemodelan 5 tabel Gold tanpa fan-out
└── evidence/
    ├── dag_evidence1.PNG                     <-- Screenshot DAG Airflow sukses (semua hijau)
    ├── dag_evidence2.PNG                     <-- Screenshot detail eksekusi task
    └── sample_engine_queries.md              <-- Log query & screenshot hasil Streamlit UI
```

*(Catatan: Folder `data/`, environment `.venv/`, dan file `.env` tidak perlu diunggah ke GitHub sesuai ketentuan `README.md`).*

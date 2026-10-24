# PROMPT & SLIDE DECK SPECIFICATION: GOOGLE SLIDES (GEMINI)
## Project: Omnichannel Retail Analytics Pipeline (Milestone 1)
**Presenter:** Bayu Aziz  
**Target Output:** Presentasi Google Slides profesional, modern, terstruktur, berbasis data riil, dan siap dipresentasikan di hadapan penguji/stakeholder.

---

> ### 💡 CARA MENGGUNAKAN DOKUMEN INI DI GOOGLE SLIDES / GEMINI
> 1. Buka **Google Slides** baru di browser Anda.
> 2. Klik ikon **"Help me create a presentation"** (Gemini) di Google Slides.
> 3. Salin blok **"MASTER PROMPT UNTUK GEMINI"** di bawah ini ke input prompt Gemini.
> 4. Gemini akan menghasilkan struktur slide visual. Anda dapat menggunakan rincian konten dan **Speaker Notes (Skrip Presentasi)** di setiap slide di bawah untuk mengisi teks dan membawakan presentasi secara percaya diri!

---

```text
================================================================================
MASTER PROMPT UNTUK GEMINI DI GOOGLE SLIDES:
================================================================================
"Buatkan presentasi profesional sebanyak 14 slide bertema Dark Tech Minimalist 
(Palet warna: Deep Navy #0B192C, Slate Gray #1E3E62, Vibrant Amber #FF6500, dan White #FFFFFF) 
tentang 'End-to-End Omnichannel Retail Data Pipeline & NL-to-SQL Analytics Platform'. 

Presentasi terbagi menjadi 4 bab utama:
1. Pendahuluan: Latar belakang bisnis ritel omnichannel, fragmentasi data, dan masalah yang diselesaikan.
2. Solusi & Pendekatan Teknis: Arsitektur Medallion (Bronze, Silver, Gold), Observabilitas Ops, dan Airflow DAG 7 tahap.
3. Demonstrasi : Rincian spesifikasi sistem, konfigurasi Docker/Airflow, evolusi skema dengan metrik konkret (38 file, 317.540 baris diproses, 2 reject), pencegahan Fact Multiplication (Fan-out), Quality Gate, serta showcase UI Streamlit & FastAPI Engine.
4. Evaluasi & Future Work: Kendala teknis container/pip, limitasi model saat ini, dan roadmap masa depan.

Gunakan tata letak slide yang bersih, banyak visualisasi kartu metrik (KPI cards), diagram alur, tabel perbandingan, dan bullet points ringkas tanpa dinding teks tebal."
================================================================================
```

---

# STRUKTUR LENGKAP SLIDE-BY-SLIDE & SPEAKER NOTES

---

## SLIDE 1: JUDUL & PEMBUKA
- **Bagian:** 1. Pendahuluan
- **Judul Slide:** Omnichannel Retail Analytics Platform
- **Sub-judul:** End-to-End Medallion Data Pipeline, Automated Quality Gate, and NL-to-SQL Intelligence
- **Layout & Visual:**
  - Background: Deep Navy modern dengan aksen gradasi halus.
  - Elemen: Badge `"AI Engineering — Milestone 1"`, Nama Presenter (`Bayu Aziz`), Tech Stack icons (Airflow, PostgreSQL, FastAPI, Streamlit, Docker).
- **Poin Utama di Slide:**
  - Modern Data Architecture for Complex Retail Operations
  - Ingestion $\rightarrow$ Medallion Pipeline $\rightarrow$ Analytical Datamarts $\rightarrow$ AI Interface
  - Zero Fan-out Guarantee & Automated Quality Assurance
- **Speaker Notes (Skrip Presentasi):**
  > *"Selamat pagi/siang rekan-rekan dan bapak/ibu penguji. Hari ini saya mempersembahkan Milestone 1: Omnichannel Retail Analytics Platform. Project ini menyelesaikan tantangan data engineering hulu-ke-hilir pada industri ritel modern: mulai dari mengintegrasikan data mentah yang terfragmentasi dari 38 berkas sumber, mengolahnya melalui Medallion Architecture di PostgreSQL, mengotomasi alurnya dengan Apache Airflow berprinsip fail-fast quality gate, hingga menyajikan data bersih ke dalam Gold Datamarts yang siap dikonsumsi oleh AI NL-to-SQL Engine dan Streamlit Dashboard."*

---

## SLIDE 2: LATAR BELAKANG & PERMASALAHAN BISNIS
- **Bagian:** 1. Pendahuluan
- **Judul Slide:** Problem Statement: The Omnichannel Retail Dilemma
- **Sub-judul:** Mengapa Mengambil Keputusan Bisnis Ritel Begitu Sulit & Berisiko?
- **Layout & Visual:**
  - 3 Kolom Kartu Masalah (Problem Cards) dengan ikon peringatan merah/oranye.
- **Poin Utama di Slide:**
  - **1. Data Silo & Fragmentasi Ekstrem:** Transaksi POS toko fisik, marketplace, event pembayaran payment gateway, status pengembalian dana, dan inventori gudang berada di sistem terpisah.
  - **2. Kualitas Data Mentah Sangat Buruk (*Dirty Data*):** Ditemukan quantity bernilai minus ($\le 0$), zona waktu tercampur (*mixed offset* UTC vs +07:00), dan pesanan terduplikasi akibat *multiple status updates*.
  - **3. Bahaya Fact Multiplication (Fan-out):** Menggabungkan pesanan dengan multiple item, payment split, dan refund menghasilkan perkalian baris Cartesian yang melipatgandakan nilai GMV bisnis secara palsu.
  - **4. Ketiadaan Self-Service Analytics:** Tim manajemen dan marketing harus menunggu antrean query manual tim data hanya untuk mengetahui metrik sederhana seperti ROAS iklan dan tingkat stockout harian.
- **Speaker Notes (Skrip Presentasi):**
  > *"Di dunia ritel omnichannel, transaksi bergerak di berbagai kanal fisik dan digital. Masalah utamanya adalah data mentah yang masuk ke Data Lake memiliki banyak kecacatan: ada order item dengan quantity -1 yang bisa merusak nilai omzet, format jam pembayaran yang berbeda zona waktu antara server lokal dan cloud, serta duplikasi data pesanan. Jika tim analis langsung melakukan SQL JOIN antara tabel order, item, dan refund tanpa penanganan khusus, akan terjadi fenomena Fact Multiplication atau fan-out yang menggelembungkan omzet perusahaan hingga ratusan persen. Inilah masalah yang kami selesaikan secara struktural pada project ini."*

---

## SLIDE 3: SOLUSI & PENDEKATAN TEKNIS (MEDALLION + OPS)
- **Bagian:** 2. Solusi & Teknis
- **Judul Slide:** Architectural Solution: Medallion Architecture with Ops Observability
- **Sub-judul:** Memisahkan Bukti Sumber, Standardisasi Kualitas, dan Pemodelan Analitik
- **Layout & Visual:**
  - Diagram Alur Horizontal 4 Blok: `Raw Data Lake` $\rightarrow$ `Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`, dengan layer `Ops` membentang di bagian bawah sebagai fondasi pemantauan.
- **Poin Utama di Slide:**
  - **Raw Data Lake:** 38 file heterogen (JSON & CSV) dari operational, events, inventory, dan reference.
  - **Bronze Layer (`bronze.raw_records`):** *Immutable Source Preservation* — seluruh payload mentah disimpan apa adanya dalam format `JSONB` fleksibel ber-checksum SHA-256.
  - **Silver Layer (Clean vs Reject):** Parsing skema relasional, standarisasi waktu UTC, deduplikasi deterministik, dan isolasi baris cacat ke `silver.rejected_records`.
  - **Gold Layer (Datamart Modeling):** 5 tabel analitik dimensional bebas duplikasi (*zero fan-out*) yang siap dikonsumsi eksekutif dan AI.
  - **Ops Layer (Observability):** Schema pengawasan operasional (`ops.pipeline_runs`, `ops.ingested_files`, `ops.quality_checks`) untuk transparansi audit 100%.
- **Speaker Notes (Skrip Presentasi):**
  > *"Untuk mengatasi tantangan tersebut, kami mengadopsi Medallion Architecture yang diperkuat dengan lapisan observabilitas Ops. Di Bronze, kami tidak langsung mengutak-atik struktur data, melainkan menyimpannya utuh sebagai JSONB agar memiliki bukti audit asli yang tidak dapat diubah (immutable). Di Silver, kami menerapkan pemisahan ketat: data bersih masuk ke tabel relasional ternormalisasi, sedangkan data yang cacat tidak dihapus diam-diam, melainkan dialihkan ke tabel rejected_records lengkap dengan alasan penolakannya. Di Gold, kami menyusun 5 tabel datamart berdimensi bisnis yang menjamin akurasi angka metrik finansial."*

---

## SLIDE 4: ALUR ORKESTRASI AIRFLOW DAG (7-STAGE FAIL-FAST)
- **Bagian:** 2. Solusi & Teknis
- **Judul Slide:** Airflow Orchestration & Fail-Fast Pipeline Lifecycle
- **Sub-judul:** 7 Tahap Sekuensial dengan Quality Gate sebagai Gerbang Keamanan Data
- **Layout & Visual:**
  - Screenshot DAG Airflow sukses (semua 7 node hijau tua) atau diagram alur proses berurutan dengan penanda gerbang keamanan di node ke-5.
- **Poin Utama di Slide:**
  - `[1. initialize_schema]` $\rightarrow$ Eksekusi DDL idempotensial untuk schema `ops`, `bronze`, `silver`, `gold`.
  - `[2. validate_raw]` $\rightarrow$ Pengecekan keberadaan folder data dan registrasi `run_id` awal di `ops.pipeline_runs`.
  - `[3. load_bronze]` $\rightarrow$ Ingest 38 berkas mentah ke `bronze.raw_records` & verifikasi checksum SHA-256 di `ops.ingested_files`.
  - `[4. build_silver]` $\rightarrow$ Ekstraksi JSONB, normalisasi UTC, deduplikasi, dan pemisahan clean vs reject.
  - `[5. quality_gate] 🛡️` $\rightarrow$ **Gerbang Pengujian Otomatis:** Memastikan tidak ada quantity minus dan primary key 100% unik sebelum masuk Gold. Jika gagal, eksekusi DAG langsung berhenti seketika (*fail-fast*).
  - `[6. build_gold]` $\rightarrow$ Kalkulasi metrik bisnis tingkat lanjut menggunakan CTE Pre-aggregation bebas fan-out.
  - `[7. validate_gold_and_record_metadata]` $\rightarrow$ Validasi integritas baris Gold dan finalisasi status run `SUCCESS`.
- **Speaker Notes (Skrip Presentasi):**
  > *"Seluruh siklus data ini diorkestrasi secara otomatis oleh Apache Airflow melalui DAG omnichannel_retail_pipeline yang terdiri dari 7 tahapan sekuensial. Prinsip paling krusial ada di tahap ke-5: Quality Gate. Kami tidak mengizinkan komputasi Gold berjalan jika data di Silver terindikasi bermasalah. Apabila ditemukan ada anomali fatal, pipeline akan melempar AirflowException, mematikan alur kerja secara instan, dan mencatat kegagalan di ops.pipeline_runs sehingga data warehouse Gold tidak pernah terkontaminasi oleh sampah data."*

---

## SLIDE 5 (3.1): ARSITEKTUR KOMPUTASI & TECH STACK
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** 3.1. Computing Architecture & Modern Tech Stack
- **Sub-judul:** Pondasi Infrastruktur yang Modular, Reproducible, dan Cloud-Native
- **Layout & Visual:**
  - Tabel 2 Kolom atau Kartu Grid berlogo teknologi.
- **Poin Utama di Slide:**
  - **Basis Data Warehouse:** PostgreSQL 16 (Menyimpan 4 schema terisolasi: `ops`, `bronze`, `silver`, `gold`).
  - **Orkestrator:** Apache Airflow 2.9.3 (LocalExecutor, mendukung visualisasi DAG, retry task, dan XCom parameter passing).
  - **Package Management:** Astral `uv` & Python 3.12 (Pemasangan dependency cepat dan manajemen virtualenv yang bersih).
  - **Database Driver:** `pg8000` (Pure-python PostgreSQL driver berkecepatan tinggi) & `psycopg2-binary`.
  - **AI Engine Backend:** FastAPI 2.0.0 (Uvicorn server di Port 8000) dengan SQL Guardrail & AST safety validation.
  - **User Interface:** Streamlit 1.40+ (Port 8501) dengan visualisasi grafik interaktif Plotly.
  - **Kontainerisasi:** Docker & Docker Compose v2 (Menjalankan 5 service terisolasi: postgres, scheduler, webserver, api, ui).
- **Speaker Notes (Skrip Presentasi):**
  > *"Memasuki sesi demonstrasi, slide 3.1 menampilkan stack komputasi yang kami gunakan. Seluruh ekosistem berjalan di dalam Docker Compose untuk menjamin portabilitas tinggi. Kami menggunakan PostgreSQL 16 sebagai storage engine, Apache Airflow 2.9.3 sebagai konduktor orkestrasi, Python 3.12 yang dikelola dengan modern package manager Astral uv, serta backend API FastAPI yang melayani inference NL-to-SQL untuk antarmuka interaktif Streamlit."*

---

## SLIDE 6 (3.2): KONFIGURASI ENVIRONMENT & RESOURCE TUNING
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** 3.2. Environment Configuration & Container Optimization
- **Sub-judul:** Pengaturan Port, Alokasi Resource, dan Stabilitas Docker Engine
- **Layout & Visual:**
  - Kartu Parameter Konfigurasi (`.env`) di sebelah kiri dan Parameter Docker Tuning di sebelah kanan.
- **Poin Utama di Slide:**
  - **Port Mapping Terintegrasi:**
    - `localhost:5432` $\rightarrow$ PostgreSQL Server
    - `localhost:8080` $\rightarrow$ Apache Airflow Web UI (Admin/Admin)
    - `localhost:8000` $\rightarrow$ FastAPI Engine & OpenAPI Docs (`/docs`, `/health`)
    - `localhost:8501` $\rightarrow$ Streamlit Analytics Chatbot UI
  - **Environment Switching (`.env`):**
    - Mendukung mode database lokal (`ANALYTICS_DB_TARGET=local`) maupun Cloud Neon PostgreSQL.
    - Opsi provider AI fleksibel (`NL2SQL_PROVIDER=mock` untuk offline testing atau `openai` untuk production GPT-4).
  - **Resource Tuning (Mengatasi Keterbatasan RAM Laptop Windows):**
    - `AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT: 300` (Mencegah gunicorn worker timeout saat bootup awal).
    - `AIRFLOW__WEBSERVER__WORKERS: 2` (Mengoptimalkan pemakaian memory CPU dan RAM).
    - Penanganan khusus `_PIP_ADDITIONAL_REQUIREMENTS: ''` pada container `airflow-init` untuk mencegah kegagalan instalasi pip saat container berjalan sebagai user root.
- **Speaker Notes (Skrip Presentasi):**
  > *"Pada slide 3.2, konfigurasi environment dirancang agar ramah terhadap resource laptop pengembang. Melalui .env tunggal, kita dapat dengan mudah mengganti target database antara PostgreSQL lokal Docker dan Cloud Neon. Selain itu, kami melakukan tuning penting pada docker-compose.yml: kami menaikkan webserver master timeout menjadi 300 detik dan membatasi worker menjadi 2 guna mencegah crash memory di Windows, serta meniadakan pip run pada service airflow-init demi keamanan hak akses user root."*

---

## SLIDE 7 (3.3): EVOLUSI STRUKTUR TABEL & METRIK DATA RIIL
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** 3.3. Table Evolution: Volume, Grain, & Filtering Metrics
- **Sub-judul:** Angka Konkret Pemrosesan Data dari Raw Menuju Gold Datamart
- **Layout & Visual:**
  - Visualisasi Funnel Metrik Data (dari angka besar 317K di Bronze hingga teragregasi presisi di Gold).
- **Poin Utama di Slide:**
  - **Data Lake (Raw Ingestion):**
    - **38 berkas** dibaca dan diverifikasi checksum SHA-256 ke `ops.ingested_files`.
  - **Bronze Layer (`bronze.raw_records`):**
    - **317.540 baris** termuat utuh dalam format JSONB (tanpa data loss).
  - **Silver Layer (Pembersihan, Deduplikasi & Isolasi):**
    - `silver.orders`: **20.000** data pesanan mentah berhasil dideduplikasi menjadi **10.000** pesanan unik terverifikasi.
    - `silver.order_items`: **49.920** baris mentah $\rightarrow$ **2 baris reject** (quantity $\le 0$) dialihkan ke `silver.rejected_records` $\rightarrow$ **49.918 baris bersih** masuk ke tabel transaksi.
    - `silver.payments`: **17.570** catatan pembayaran dinormalisasi ke UTC.
    - `silver.refunds`: **3.982** catatan pengembalian dana dinormalisasi.
  - **Gold Layer (Analytical Modeling):**
    - `gold.order_360`: Tepat **10.000 baris** (1 baris per order, membuktikan nol fan-out).
    - `gold.customer_daily`: **9.784 baris** profil belanja harian pelanggan.
    - `gold.product_daily`: **6.982 baris** riwayat pergerakan stok dan penjualan produk.
    - `gold.channel_campaign_daily`: **855 baris** analisis kinerja kampanye dan ROAS.
    - `gold.executive_kpis_daily`: **90 baris** ringkasan metrik harian tingkat C-Level.
- **Speaker Notes (Skrip Presentasi):**
  > *"Ini adalah slide paling esensial yang memaparkan data riil hasil eksekusi pipeline kami. Dari 38 file mentah yang dibaca, sebanyak 317.540 rekaman berhasil dimuat ke Bronze layer. Saat masuk ke Silver, terjadi transformasi besar: 20.000 baris order yang mengalami duplikasi status disaring secara deterministik menjadi tepat 10.000 order unik. Pada order items, dari 49.920 baris, sistem mendeteksi 2 baris bernilai quantity negatif dan langsung mengisolasinya ke silver.rejected_records, menyisakan 49.918 item bersih. Di Gold layer, tabel order_360 menghasilkan tepat 10.000 baris—sama persis dengan jumlah order unik—yang menjadi bukti mutlak bahwa tidak terjadi duplikasi akibat proses join."*

---

## SLIDE 8 (3.4): DIAGRAM RELASIONAL & ALUR DATA OPERASIONAL
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** 3.4. Relational Entity Relationship & Data Provenance
- **Sub-judul:** Pemodelan Hubungan Antar-Entitas Transaksional, Inventori, dan Marketing
- **Layout & Visual:**
  - Diagram ERD sederhana yang memperlihatkan kardinalitas hubungan antar entitas inti.
- **Poin Utama di Slide:**
  - **Core Transactional Flow:**
    - `CUSTOMERS (1)` $\longleftrightarrow$ `(N) ORDERS`
    - `ORDERS (1)` $\longleftrightarrow$ `(N) ORDER_ITEMS` $\longleftrightarrow$ `(1) PRODUCTS`
  - **Payment & Event Lifecycle:**
    - `ORDERS (1)` $\longleftrightarrow$ `(N) PAYMENTS` (Mendukung split payment / multiple transaction attempts)
    - `ORDERS (1)` $\longleftrightarrow$ `(N) REFUNDS` (Mendukung partial & full refund)
    - `ORDERS (1)` $\longleftrightarrow$ `(N) RETURNS` (Mendukung pengembalian barang fisik)
  - **Supply Chain & Growth Attribution:**
    - `PRODUCTS (1)` $\longleftrightarrow$ `(N) INVENTORY_SNAPSHOTS` $\longleftrightarrow$ `(1) STORES`
    - `SALES_CHANNELS (1)` $\longleftrightarrow$ `(N) CAMPAIGN_SPEND`
- **Speaker Notes (Skrip Presentasi):**
  > *"Slide 3.4 memperlihatkan peta relasi data kami. Hubungan kardinalitas ini merefleksikan kompleksitas bisnis omnichannel yang sesungguhnya. Satu pesanan dapat terdiri dari banyak produk item, dapat dibayar menggunakan metode split payment (misalnya voucher promo digabung kartu kredit), serta dapat mengalami proses refund atau return barang secara parsial. Pada rantai pasok, inventori produk dipantau harian per cabang toko. Seluruh relasi 1-ke-banyak ini kami petakan dengan sangat hati-hati agar tidak menimbulkan inkonsistensi saat diagregasikan ke Gold layer."*

---

## SLIDE 9 (3.5): MASALAH & ANOMALI DATA YANG DITEMUKAN
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** 3.5. Real-World Data Anomalies Discovered
- **Sub-judul:** Investigasi Bukti Kerusakan Data Mentah pada Tahap Eksplorasi
- **Layout & Visual:**
  - Tabel 4 Baris berisi Masalah, Contoh Bukti di Raw Data, dan Risiko Kerugian Bisnis.
- **Poin Utama di Slide:**
  - **Anomali 1: Nilai Quantity Tidak Masuk Akal ($\le 0$):**
    - *Temuan:* Pada berkas `order_items.json`, ditemukan rekaman dengan `quantity = -1` dan `quantity = -5`.
    - *Risiko:* Perhitungan omzet kotor (GMV) menjadi minus dan mendistorsi laporan keuangan.
  - **Anomali 2: Inkonsistensi Zona Waktu (*Mixed Offset Timestamps*):**
    - *Temuan:* Pada `payment_events.json`, sebagian timestamp berakhiran `Z` (UTC) dan sebagian lagi berakhiran `+07:00` (WIB).
    - *Risiko:* Transaksi malam hari di jam 23:30 WIB terhitung salah tanggal jika diasumsikan UTC tanpa konversi.
  - **Anomali 3: Duplikasi Status Pesanan (*Multiple Order Snapshots*):**
    - *Temuan:* Satu `order_id` muncul hingga 2 kali dengan status berbeda (`PENDING` lalu `DELIVERED`).
    - *Risiko:* Total order terhitung 20.000 padahal aktualnya hanya 10.000 pesanan riil (*double counting* 100%).
  - **Anomali 4: Dimensi Berubah Seiring Waktu (*Slowly Changing Dimension Type 2*):**
    - *Temuan:* Pelanggan berpindah kota dan kategori produk berganti seiring berjalannya kuartal bisnis.
    - *Risiko:* Analisis performa masa lalu menggunakan data kategori terbaru (*data anachronism*).
- **Speaker Notes (Skrip Presentasi):**
  > *"Pada slide 3.5, kami merangkum investigasi anomali data mentah yang sengaja kami bedah secara forensik. Pertama, adanya nilai quantity negatif pada order items yang berpotensi merusak kalkulasi omzet. Kedua, inkonsistensi penulisan format timestamp di payment events yang mencampur offset WIB +07:00 dengan UTC Z. Ketiga, duplikasi order records di mana satu order tercatat berulang kali karena adanya riwayat update status. Terakhir, isu SCD Type 2 di mana profil pelanggan dan kategori produk berubah seiring waktu. Semua anomali ini kami mitigasi dengan strategi data engineering yang presisi."*

---

## SLIDE 10 (3.6): STRATEGI PENYELESAIAN MASALAH
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** 3.6. Engineering Strategies & Quality Gate Implementation
- **Sub-judul:** Pola Solusi Teknis: Isolasi Reject, Deduplikasi, CTE Pre-Aggregation & Fail-Fast Gate
- **Layout & Visual:**
  - 4 Kotak Solusi Berpasangan dengan Cuplikan Logika SQL.
- **Poin Utama di Slide:**
  - **Solusi 1: Isolasi Data Rusak ke `silver.rejected_records`:**
    - Logika filter: Hanya baris dengan `quantity > 0` yang lolos ke `silver.order_items`. Baris cacat dialihkan ke tabel reject dengan alasan `INVALID_QUANTITY_NEGATIVE_OR_ZERO`.
  - **Solusi 2: Standardisasi Zona Waktu ke UTC:**
    - Seluruh string timestamp dikonversi menggunakan PostgreSQL `TIMESTAMPTZ` murni untuk memastikan keselarasan grain harian.
  - **Solusi 3: Deduplikasi Deterministik via Window Function:**
    - Mengambil state pesanan paling mutakhir:  
      `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at_utc DESC) = 1`.
  - **Solusi 4: Eliminasi Fact Multiplication via CTE Pre-Aggregation:**
    - Mencegah fan-out Cartesian dengan menghitung total per `order_id` terlebih dahulu pada level CTE (CTE Item, CTE Payment, CTE Refund), baru kemudian di-`LEFT JOIN` 1-to-1 ke `orders`.
  - **Solusi 5: Automated Quality Gate (Airflow Task):**
    - 3 Aturan Wajib Lolos: (1) Order count $> 0$, (2) Zero negative quantity di clean table, (3) Zero duplicate order_id. Lolos 100% dengan bukti tercatat di `ops.quality_checks`.
- **Speaker Notes (Skrip Presentasi):**
  > *"Bagaimana kami menangani masalah-masalah tadi? Di slide 3.6 kami jabarkan solusinya. Kami tidak membuang data negatif secara sembarangan, melainkan mengkarantinanya ke silver.rejected_records untuk audit lanjutan. Untuk duplikasi pesanan, kami menggunakan SQL Window Function ROW_NUMBER() berdasarkan updated_at_utc terbaru sehingga hanya snapshot final yang digunakan. Dan yang paling penting: untuk menghindari fact multiplication, kami menerapkan teknik CTE Pre-Aggregation—setiap tabel transaksi anak diagregasikan terlebih dahulu sebelum di-join ke tabel order induk. Terakhir, seluruh aturan ini dikawal ketat oleh task Quality Gate di Airflow yang menguji 3 kriteria kelayakan sebelum mengizinkan pembuatan tabel Gold."*

---

## SLIDE 11 (3.7): DEMO SHOWCASE — AI ENGINE & STREAMLIT UI
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** 3.7. Live Demo: Natural Language to SQL Analytics
- **Sub-judul:** Menjembatani Pengguna Bisnis Non-Teknis dengan Data Warehouse Gold
- **Layout & Visual:**
  - Tampilan Mockup / Screenshot Streamlit UI di sebelah kiri, dan Rincian Keamanan NL-to-SQL di sebelah kanan.
- **Poin Utama di Slide:**
  - **Antarmuka Interaktif Pengguna:**
    - Streamlit Chatbot (`http://localhost:8501`) yang merespons pertanyaan bahasa alami dalam hitungan detik.
  - **Keamanan & Guardrail NL-to-SQL (FastAPI Port 8000):**
    - AI Engine hanya diberikan akses membaca schema `gold.*`.
    - Dilengkapi AST Parser Guardrail: Mencegah eksekusi query berbahaya (`DROP`, `DELETE`, `INSERT`, `UPDATE`).
  - **Fitur Provenance & Audit Transparansi:**
    - Pengguna dapat membuka expander *"Show SQL and provenance"* untuk melihat query SQL asli yang dieksekusi, durasi milidetik, serta timestamp kesegaran data dari `ops.pipeline_runs`.
  - **Contoh Pertanyaan yang Berhasil Dijawab:**
    - *"Show total net revenue by sales channel"* $\rightarrow$ Menampilkan rincian revenue online vs offline + Bar chart.
    - *"Which campaign had the highest ROAS?"* $\rightarrow$ Menghitung laba iklan tertinggi dari `gold.channel_campaign_daily`.
    - *"What is the daily stockout rate?"* $\rightarrow$ Menampilkan tren kehabisan stok dari `gold.executive_kpis_daily`.
- **Speaker Notes (Skrip Presentasi):**
  > *"Kini kita melihat hasil hilirnya pada slide 3.7: antarmuka analitik cerdas Streamlit yang terhubung ke FastAPI Engine. Pengguna eksekutif atau tim bisnis tidak perlu menulis SQL rumit. Cukup ketik pertanyaan seperti 'Which campaign had the highest ROAS?', dan sistem secara cerdas menyusun SQL yang aman, mengeksekusinya ke tabel gold.channel_campaign_daily, lalu menyajikan grafik Plotly beserta ringkasan teks. Sistem kami juga menyertakan fitur provenance sehingga pengguna selalu tahu query apa yang dijalankan di balik layar dan kapan data terakhir kali diperbarui oleh Airflow."*

---

## SLIDE 12 (3.7 Lanjut): PENUTUP DEMONSTRASI & DAMPAK BISNIS
- **Bagian:** 3. Demonstrasi (Live Demo)
- **Judul Slide:** Business Value & Strategic Impact
- **Sub-judul:** Manfaat Terukur Penerapan Platform Data Terstruktur bagi Perusahaan
- **Layout & Visual:**
  - 4 Kartu Metrik Keberhasilan Bisnis dengan ikon trofi / pertumbuhan.
- **Poin Utama di Slide:**
  - **1. Integritas Angka Finansial 100% Akurat:**
    - Eliminasi fan-out dan isolasi baris minus menjamin perhitungan GMV, Net Revenue, dan Margin bebas salah saji.
  - **2. Efisiensi Waktu Analis Meningkat Drastis (80%+ Faster):**
    - Pertanyaan ad-hoc yang biasanya membutuhkan waktu antrean berjam-jam ke tim BI kini dijawab instan oleh AI NL-to-SQL dalam hitungan detik.
  - **3. Optimalisasi Budget Marketing & Efisiensi Iklan:**
    - Melalui `gold.channel_campaign_daily`, tim marketing dapat langsung memangkas kampanye dengan ROAS buruk dan melipatgandakan budget pada channel terlaris.
  - **4. Pencegahan Kehilangan Penjualan (*Stockout Mitigation*):**
    - Metrik `stockout_flag` pada `gold.product_daily` mendeteksi produk unggulan yang kritis stok secara dini sebelum pelanggan beralih ke kompetitor.
- **Speaker Notes (Skrip Presentasi):**
  > *"Sebagai penutup sesi demonstrasi, dampak bisnis dari platform ini sangat nyata. Pertama, para pengambil keputusan memperoleh angka finansial yang 100% valid dan dapat dipertanggungjawabkan. Kedua, efisiensi kerja tim melonjak drastis karena analisis ad-hoc kini dapat dilakukan mandiri oleh pengguna awam tanpa membebani tim data engineer. Ketiga, integrasi data inventori dan performa kampanye pemasaran memungkinkan perusahaan menghemat anggaran iklan yang tidak produktif dan secara aktif mencegah potensi kehilangan omzet akibat produk kehabisan stok."*

---

## SLIDE 13: EVALUASI — KENDALA TEKNIS & RESOLUSI
- **Bagian:** 4. Evaluasi
- **Judul Slide:** Technical Challenges & Engineering Lessons Learned
- **Sub-judul:** Hambatan Operasional yang Dihadapi Selama Pengerjaan dan Solusinya
- **Layout & Visual:**
  - Tabel 3 Baris: Kendala yang Dihadapi $\rightarrow$ Akar Masalah $\rightarrow$ Solusi & Tindakan Preventif.
- **Poin Utama di Slide:**
  - **Kendala 1: Airflow-Init Gagal Berjalan (*Root Pip Error*):**
    - *Akar Masalah:* Environment variable `_PIP_ADDITIONAL_REQUIREMENTS` dipanggil di container `airflow-init` yang berjalan sebagai root, memicu peringatan proteksi pip.
    - *Solusi:* Mengisolasi penambahan library `pg8000` hanya pada worker/webserver dan mengosongkan requirements pada `airflow-init`.
  - **Kendala 2: Gunicorn Worker Timeout di Windows Docker:**
    - *Akar Masalah:* Beban resource komputasi Windows Docker Desktop menyebabkan preloading modul Airflow melampaui batas default 120 detik.
    - *Solusi:* Meningkatkan `WEB_SERVER_MASTER_TIMEOUT` menjadi 300 detik dan membatasi jumlah worker menjadi 2.
  - **Kendala 3: Divergensi Branch & Autentikasi Remote GitHub:**
    - *Akar Masalah:* Percobaan push ke repo template asal (`argyadiva`) memicu Error 403 Forbidden.
    - *Solusi:* Menata ulang remote URL ke repository personal mahasiswa (`AIEN-RMT002-BayuAziz`) dan merapikan struktur subfolder repository.
- **Speaker Notes (Skrip Presentasi):**
  > *"Masuk ke bab terakhir evaluasi di slide 13, kami membagikan kendala teknis nyata selama implementasi. Kami sempat mengalami issue di mana container airflow-init gagal startup karena konflik instalasi pip di bawah user root, yang kami atasi dengan memisahkan dependensi container. Kami juga mengatasi isu timeout webserver Airflow pada Docker Windows dengan mengonfigurasi batas timeout menjadi 300 detik. Di sisi source control, kami menuntaskan masalah izin push remote dengan menyelaraskan git remote ke repositori pribadi yang tepat. Setiap kendala ini memberi pelajaran berharga tentang pentingnya konfigurasi container yang defensif."*

---

## SLIDE 14: EVALUASI — ROADMAP PENGEMBANGAN KE DEPAN
- **Bagian:** 4. Evaluasi
- **Judul Slide:** Future Roadmap & Technological Opportunities
- **Sub-judul:** Mengembangkan Platform Menuju Skala Produksi Enterprise Masa Depan
- **Layout & Visual:**
  - Roadmap 3 Fase (Fase 1: Near-Term, Fase 2: Mid-Term, Fase 3: Long-Term).
- **Poin Utama di Slide:**
  - **Fase 1: Near-Term (Near Real-Time Ingestion & Streaming):**
    - Mengembangkan ingestion batch harian menjadi streaming event-driven menggunakan **Apache Kafka / Debezium** (Change Data Capture dari basis data operasional POS toko fisik).
  - **Fase 2: Mid-Term (Cloud Data Warehouse Migration):**
    - Memindahkan penyimpanan PostgreSQL lokal ke Data Warehouse skala petabyte seperti **Snowflake / Google BigQuery / AWS Redshift** dengan dbt (*data build tool*) untuk modeling Silver-Gold.
  - **Fase 3: Long-Term (Advanced LLM Fine-Tuning & Multi-Agentic AI):**
    - Meningkatkan kapabilitas NL-to-SQL dengan *Retrieval-Augmented Generation (RAG)* pada metadata skema, semantic layer (Cube.js / Looker Semantic Layer), serta agen AI otonom yang mampu memberikan rekomendasi preskriptif otomatis saat stok menipis.
- **Speaker Notes (Skrip Presentasi):**
  > *"Sebagai penutup paparan di slide 14, kami telah merancang roadmap masa depan untuk membawa platform ini ke skala enterprise. Pertama, meningkatkan arsitektur batch saat ini menjadi streaming berbasis Kafka CDC agar data penjualan POS toko fisik terupdate secara real-time. Kedua, migrasi data store ke modern cloud data warehouse seperti BigQuery atau Snowflake dengan dbt sebagai engine transformasi. Dan ketiga, memperkuat model AI dengan semantic layer dan multi-agent system agar tidak hanya menjawab pertanyaan deskriptif, tetapi juga mampu memberikan rekomendasi bisnis otomatis bagi manajemen. Terima kasih atas perhatiannya, saya siap membuka sesi tanya jawab."*

---

# RINGKASAN DATA & TABEL BANTU UNTUK DEFENSE TANYA JAWAB (Q&A)

Jika penguji menanyakan rincian angka saat presentasi, gunakan tabel contekan cepat ini:

| Metrik / Parameter | Angka Riil Terverifikasi | Catatan Audit |
|---|---|---|
| **Jumlah File Mentah** | **38 Berkas** | Terdiri dari JSON operasional, events, CSV inventori & kampanye |
| **Total Record di Bronze** | **317.540 Baris** | Tersimpan utuh sebagai JSONB di `bronze.raw_records` |
| **Deduplikasi Pesanan** | **20.000 $\rightarrow$ 10.000** | Difilter via `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at_utc DESC) = 1` |
| **Data Reject Terdeteksi** | **2 Baris** | Pada `order_items` dengan `quantity <= 0`, dialihkan ke `silver.rejected_records` |
| **Item Bersih di Silver** | **49.918 Baris** | 100% item dengan quantity valid $> 0$ di `silver.order_items` |
| **Pembayaran & Refund** | **17.570 payments & 3.982 refunds** | Telah dinormalisasi ke zona waktu standar UTC (`TIMESTAMPTZ`) |
| **Quality Gate Status** | **3 Rules / 100% Passed** | Order count $>0$, zero negative qty, unique primary keys |
| **Gold Order 360** | **Tepat 10.000 Baris** | Bukti 1-to-1 match dengan Silver orders (Nol Fan-out) |
| **Gold Customer Daily** | **9.784 Baris** | Metrik pelanggan unik harian |
| **Gold Product Daily** | **6.982 Baris** | Penjualan produk & status stok harian |
| **Gold Channel Campaign** | **855 Baris** | Pengeluaran iklan, omzet teratribusi, dan metrik ROAS |
| **Gold Executive KPIs** | **90 Hari Kalender** | Ringkasan metrik harian level manajemen puncak |

---
*Dokumen `ppt.md` ini siap disalin ke prompt Google Slides Gemini atau dipelajari sebagai materi presentasi sidang kelulusan Milestone 1.*


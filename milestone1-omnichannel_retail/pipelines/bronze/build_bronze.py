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
    print(f"[{run_id}] Bronze Ingestion Dimulai...")
    
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
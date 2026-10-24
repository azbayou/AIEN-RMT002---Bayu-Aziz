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
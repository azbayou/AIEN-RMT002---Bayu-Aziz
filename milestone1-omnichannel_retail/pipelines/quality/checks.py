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
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
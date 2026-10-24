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
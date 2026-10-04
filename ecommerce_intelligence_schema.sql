-- =====================================================================
-- E-commerce Profitability and Growth Intelligence Platform
-- Complete database script for PostgreSQL 18
-- Database: ecommerce_intelligence
--
-- BEFORE RUNNING
-- 1. Create the database ONCE, while connected to the "postgres" database.
--    This statement must be run alone (not inside this script):
--
--        CREATE DATABASE ecommerce_intelligence;
--
-- 2. Connect your VS Code editor to "ecommerce_intelligence".
--    Check that the line above the editor says ecommerce_intelligence.
--
-- 3. Select all (Ctrl+A) and run. Every statement uses IF NOT EXISTS,
--    so running this script again is safe.
--
-- Final result: 5 schemas, 24 tables
--   raw 1 | staging 1 | core 16 | analytics 4 | growth 2
-- =====================================================================


-- ---------------------------------------------------------------------
-- PART 1: SCHEMAS
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS growth;


-- ---------------------------------------------------------------------
-- PART 2: PIPELINE TABLES (raw and staging)
-- business_id is added in Part 4, after core.business exists.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.raw_data (
    raw_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source      TEXT        NOT NULL,
    data_type   TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_raw_source CHECK (
        source IN ('shopify', 'daraz', 'meta_ads', 'courier', 'payment_gateway', 'inventory')
    )
);

CREATE TABLE IF NOT EXISTS staging.staging_data (
    staging_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_id       BIGINT      NOT NULL
                 REFERENCES raw.raw_data (raw_id),
    source       TEXT        NOT NULL,
    data_type    TEXT        NOT NULL,
    data         JSONB       NOT NULL,
    imported_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_staging_source CHECK (
        source IN ('shopify', 'daraz', 'meta_ads', 'courier', 'payment_gateway', 'inventory')
    )
);

CREATE INDEX IF NOT EXISTS idx_staging_raw_id
    ON staging.staging_data (raw_id);


-- ---------------------------------------------------------------------
-- PART 3: CORE TABLES
-- ---------------------------------------------------------------------

-- 3.1 business, category, product
CREATE TABLE IF NOT EXISTS core.business (
    business_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_name  TEXT        NOT NULL,
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_business_name UNIQUE (business_name)
);

CREATE TABLE IF NOT EXISTS core.category (
    category_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id    BIGINT NOT NULL REFERENCES core.business (business_id),
    category_name  TEXT   NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_category_per_business UNIQUE (business_id, category_name)
);

CREATE TABLE IF NOT EXISTS core.product (
    product_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id         BIGINT NOT NULL REFERENCES core.business (business_id),
    category_id         BIGINT REFERENCES core.category (category_id),
    external_product_id TEXT   NOT NULL,
    sku                 TEXT,
    product_name        TEXT   NOT NULL,
    unit_cost           NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (unit_cost >= 0),
    selling_price       NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (selling_price >= 0),
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_product_external UNIQUE (business_id, external_product_id)
);

-- 3.2 customer, orders, order_item
CREATE TABLE IF NOT EXISTS core.customer (
    customer_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id          BIGINT NOT NULL REFERENCES core.business (business_id),
    external_customer_id TEXT,
    full_name            TEXT,
    phone                TEXT NOT NULL,
    email                TEXT,
    city                 TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_customer_phone UNIQUE (business_id, phone),
    CONSTRAINT chk_customer_phone CHECK (phone ~ '^\+92[0-9]{10}$')
);

CREATE TABLE IF NOT EXISTS core.orders (
    order_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id       BIGINT NOT NULL REFERENCES core.business (business_id),
    customer_id       BIGINT NOT NULL REFERENCES core.customer (customer_id),
    external_order_id TEXT   NOT NULL,
    order_date        TIMESTAMPTZ NOT NULL,
    order_status      TEXT   NOT NULL,
    payment_method    TEXT   NOT NULL,
    subtotal_amount   NUMERIC(12,2) NOT NULL CHECK (subtotal_amount >= 0),
    discount_amount   NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    shipping_charged  NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (shipping_charged >= 0),
    total_amount      NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    currency          CHAR(3) NOT NULL DEFAULT 'PKR',
    shipping_city     TEXT,
    shipping_province TEXT,
    source_platform   TEXT NOT NULL DEFAULT 'shopify',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_order_external UNIQUE (business_id, external_order_id),
    CONSTRAINT uq_orders_order_business UNIQUE (order_id, business_id),
    CONSTRAINT chk_order_status CHECK (
        order_status IN ('pending','confirmed','fulfilled','delivered','cancelled','returned','rto')
    ),
    CONSTRAINT chk_payment_method CHECK (
        payment_method IN ('cod','card','bank_transfer','wallet','other')
    )
);

CREATE TABLE IF NOT EXISTS core.order_item (
    order_item_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id       BIGINT NOT NULL REFERENCES core.business (business_id),
    order_id          BIGINT NOT NULL REFERENCES core.orders (order_id),
    product_id        BIGINT NOT NULL REFERENCES core.product (product_id),
    quantity          INTEGER NOT NULL CHECK (quantity > 0),
    unit_price        NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    discount_amount   NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    unit_cost_at_sale NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (unit_cost_at_sale >= 0),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON core.orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date     ON core.orders (business_id, order_date);
CREATE INDEX IF NOT EXISTS idx_item_order      ON core.order_item (order_id);
CREATE INDEX IF NOT EXISTS idx_item_product    ON core.order_item (product_id);

-- 3.3 courier, shipment, payment, rto_return
CREATE TABLE IF NOT EXISTS core.courier (
    courier_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id   BIGINT NOT NULL REFERENCES core.business (business_id),
    courier_name  TEXT   NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_courier_per_business UNIQUE (business_id, courier_name)
);

CREATE TABLE IF NOT EXISTS core.shipment (
    shipment_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id        BIGINT NOT NULL REFERENCES core.business (business_id),
    order_id           BIGINT NOT NULL REFERENCES core.orders (order_id),
    courier_id         BIGINT NOT NULL REFERENCES core.courier (courier_id),
    tracking_number    TEXT   NOT NULL,
    shipment_status    TEXT   NOT NULL,
    shipped_at         TIMESTAMPTZ,
    delivered_at       TIMESTAMPTZ,
    shipping_cost      NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (shipping_cost >= 0),
    cod_amount         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (cod_amount >= 0),
    delivery_attempts  INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_shipment_tracking UNIQUE (business_id, courier_id, tracking_number),
    CONSTRAINT chk_shipment_status CHECK (
        shipment_status IN ('booked','in_transit','delivered','failed_delivery','returning','returned')
    ),
    CONSTRAINT chk_shipment_dates CHECK (
        shipped_at IS NULL OR delivered_at IS NULL OR delivered_at >= shipped_at
    )
);

CREATE TABLE IF NOT EXISTS core.payment (
    payment_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id         BIGINT NOT NULL REFERENCES core.business (business_id),
    order_id            BIGINT NOT NULL REFERENCES core.orders (order_id),
    external_payment_id TEXT,
    payment_method      TEXT   NOT NULL,
    payment_type        TEXT   NOT NULL DEFAULT 'payment',
    payment_status      TEXT   NOT NULL,
    amount              NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    gateway_fee         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (gateway_fee >= 0),
    paid_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    provider            TEXT,
    provider_account_identifier TEXT,
    currency            CHAR(3),
    original_payment_id BIGINT,
    CONSTRAINT uq_payment_external UNIQUE (business_id, external_payment_id),
    CONSTRAINT chk_pay_method CHECK (
        payment_method IN ('cod','card','bank_transfer','wallet','other')
    ),
    CONSTRAINT chk_pay_type CHECK (payment_type IN ('payment','refund')),
    CONSTRAINT chk_pay_status CHECK (
        payment_status IN ('pending','paid','failed')
    ),
    CONSTRAINT chk_payment_currency_format
        CHECK (currency IS NULL OR currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_payment_original_is_refund
        CHECK (original_payment_id IS NULL OR payment_type = 'refund'),
    CONSTRAINT uq_payment_id_business UNIQUE (payment_id, business_id),
    CONSTRAINT fk_payment_order_business
        FOREIGN KEY (order_id, business_id)
        REFERENCES core.orders (order_id, business_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_payment_original_business
        FOREIGN KEY (original_payment_id, business_id)
        REFERENCES core.payment (payment_id, business_id)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS core.payment_settlement (
    settlement_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id BIGINT NOT NULL
        REFERENCES core.business (business_id)
        ON DELETE RESTRICT,
    provider TEXT NOT NULL,
    provider_account_identifier TEXT,
    external_settlement_id TEXT,
    currency CHAR(3) NOT NULL,
    gross_amount NUMERIC(20,6),
    fee_amount NUMERIC(20,6),
    adjustment_amount NUMERIC(20,6),
    net_amount NUMERIC(20,6),
    status TEXT NOT NULL,
    settled_at TIMESTAMPTZ,
    expected_arrival_at TIMESTAMPTZ,
    provider_status TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_payment_settlement_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_payment_settlement_status
        CHECK (status IN ('pending', 'settled', 'failed', 'cancelled', 'unknown')),
    CONSTRAINT chk_payment_settlement_gross
        CHECK (gross_amount IS NULL OR gross_amount >= 0),
    CONSTRAINT chk_payment_settlement_fee
        CHECK (fee_amount IS NULL OR fee_amount >= 0),
    CONSTRAINT uq_payment_settlement_business
        UNIQUE (settlement_id, business_id)
);

CREATE TABLE IF NOT EXISTS core.payment_settlement_entry (
    settlement_entry_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id BIGINT NOT NULL,
    settlement_id BIGINT NOT NULL,
    payment_id BIGINT,
    external_entry_id TEXT,
    external_transaction_id TEXT,
    entry_type TEXT NOT NULL,
    signed_amount NUMERIC(20,6) NOT NULL,
    currency CHAR(3) NOT NULL,
    provider_status TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_payment_settlement_entry_type
        CHECK (entry_type IN ('payment', 'refund', 'fee', 'adjustment', 'reserve', 'other')),
    CONSTRAINT chk_payment_settlement_entry_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT fk_settlement_entry_settlement_business
        FOREIGN KEY (settlement_id, business_id)
        REFERENCES core.payment_settlement (settlement_id, business_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_settlement_entry_payment_business
        FOREIGN KEY (payment_id, business_id)
        REFERENCES core.payment (payment_id, business_id)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS core.rto_return (
    return_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id          BIGINT NOT NULL REFERENCES core.business (business_id),
    order_id             BIGINT NOT NULL REFERENCES core.orders (order_id),
    shipment_id          BIGINT REFERENCES core.shipment (shipment_id),
    return_type          TEXT   NOT NULL,
    return_reason        TEXT,
    return_date          TIMESTAMPTZ NOT NULL,
    return_shipping_cost NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (return_shipping_cost >= 0),
    refund_amount        NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (refund_amount >= 0),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_return_type CHECK (return_type IN ('rto','customer_return'))
);

CREATE INDEX IF NOT EXISTS idx_shipment_order  ON core.shipment (order_id);
CREATE INDEX IF NOT EXISTS idx_payment_order   ON core.payment (order_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_provider_account_external
    ON core.payment (
        business_id,
        provider,
        provider_account_identifier,
        external_payment_id
    )
    WHERE provider IS NOT NULL
      AND provider_account_identifier IS NOT NULL
      AND external_payment_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_settlement_provider_external
    ON core.payment_settlement (
        business_id,
        provider,
        provider_account_identifier,
        external_settlement_id
    )
    WHERE provider_account_identifier IS NOT NULL
      AND external_settlement_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_settlement_entry_external_id
    ON core.payment_settlement_entry (
        business_id,
        settlement_id,
        external_entry_id
    )
    WHERE external_entry_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_return_order    ON core.rto_return (order_id);
CREATE INDEX IF NOT EXISTS idx_return_shipment ON core.rto_return (shipment_id);

-- 3.4 inventory
CREATE TABLE IF NOT EXISTS core.inventory (
    inventory_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id        BIGINT NOT NULL REFERENCES core.business (business_id),
    product_id         BIGINT NOT NULL REFERENCES core.product (product_id),
    location_name      TEXT   NOT NULL,
    snapshot_date      DATE   NOT NULL,
    quantity_available INTEGER NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_inventory_snapshot
        UNIQUE (business_id, product_id, location_name, snapshot_date),
    CONSTRAINT chk_inventory_location CHECK (btrim(location_name) <> '')
);

CREATE INDEX IF NOT EXISTS idx_inventory_date
    ON core.inventory (business_id, snapshot_date);

-- 3.5 marketing_platform, marketing_campaign, ad_spend
CREATE TABLE IF NOT EXISTS core.marketing_platform (
    platform_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    platform_name  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_platform_name UNIQUE (platform_name)
);

CREATE TABLE IF NOT EXISTS core.marketing_campaign (
    campaign_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id           BIGINT NOT NULL REFERENCES core.business (business_id),
    platform_id           BIGINT NOT NULL REFERENCES core.marketing_platform (platform_id),
    external_campaign_id  TEXT   NOT NULL,
    campaign_name         TEXT   NOT NULL,
    campaign_status       TEXT   NOT NULL DEFAULT 'active',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_campaign_external
        UNIQUE (business_id, platform_id, external_campaign_id),
    CONSTRAINT chk_campaign_status CHECK (
        campaign_status IN ('active','paused','archived')
    )
);

CREATE TABLE IF NOT EXISTS core.ad_spend (
    ad_spend_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id    BIGINT NOT NULL REFERENCES core.business (business_id),
    campaign_id    BIGINT NOT NULL REFERENCES core.marketing_campaign (campaign_id),
    spend_date     DATE   NOT NULL,
    spend_amount   NUMERIC(12,2) NOT NULL CHECK (spend_amount >= 0),
    impressions    BIGINT NOT NULL DEFAULT 0 CHECK (impressions >= 0),
    clicks         BIGINT NOT NULL DEFAULT 0 CHECK (clicks >= 0),
    purchases      INTEGER NOT NULL DEFAULT 0 CHECK (purchases >= 0),
    purchase_value NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (purchase_value >= 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_ad_spend_day UNIQUE (campaign_id, spend_date)
);

CREATE INDEX IF NOT EXISTS idx_campaign_business ON core.marketing_campaign (business_id);
CREATE INDEX IF NOT EXISTS idx_ad_spend_date     ON core.ad_spend (business_id, spend_date);

-- Seed the one platform row we need
INSERT INTO core.marketing_platform (platform_name)
VALUES ('meta_ads')
ON CONFLICT (platform_name) DO NOTHING;

-- 3.6 expense, financial_transaction
CREATE TABLE IF NOT EXISTS core.expense (
    expense_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id      BIGINT NOT NULL REFERENCES core.business (business_id),
    expense_date     DATE   NOT NULL,
    expense_category TEXT   NOT NULL,
    description      TEXT,
    amount           NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_expense_category CHECK (
        expense_category IN ('rent','salaries','packaging','software','utilities','platform_fee','other')
    )
);

CREATE TABLE IF NOT EXISTS core.financial_transaction (
    transaction_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id       BIGINT NOT NULL REFERENCES core.business (business_id),
    transaction_date  TIMESTAMPTZ NOT NULL,
    transaction_type  TEXT NOT NULL,
    direction         TEXT NOT NULL,
    amount            NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    order_id          BIGINT REFERENCES core.orders (order_id),
    payment_id        BIGINT REFERENCES core.payment (payment_id),
    shipment_id       BIGINT REFERENCES core.shipment (shipment_id),
    ad_spend_id       BIGINT REFERENCES core.ad_spend (ad_spend_id),
    expense_id        BIGINT REFERENCES core.expense (expense_id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_txn_direction CHECK (direction IN ('in','out')),
    CONSTRAINT chk_txn_type CHECK (
        transaction_type IN ('sale','refund','cogs','shipping_cost','rto_cost','gateway_fee','ad_spend','expense')
    ),
    CONSTRAINT chk_txn_single_source CHECK (
        num_nonnulls(payment_id, shipment_id, ad_spend_id, expense_id) <= 1
    )
);

CREATE INDEX IF NOT EXISTS idx_expense_date ON core.expense (business_id, expense_date);
CREATE INDEX IF NOT EXISTS idx_txn_date     ON core.financial_transaction (business_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_txn_order    ON core.financial_transaction (order_id);


-- ---------------------------------------------------------------------
-- PART 4: COLUMNS THAT NEEDED OTHER TABLES TO EXIST FIRST
-- ---------------------------------------------------------------------

-- business_id on the pipeline tables (needs core.business)
-- Note: on a fresh database these tables are empty, so NOT NULL is safe.
ALTER TABLE raw.raw_data
    ADD COLUMN IF NOT EXISTS business_id BIGINT NOT NULL
    REFERENCES core.business (business_id);

ALTER TABLE staging.staging_data
    ADD COLUMN IF NOT EXISTS business_id BIGINT NOT NULL
    REFERENCES core.business (business_id);

CREATE INDEX IF NOT EXISTS idx_raw_business
    ON raw.raw_data (business_id);

CREATE INDEX IF NOT EXISTS idx_staging_business
    ON staging.staging_data (business_id);

CREATE TABLE IF NOT EXISTS raw.sync_log (
    sync_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id      BIGINT NOT NULL REFERENCES core.business (business_id),
    source           TEXT NOT NULL,
    sync_type        TEXT NOT NULL, -- 'full', 'incremental', 'mock_test'
    status           TEXT NOT NULL, -- 'started', 'success', 'failed'
    records_fetched  INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated  INTEGER DEFAULT 0,
    error_message    TEXT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sync_log_lookup
    ON raw.sync_log (business_id, source, status, completed_at);

-- campaign_id on orders (needs core.marketing_campaign)
ALTER TABLE core.orders
    ADD COLUMN IF NOT EXISTS campaign_id BIGINT
    REFERENCES core.marketing_campaign (campaign_id);

CREATE INDEX IF NOT EXISTS idx_orders_campaign ON core.orders (campaign_id);


-- ---------------------------------------------------------------------
-- PART 5: ANALYTICS TABLES
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.profit_calculation (
    profit_id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id             BIGINT NOT NULL REFERENCES core.business (business_id),
    order_id                BIGINT NOT NULL REFERENCES core.orders (order_id),
    revenue                 NUMERIC(12,2) NOT NULL CHECK (revenue >= 0),
    cogs                    NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (cogs >= 0),
    ad_cost_allocated       NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (ad_cost_allocated >= 0),
    shipping_cost           NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (shipping_cost >= 0),
    rto_cost                NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (rto_cost >= 0),
    discount_amount         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    gateway_fee             NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (gateway_fee >= 0),
    platform_fee            NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (platform_fee >= 0),
    other_expense_allocated NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (other_expense_allocated >= 0),
    true_profit             NUMERIC(12,2) NOT NULL,
    calculated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_profit_order UNIQUE (order_id),
    CONSTRAINT chk_profit_formula CHECK (
        true_profit = revenue - cogs - ad_cost_allocated - shipping_cost
                      - rto_cost - discount_amount - gateway_fee
                      - platform_fee - other_expense_allocated
    )
);

CREATE TABLE IF NOT EXISTS analytics.rto_prediction (
    prediction_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id     BIGINT NOT NULL REFERENCES core.business (business_id),
    order_id        BIGINT NOT NULL REFERENCES core.orders (order_id),
    model_version   TEXT   NOT NULL,
    rto_probability NUMERIC(5,4) NOT NULL CHECK (rto_probability BETWEEN 0 AND 1),
    risk_level      TEXT   NOT NULL,
    predicted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_rto_pred UNIQUE (order_id, model_version),
    CONSTRAINT chk_risk_level CHECK (risk_level IN ('low','medium','high'))
);

CREATE TABLE IF NOT EXISTS analytics.sales_forecast (
    forecast_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id    BIGINT NOT NULL REFERENCES core.business (business_id),
    product_id     BIGINT NOT NULL REFERENCES core.product (product_id),
    forecast_date  DATE   NOT NULL,
    forecast_units NUMERIC(12,2) NOT NULL CHECK (forecast_units >= 0),
    lower_bound    NUMERIC(12,2) CHECK (lower_bound >= 0),
    upper_bound    NUMERIC(12,2) CHECK (upper_bound >= 0),
    model_version  TEXT   NOT NULL,
    generated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_sales_forecast UNIQUE (business_id, product_id, forecast_date, model_version),
    CONSTRAINT chk_sales_bounds CHECK (
        (lower_bound IS NULL OR lower_bound <= forecast_units)
        AND (upper_bound IS NULL OR upper_bound >= forecast_units)
    )
);

CREATE TABLE IF NOT EXISTS analytics.inventory_forecast (
    forecast_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id             BIGINT NOT NULL REFERENCES core.business (business_id),
    product_id              BIGINT NOT NULL REFERENCES core.product (product_id),
    forecast_date           DATE   NOT NULL,
    forecast_stock_units    NUMERIC(12,2) NOT NULL,
    days_of_stock_remaining NUMERIC(8,1) CHECK (days_of_stock_remaining >= 0),
    suggested_reorder_qty   INTEGER NOT NULL DEFAULT 0 CHECK (suggested_reorder_qty >= 0),
    model_version           TEXT   NOT NULL,
    generated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_inventory_forecast UNIQUE (business_id, product_id, forecast_date, model_version)
);

CREATE INDEX IF NOT EXISTS idx_profit_business   ON analytics.profit_calculation (business_id);
CREATE INDEX IF NOT EXISTS idx_rto_pred_business ON analytics.rto_prediction (business_id);
CREATE INDEX IF NOT EXISTS idx_sales_fc_lookup   ON analytics.sales_forecast (business_id, product_id, forecast_date);
CREATE INDEX IF NOT EXISTS idx_inv_fc_lookup     ON analytics.inventory_forecast (business_id, product_id, forecast_date);


-- ---------------------------------------------------------------------
-- PART 6: GROWTH TABLES
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS growth.recommendation (
    recommendation_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id            BIGINT NOT NULL REFERENCES core.business (business_id),
    recommendation_type    TEXT   NOT NULL,
    title                  TEXT   NOT NULL,
    description            TEXT   NOT NULL,
    product_id             BIGINT REFERENCES core.product (product_id),
    campaign_id            BIGINT REFERENCES core.marketing_campaign (campaign_id),
    courier_id             BIGINT REFERENCES core.courier (courier_id),
    priority               TEXT   NOT NULL DEFAULT 'medium',
    expected_impact_amount NUMERIC(12,2),
    rule_version           TEXT   NOT NULL,
    status                 TEXT   NOT NULL DEFAULT 'new',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at             TIMESTAMPTZ,
    CONSTRAINT chk_rec_type CHECK (
        recommendation_type IN ('reduce_ad_spend','increase_stock','adjust_marketing_rto',
                                'focus_on_product','change_courier','other')
    ),
    CONSTRAINT chk_rec_priority CHECK (priority IN ('low','medium','high')),
    CONSTRAINT chk_rec_status CHECK (
        status IN ('new','viewed','accepted','rejected','completed','expired')
    ),
    CONSTRAINT chk_rec_expiry CHECK (expires_at IS NULL OR expires_at > created_at),
    CONSTRAINT chk_rec_title CHECK (btrim(title) <> '')
);

CREATE TABLE IF NOT EXISTS growth.business_feedback (
    feedback_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id        BIGINT NOT NULL REFERENCES core.business (business_id),
    recommendation_id  BIGINT NOT NULL REFERENCES growth.recommendation (recommendation_id),
    action_taken       TEXT   NOT NULL,
    outcome            TEXT   NOT NULL DEFAULT 'unknown',
    rating             SMALLINT CHECK (rating BETWEEN 1 AND 5),
    comment            TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_fb_action CHECK (
        action_taken IN ('implemented','partially_implemented','ignored','not_applicable')
    ),
    CONSTRAINT chk_fb_outcome CHECK (
        outcome IN ('improved','no_change','worsened','unknown')
    )
);

CREATE INDEX IF NOT EXISTS idx_rec_business ON growth.recommendation (business_id, status);
CREATE INDEX IF NOT EXISTS idx_rec_product  ON growth.recommendation (product_id);
CREATE INDEX IF NOT EXISTS idx_rec_campaign ON growth.recommendation (campaign_id);
CREATE INDEX IF NOT EXISTS idx_fb_rec       ON growth.business_feedback (recommendation_id);
CREATE INDEX IF NOT EXISTS idx_fb_business  ON growth.business_feedback (business_id);


-- ---------------------------------------------------------------------
-- PART 6.5: MULTI-TENANT STORE CONNECTIONS & OAUTH STATE
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.store_connection (
    connection_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id               BIGINT NOT NULL REFERENCES core.business (business_id) ON DELETE CASCADE,
    platform                  TEXT NOT NULL,
    store_domain              TEXT NOT NULL,
    access_token              TEXT NOT NULL,               -- Fernet-encrypted
    refresh_token             TEXT NOT NULL,               -- Fernet-encrypted
    access_token_expires_at   TIMESTAMPTZ,
    refresh_token_expires_at  TIMESTAMPTZ,
    scopes                    TEXT NOT NULL,
    status                    TEXT NOT NULL DEFAULT 'active',
    installed_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    CONSTRAINT uq_business_platform UNIQUE (business_id, platform),
    CONSTRAINT uq_store_domain UNIQUE (store_domain),
    CONSTRAINT chk_store_platform CHECK (platform IN ('shopify', 'daraz', 'woocommerce')),
    CONSTRAINT chk_store_status CHECK (status IN ('active', 'disconnected', 'revoked'))
);

CREATE INDEX IF NOT EXISTS idx_store_connection_lookup 
ON core.store_connection (business_id, platform, status);

CREATE INDEX IF NOT EXISTS idx_store_token_expiry
ON core.store_connection (platform, status, access_token_expires_at);

CREATE TABLE IF NOT EXISTS core.oauth_state (
    state_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id  BIGINT NOT NULL REFERENCES core.business (business_id) ON DELETE CASCADE,
    state_nonce  TEXT NOT NULL UNIQUE,
    platform     TEXT NOT NULL DEFAULT 'shopify',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '15 minutes')
);

CREATE INDEX IF NOT EXISTS idx_oauth_state_lookup
ON core.oauth_state (state_nonce, platform);



-- ---------------------------------------------------------------------
-- PART 6.6: UNIVERSAL MULTI-TENANT PLATFORM CONNECTION REGISTRY
-- (Phase 1 — Common Connection Infrastructure)
-- ---------------------------------------------------------------------

-- Stores encrypted credentials for every external platform integration.
-- All credential columns are encrypted at rest with Fernet AES-128-CBC + HMAC-SHA256.
-- This table replaces the Shopify-specific core.store_connection as the
-- canonical connection store for all platforms.
CREATE TABLE IF NOT EXISTS core.platform_connection (
    connection_id                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_id                       BIGINT NOT NULL REFERENCES core.business (business_id) ON DELETE CASCADE,
    platform                          TEXT NOT NULL,          -- Legacy provider values may remain in rows; active scope is application-managed.
    platform_category                 TEXT NOT NULL,          -- 'ecommerce' | 'marketing' | 'courier' | 'payment'
    account_identifier                TEXT NOT NULL,          -- store domain, seller ID, ad account ID, etc.
    account_name                      TEXT,
    auth_type                         TEXT NOT NULL,          -- 'oauth2' | 'api_key' | 'token' | 'credentials'
    encrypted_access_token            TEXT,                   -- Fernet-encrypted primary access token / API key
    encrypted_refresh_token           TEXT,                   -- Fernet-encrypted refresh token (OAuth2 only)
    encrypted_additional_credentials  TEXT,                   -- Fernet-encrypted JSON for multi-part creds
    access_token_expires_at           TIMESTAMPTZ,
    refresh_token_expires_at          TIMESTAMPTZ,
    scopes                            TEXT,
    status                            TEXT NOT NULL DEFAULT 'active',
    error_message                     TEXT,
    last_synced_at                    TIMESTAMPTZ,
    installed_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_business_platform_account UNIQUE (business_id, platform, account_identifier),
    CONSTRAINT chk_platform_category CHECK (platform_category IN ('ecommerce', 'marketing', 'courier', 'payment')),
    CONSTRAINT chk_auth_type CHECK (auth_type IN ('oauth2', 'api_key', 'token', 'credentials')),
    CONSTRAINT chk_conn_status CHECK (status IN ('active', 'needs_reauth', 'disconnected', 'error'))
);

COMMENT ON TABLE core.platform_connection IS
    'Universal multi-tenant connection registry for all external platform integrations. '
    'Credential fields are always encrypted at rest using Fernet AES-128-CBC + HMAC-SHA256.';

CREATE INDEX IF NOT EXISTS idx_platform_conn_business
    ON core.platform_connection (business_id);
CREATE INDEX IF NOT EXISTS idx_platform_conn_status
    ON core.platform_connection (status)
    WHERE status <> 'active';


-- ---------------------------------------------------------------------
-- PART 7: VERIFICATION
-- Expected result: analytics 4, core 19, growth 2, raw 1, staging 1
-- (core count: +1 for core.platform_connection vs previous 18)
-- ---------------------------------------------------------------------
SELECT table_schema, count(*) AS tables
FROM information_schema.tables
WHERE table_schema IN ('raw', 'staging', 'core', 'analytics', 'growth')
  AND table_type = 'BASE TABLE'
GROUP BY table_schema
ORDER BY table_schema;

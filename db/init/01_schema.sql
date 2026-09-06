CREATE TABLE website_sessions (
    website_session_id  INTEGER PRIMARY KEY,
    created_at          TIMESTAMP NOT NULL,
    user_id             INTEGER NOT NULL,
    is_repeat_session   SMALLINT NOT NULL,
    utm_source          VARCHAR,
    utm_campaign        VARCHAR,
    utm_content         VARCHAR,
    device_type         VARCHAR,
    http_referer        VARCHAR
);

CREATE TABLE website_pageviews (
    website_pageview_id  INTEGER PRIMARY KEY,
    created_at           TIMESTAMP NOT NULL,
    website_session_id   INTEGER NOT NULL REFERENCES website_sessions (website_session_id),
    pageview_url         VARCHAR NOT NULL
);

CREATE TABLE products (
    product_id    INTEGER PRIMARY KEY,
    created_at    TIMESTAMP NOT NULL,
    product_name  VARCHAR NOT NULL
);

CREATE TABLE orders (
    order_id             INTEGER PRIMARY KEY,
    created_at           TIMESTAMP NOT NULL,
    website_session_id   INTEGER NOT NULL REFERENCES website_sessions (website_session_id),
    user_id              INTEGER NOT NULL,
    primary_product_id   INTEGER NOT NULL REFERENCES products (product_id),
    items_purchased      INTEGER NOT NULL,
    price_usd            NUMERIC(10, 2) NOT NULL,
    cogs_usd             NUMERIC(10, 2) NOT NULL
);

CREATE TABLE order_items (
    order_item_id    INTEGER PRIMARY KEY,
    created_at       TIMESTAMP NOT NULL,
    order_id         INTEGER NOT NULL REFERENCES orders (order_id),
    product_id       INTEGER NOT NULL REFERENCES products (product_id),
    is_primary_item  SMALLINT NOT NULL,
    price_usd        NUMERIC(10, 2) NOT NULL,
    cogs_usd         NUMERIC(10, 2) NOT NULL
);

CREATE TABLE order_item_refunds (
    order_item_refund_id  INTEGER PRIMARY KEY,
    created_at            TIMESTAMP NOT NULL,
    order_item_id         INTEGER NOT NULL REFERENCES order_items (order_item_id),
    order_id              INTEGER NOT NULL REFERENCES orders (order_id),
    refund_amount_usd     NUMERIC(10, 2) NOT NULL
);

CREATE INDEX idx_pageviews_session_id ON website_pageviews (website_session_id);
CREATE INDEX idx_orders_session_id ON orders (website_session_id);
CREATE INDEX idx_orders_primary_product_id ON orders (primary_product_id);
CREATE INDEX idx_order_items_order_id ON order_items (order_id);
CREATE INDEX idx_order_items_product_id ON order_items (product_id);
CREATE INDEX idx_refunds_order_item_id ON order_item_refunds (order_item_id);
CREATE INDEX idx_refunds_order_id ON order_item_refunds (order_id);

\copy website_sessions FROM '/data/website_sessions.csv' WITH (FORMAT csv, HEADER true);
\copy products FROM '/data/products.csv' WITH (FORMAT csv, HEADER true);
\copy website_pageviews FROM '/data/website_pageviews.csv' WITH (FORMAT csv, HEADER true);
\copy orders FROM '/data/orders.csv' WITH (FORMAT csv, HEADER true);
\copy order_items FROM '/data/order_items.csv' WITH (FORMAT csv, HEADER true);
\copy order_item_refunds FROM '/data/order_item_refunds.csv' WITH (FORMAT csv, HEADER true);

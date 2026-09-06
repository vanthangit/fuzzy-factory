# Fuzzy Factory: End-to-End ELT + Advanced SQL Analytics

Một project end-to-end trên bộ dữ liệu ecommerce Maven Fuzzy Factory (3 năm
session/pageview/order/refund của 1 cửa hàng bán thú nhồi bông online). Một
pipeline duy nhất, gồm 2 tầng đọc song song với nhau:

1. **Tầng ELT** — Postgres (mô phỏng production OLTP) → DuckDB (warehouse)
   → dbt Core, điều phối bởi Airflow, toàn bộ chạy trong Docker.
2. **Tầng phân tích SQL** — 5 câu hỏi kinh doanh trả lời bằng CTE, window
   function, và join trên chính các fact table mà tầng ELT xây dựng.

Mục tiêu không phải 1 dashboard đẹp — mà là bằng chứng: người dựng được
warehouse từ OLTP thô cũng chính là người viết được SQL biến warehouse đó
thành quyết định kinh doanh.

## 5 câu hỏi kinh doanh đã trả lời (kèm impact thật, tính từ data)

| # | Câu hỏi | Kỹ thuật SQL | Insight thật + impact ước tính |
|---|---|---|---|
| 1 | Kênh nào convert tốt nhưng đang bị underinvest? | `RANK() OVER (PARTITION BY month)` | Tháng 4/2012: `gsearch/brand` đạt **9.23%** conversion chỉ với 65 session, trong khi `gsearch/nonbrand` (kênh được rót traffic nhiều nhất — 3,509 session) chỉ đạt **2.45%**. Kênh brand convert gấp ~3.8 lần nhưng gần như không được đầu tư thêm traffic. |
| 2 | Bước nào trong funnel rò rỉ nhiều nhất? | CTE + window function đánh dấu thứ tự pageview, tổng hợp cộng dồn | Bước **product page → cart** giữ chân được đúng **36.35%** — thấp nhất toàn funnel (mất 166,278/261,231 session). Nếu cải thiện retention bước này lên 45% (tăng 8.65pp), ước tính có thêm **~7,700 đơn/3 năm ≈ $153,700/năm** doanh thu (AOV $59.99). |
| 3 | Conversion đang tăng hay giảm, trừ nhiễu ngày-qua-ngày? | `AVG() OVER (ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` | Trung bình trượt 7 ngày làm lộ rõ các đợt tăng/giảm bền vững (vd sau khi đổi lander) mà biểu đồ theo-ngày thô bị nhiễu che mất. |
| 4 | Sản phẩm nào có tỷ lệ hoàn tiền bất thường? | Join `order_items` ⋈ `order_item_refunds` cùng grain + benchmark bằng window function | **The Birthday Sugar Panda**: refund rate **6.04%** vs trung bình công ty **3.66%** (+2.38pp). Riêng phần "bất thường" này gây thêm **~118 lượt hoàn tiền dư**, tương đương **~$1,800/năm** chi phí có thể tránh được nếu điều tra nguyên nhân (QA, mô tả sản phẩm, đóng gói). |
| 5 | Sản phẩm mua đầu tiên nào dự báo LTV cao nhất? | Cohort theo sản phẩm mua đầu + `LAG`/`LEAD` đo thời gian quay lại mua tiếp | Khách bắt đầu bằng **The Forever Love Bear** có LTV trung bình **$67.43**, cao hơn **$31.96** so với nhóm bắt đầu bằng **The Hudson River Mini bear** ($35.47) — thấp nhất. Dịch chuyển ngân sách marketing ưu tiên sản phẩm "cửa ngõ" tốt hơn cho khách mới là cơ hội trực tiếp tăng LTV trung bình toàn cohort. |

*(Cách tính impact chi tiết, kèm giả định, nằm trong comment ở cuối mỗi file `analyses/*.sql`.)*

## Kiến trúc

```
┌─────────────┐   dbt-duckdb    ┌──────────────┐  Cosmos: 1 model = 1  ┌─────────────┐
│  Postgres   │  postgres ext.  │   DuckDB     │  Task Group (run+test)│  dbt marts  │
│  (OLTP src) │ ───────────────▶│ raw schema   │──────────────────────▶│ star schema │
└─────────────┘  extract_load   └──────────────┘                       └─────────────┘
        ▲                              ▲                                      ▲
        │                              │                                      │
        └──────── extract_load >> dbt_models (Cosmos TaskGroup) >> dbt_docs ──┘
                       (chay tren airflow-worker qua Celery)

┌────────────────────────────── Airflow (CeleryExecutor) ──────────────────────────────┐
│  airflow-webserver (UI)   airflow-scheduler   airflow-worker   airflow-triggerer      │
│         │                        │                   │                │              │
│         └──────────────── airflow-postgres (metadata riêng) ──────────┘              │
│                                   │                                                    │
│                                redis (broker)                                          │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

Toàn bộ chạy qua `docker-compose`. Airflow dùng đúng kiến trúc tham chiếu
chính thức cho self-hosted production (`CeleryExecutor`): webserver,
scheduler, worker và triggerer là 4 container tách biệt, dùng chung 1
Postgres metadata riêng (`airflow-postgres` — không lẫn với `postgres`
chứa data nghiệp vụ) và Redis làm broker. `airflow-init` là container
chạy 1 lần lúc khởi tạo (migrate schema + tạo admin user), tự thoát sau
khi xong. Nhờ metadata nằm trên Postgres thật (không phải SQLite gắn
trong 1 container), lịch sử DAG run, task logs, và tài khoản đăng nhập
đều sống sót qua mọi lần `docker-compose down`/`up` hay rebuild — chỉ mất
khi chủ động xoá volume (`docker-compose down -v`).

### Data model (star schema)

- `dim_products`
- `fct_sessions` — grain: `website_session_id`, kèm landing/exit page và cờ conversion
- `fct_orders` — grain: `order_id`, gắn kênh marketing từ session gốc
- `fct_order_items` — grain: `order_item_id` (tách riêng khỏi `fct_orders` để join được với `fct_refunds` cùng grain phục vụ phân tích refund-risk)
- `fct_refunds` — grain: `order_item_refund_id`

## Các quyết định kiến trúc (và vì sao không chọn phương án "hiển nhiên" hơn)

| Quyết định | Đã chọn | Thay vì | Vì sao |
|---|---|---|---|
| Nguồn dữ liệu | Postgres | CSV / dbt seed | Mô phỏng đúng OLTP production thật; `seed` chỉ dành cho bảng lookup nhỏ, không phải fact data |
| Warehouse | DuckDB | BigQuery | Reproducible với `docker-compose up`, không cần billing/service account cloud; BigQuery đã có ở project khác trong portfolio |
| Extract-Load | DuckDB `postgres` extension (`ATTACH ... TYPE postgres`) snapshot vào schema `raw` | dbt đọc thẳng Postgres lúc transform | Đúng hành vi 1 EL tool thật (Fivetran/Airbyte) — transform chạy trên snapshot, không phụ thuộc kết nối OLTP đang sống |
| Orchestration | Airflow `CeleryExecutor` — webserver/scheduler/worker/triggerer tách container riêng, Postgres metadata riêng + Redis broker | `standalone` mode (SQLite + SequentialExecutor) | Kiến trúc tham chiếu chính thức của Airflow cho self-hosted production; SQLite không hỗ trợ ghi đồng thời và không sống sót qua container recreate — không phù hợp để giữ lịch sử DAG run thật |
| Schedule | `schedule=None`, trigger thủ công | Cron thật | Dataset là lịch sử tĩnh, không có data mới — đặt cron giả sẽ là "orchestration theater" |
| Cấu trúc dbt models | Staging nhóm theo **source system** (`staging/fuzzyfactory/`), marts/intermediate nhóm theo **domain nghiệp vụ** (`marts/marketing/`) | Để phẳng 1 cấp | Đúng convention chính thức của dbt Labs — staging phản ánh hệ thống nguồn (dễ mở rộng thêm nguồn mới sau này), marts phản ánh cách stakeholder thật sự tiêu thụ data |
| DAG orchestration cho dbt | **Astronomer Cosmos** — mỗi model là 1 Task Group riêng (`run` + `test`), dependency đọc thẳng từ manifest dbt | 1 task lớn `dbt run` + 1 task lớn `dbt test` | Thấy rõ model nào fail thay vì cả `dbt run` fail chung chung; dependency không bị khai báo trùng 2 nơi (dbt và Airflow) nên không lệch khi model thay đổi |
| Concurrency cho task Cosmos | Airflow Pool 1 slot (`duckdb_writer`) — mọi task chạm `warehouse.duckdb` xếp hàng tuần tự | Để Cosmos chạy song song tự do | DuckDB chỉ cho 1 tiến trình giữ write-lock cùng lúc; chạy song song thật sẽ lỗi "Conflicting lock". Đánh đổi mất song song, chấp nhận được vì dataset nhỏ (~2 phút/lần chạy) |

## Chạy project

**1. Chuẩn bị data:** đặt 6 file CSV Maven Fuzzy Factory vào `data/raw_csv/` (xem `data/raw_csv/README.md`).

**2. Khởi động toàn bộ stack:**
```bash
docker-compose up -d --build
```
Postgres tự seed từ CSV lúc khởi tạo lần đầu (`db/init/`).

**3. Đăng nhập** tại `http://localhost:8080` bằng credential trong `.env` (mặc định `admin`/`admin`) — DAG `fuzzy_factory_elt` đã unpause sẵn (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false`), trigger là chạy ngay. Vào tab Graph để thấy từng model dbt là 1 Task Group riêng (`run` + `test`). Hoặc chạy tay để debug:
```bash
docker exec fuzzy-factory-project-airflow-webserver-1 python /opt/airflow/scripts/extract_load.py
docker exec fuzzy-factory-project-airflow-webserver-1 dbt build --project-dir /opt/airflow/dbt_project
```
*(Trên Windows Git Bash, thêm `MSYS_NO_PATHCONV=1` trước lệnh `docker exec` để tránh path bị tự động convert sai.)*

**4. Xem tài liệu dbt** (lineage graph, mô tả cột, test coverage) tại `http://localhost:8081`.

**5. Xem trực tiếp warehouse:** file `warehouse/warehouse.duckdb` được bind-mount ra host — mở bằng DuckDB extension của VSCode hoặc bất kỳ DuckDB client nào, không cần vào container.

**6. Chạy các câu phân tích:** `dbt compile` để render Jinja trong `dbt_project/analyses/*.sql` thành SQL thuần, rồi chạy trực tiếp trên `warehouse.duckdb`.

## Data quality

Hai lớp test chạy qua `dbt test` (bước 3 trong DAG, sau `dbt run`, trước `dbt docs generate`):

- **Generic tests** (`not_null`, `unique`, `relationships`) trên mọi khoá chính/khoá ngoại ở cả tầng staging và marts — 37 test.
- **Singular tests** (`dbt_project/tests/*.sql`) — 5 rule nghiệp vụ tự viết, mỗi file là 1 câu SELECT phải trả về 0 dòng mới pass:
  - `assert_order_total_matches_order_items` — tổng đơn hàng phải khớp tổng chi tiết sản phẩm
  - `assert_order_has_exactly_one_primary_item` — mỗi đơn đúng 1 sản phẩm chính
  - `assert_order_primary_product_matches_item` — 2 nguồn dữ liệu (orders/order_items) phải thống nhất về sản phẩm chính
  - `assert_refund_not_exceeding_item_price` — không hoàn tiền nhiều hơn giá đã bán
  - `assert_session_has_landing_page` — không có session rỗng (lỗi tracking)

**Kết quả trên data thật** (472,871 session · 1,188,124 pageview · 32,313 đơn · 40,025 order item · 1,731 lượt hoàn tiền): `dbt build` → 13 model + 1 seed + 42 test → **PASS=56, ERROR=0**.

## Cấu trúc repo

```
fuzzy-factory-e2e/
├── docker-compose.yml               # postgres, airflow-postgres, redis,
│                                     # airflow-init/webserver/scheduler/
│                                     # worker/triggerer, dbt-docs
├── .env / .env.example              # credential + secret (.env gitignored)
├── .gitignore                       # chan CSV + warehouse.duckdb + .env khoi git
├── data/raw_csv/                    # CSV goc (gitignored)
├── db/init/                         # Postgres schema + seed (chay luc khoi tao)
├── warehouse/                       # warehouse.duckdb (gitignored)
├── airflow/
│   ├── Dockerfile                  # dbt-core, dbt-duckdb, astronomer-cosmos
│   ├── dags/fuzzy_factory_elt.py   # DAG dung Cosmos DbtTaskGroup
│   └── scripts/extract_load.py     # Postgres -> DuckDB raw schema
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │   ├── staging/fuzzyfactory/   # nhom theo SOURCE SYSTEM, moi model co .sql + .yml rieng
    │   ├── intermediate/marketing/ # nhom theo DOMAIN nghiep vu
    │   └── marts/marketing/        # star schema
    ├── seeds/                      # funnel_steps.csv - bang tra cuu tinh
    ├── analyses/                   # 5 cau hoi kinh doanh - tang SQL that
    └── tests/                      # singular tests (business rules)
```

## Từ project này lên lakehouse thật

Mô hình 3 tầng raw → staging → marts ở đây chính là **Medallion Architecture**
(Bronze/Silver/Gold) — cách tổ chức chuẩn ở mọi lakehouse thật (Iceberg +
Trino, Databricks, Snowflake...), không riêng gì DuckDB. dbt có adapter cho
Trino/Spark/Databricks/Snowflake/BigQuery, nên toàn bộ cấu trúc thư mục,
naming convention, và cách viết test ở project này **chuyển thẳng 1:1**
sang môi trường lakehouse thật — chỉ đổi `type: duckdb` thành `type: trino`
(hay tương đương) trong `profiles.yml`.

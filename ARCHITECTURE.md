# Kiến trúc & Vận hành Chi tiết — Fuzzy Factory

File này giải thích **cách toàn bộ hệ thống hoạt động bên trong**, đặc biệt là
hạ tầng Airflow — viết để bạn hiểu đủ sâu, đủ để trả lời bất kỳ câu hỏi
phỏng vấn nào về project này. README.md là bản kể chuyện cho người đọc
ngoài (business question → impact); file này là bản kỹ thuật cho chính bạn.

---

## 1. Bức tranh toàn cảnh

```
                    ┌─────────────────────────────────────────────┐
                    │              docker-compose                  │
                    │                                               │
   CSV gốc  ──seed──▶│  postgres (OLTP)                             │
                    │       │                                       │
                    │       │ extract_load.py (chạy trong Airflow)  │
                    │       ▼                                       │
                    │  DuckDB: raw schema                          │
                    │       │                                       │
                    │       │ dbt (staging → intermediate → marts) │
                    │       ▼                                       │
                    │  DuckDB: star schema (fct_*, dim_*)          │
                    │       │                                       │
                    │       │ dbt compile (analyses/*.sql)         │
                    │       ▼                                       │
                    │  5 câu trả lời business question             │
                    │                                               │
                    │  Toàn bộ điều phối bởi: Airflow (LocalExecutor)  │
                    └─────────────────────────────────────────────┘
```

Có **2 warehouse hoàn toàn tách biệt** trong hệ thống, dễ nhầm nên nói rõ:

1. **`postgres`** — đóng vai trò OLTP nguồn (dữ liệu nghiệp vụ: sessions,
   orders...). Đây là "production database" giả lập.
2. **`airflow-postgres`** — chỉ chứa metadata của Airflow (DAG nào đã chạy,
   task nào thành công, user nào tồn tại...). Không hề chứa dữ liệu nghiệp
   vụ. Tách riêng vì 2 lý do: (a) đúng thực hành thật — không bao giờ trộn
   dữ liệu vận hành công cụ orchestration với dữ liệu nghiệp vụ; (b) vòng
   đời khác nhau — xoá sạch metadata Airflow không ảnh hưởng gì tới data.

Và **DuckDB** (file `warehouse/warehouse.duckdb`) là nơi mọi phép biến đổi
dữ liệu (dbt) diễn ra — nó đọc từ `postgres` (qua bước extract-load) nhưng
sau đó hoàn toàn độc lập.

---

## 2. Hạ tầng Airflow — vì sao có 4 container thay vì 1

Airflow có 1 chế độ chạy đơn giản gọi là `standalone` (gộp mọi thứ vào 1
process, dùng SQLite) — dùng tốt để demo nhanh nhưng **không sống sót qua
việc container bị xoá/tạo lại** (SQLite gắn chặt vào filesystem của đúng
container đó). Project này dùng **LocalExecutor** với Postgres metadata
riêng — đủ để có lịch sử DAG run bền vững và webserver/scheduler tách
biệt, mà không cần thêm broker (Redis) hay container worker riêng như
CeleryExecutor:

| Container | Vai trò | Nếu chết thì sao |
|---|---|---|
| `airflow-postgres` | "Bộ nhớ" của toàn hệ thống — lưu DAG, task instance, user, pool, connection, biến. Mọi container khác đều đọc/ghi vào đây | Mất hết lịch sử, phải khởi tạo lại |
| `airflow-webserver` | Giao diện web (`:8080`) + REST API. Đọc/ghi airflow-postgres, **không tự chạy task nào** | Không xem UI được, nhưng pipeline vẫn tự chạy bình thường |
| `airflow-scheduler` | "Bộ não" **kiêm** "đôi tay" — liên tục quét DAG, quyết định task nào đã đủ điều kiện để chạy, rồi **tự thực thi luôn** task đó bằng 1 subprocess nội bộ (không cần đẩy qua hàng đợi cho container khác) | Không có gì được lên lịch/chạy mới, task đang chạy dở bị dừng theo |
| `airflow-init` | Chạy **đúng 1 lần** lúc khởi tạo cụm: tạo schema trong airflow-postgres (`airflow db migrate`), tạo user admin, tạo Pool `duckdb_writer`. Tự thoát (`Exited (0)`) sau khi xong — đây là hành vi đúng, không phải lỗi | — |

**Điểm mấu chốt cần nhớ:** với `LocalExecutor`, `scheduler` vừa quyết định
vừa thực thi trong cùng 1 tiến trình (dùng process pool nội bộ, giới hạn
song song bằng `AIRFLOW__CORE__PARALLELISM`) — khác với `CeleryExecutor`
nơi "quyết định" (scheduler) và "thực thi" (worker) là 2 tiến trình/2
container tách biệt, giao tiếp qua broker (Redis). `webserver` trong cả 2
kiến trúc đều chỉ là "cửa sổ để nhìn vào", không bao giờ tự chạy task.

**Vì sao không cần Celery/Redis cho project này:** CeleryExecutor tồn tại
để **scale ra nhiều máy/nhiều worker** khi có tải thật, nhiều DAG chạy
đồng thời. Dataset ở đây là lịch sử tĩnh, chỉ 1 DAG, trigger thủ công,
không có nhu cầu chạy song song nhiều pipeline — thêm Redis + worker riêng
chỉ tăng số container phải quản lý mà không giải quyết vấn đề thật nào.
`LocalExecutor` + Postgres metadata riêng đã đủ đạt mục tiêu chính (lịch
sử DAG run bền vững, webserver/scheduler tách biệt) với ít thành phần
nhất có thể.

### Vòng đời 1 lần trigger DAG

```
1. Bạn bấm "Trigger" trên UI (webserver)
        │
        ▼
2. webserver ghi 1 dòng DagRun mới vào airflow-postgres (state = queued)
        │
        ▼
3. scheduler (đang liên tục polling airflow-postgres) thấy DagRun mới,
   tính task nào sẵn sàng chạy (chưa có dependency nào chưa xong)
        │
        ▼
4. scheduler tự fork 1 subprocess nội bộ để chạy task đó ngay:
   - extract_load_postgres_to_duckdb → gọi extract_load.py
   - dbt_models.<model>.run / .test → Cosmos gọi dbtRunner nội bộ
   - dbt_docs_generate → gọi `dbt docs generate`
        │
        ▼
5. subprocess xong, scheduler ghi state (success/failed) về airflow-postgres
        │
        ▼
6. scheduler thấy task xong → tính tiếp task nào ĐÃ sẵn sàng (nhờ
   dependency vừa xong) → quay lại bước 3
        │
        ▼
7. Khi mọi task xong, scheduler đánh dấu DagRun = success/failed
        │
        ▼
8. webserver chỉ ĐỌC lại toàn bộ trạng thái này từ airflow-postgres để
   vẽ lên UI - bản thân webserver không biết gì về việc task chạy ra sao
   ngoài những gì đọc được từ DB
```

---

## 3. DAG cụ thể: `fuzzy_factory_elt.py`

```python
extract_load >> dbt_models >> dbt_docs
```

- **`extract_load`** — `BashOperator` gọi `extract_load.py` (snapshot 6
  bảng Postgres sang schema `raw` của DuckDB qua `postgres` extension).
- **`dbt_models`** — không phải 1 task, mà là **`DbtTaskGroup`** của
  **Astronomer Cosmos**. Cosmos đọc `manifest.json` của dbt (sinh ra khi
  parse `dbt_project/`) và **tự động tạo 1 Task Group cho mỗi model/seed**,
  mỗi group gồm 2 task con: `run` rồi `test`. Dependency giữa các group
  được Cosmos suy ra thẳng từ `ref()`/`source()` trong SQL — không cần tự
  tay khai báo lại trong Airflow (tránh tình trạng dbt đổi mà quên sửa DAG).
- **`dbt_docs`** — `BashOperator` gọi `dbt docs generate` để sinh lại
  `manifest.json`/`catalog.json` mới nhất cho service `dbt-docs` phục vụ.

### Vì sao có `pool="duckdb_writer"` gắn vào mọi task?

DuckDB **chỉ cho 1 tiến trình giữ write-lock tại 1 thời điểm** — khác hẳn
Postgres/Trino hỗ trợ nhiều tiến trình ghi cùng lúc thật sự. Cosmos thiết
kế để chạy nhiều model **song song**, và `LocalExecutor` cũng chạy nhiều
subprocess cùng lúc nếu không giới hạn (`AIRFLOW__CORE__PARALLELISM`).
Nếu để mặc định, 2 model chạy cùng lúc sẽ tranh nhau mở `warehouse.duckdb`
→ lỗi `Conflicting lock is held`. Vấn đề này tồn tại **bất kể chọn executor
nào** (Celery hay Local đều gặp) — vì gốc rễ là giới hạn của DuckDB, không
phải của Airflow.

**Airflow Pool** là cơ chế có sẵn để giới hạn số task được chạy đồng thời
cho 1 nhóm tài nguyên dùng chung. Pool `duckdb_writer` có **đúng 1 slot** —
nghĩa là dù có 10 task cùng "sẵn sàng chạy" theo dependency, chỉ 1 task
được cấp slot để thực thi tại 1 thời điểm, các task còn lại đứng ở trạng
thái `queued` chờ tới lượt. Đây không phải hack — là cách dùng Pool đúng
sách vở của Airflow cho đúng tình huống "tài nguyên dùng chung giới hạn
concurrency" (rate-limited API, hệ thống legacy single-threaded... đều
dùng pattern này).

**Cái giá phải trả:** mất khả năng chạy song song thật của Cosmos — 26
task (13 model × ~2 + 1 seed) chạy tuần tự, mất ~2 phút thay vì có thể
~30-40 giây nếu song song. Chấp nhận được vì dataset nhỏ. Nếu sau này đổi
warehouse sang engine hỗ trợ multi-writer thật (Postgres, Trino), bỏ pool
này đi là tận dụng được ngay lợi thế song song của Cosmos.

---

## 4. Tầng dbt — 5 lớp, mỗi lớp 1 trách nhiệm

```
raw (Postgres snapshot)
  │
  ▼
staging/fuzzyfactory/     — 1:1 với bảng nguồn, chỉ rename/cast, KHÔNG join
  │
  ▼
intermediate/marketing/   — logic window function dùng lại được (đánh số
  │                          pageview trong session, đánh số đơn theo user)
  ▼
marts/marketing/          — star schema thành phẩm (fct_*, dim_*), JOIN
  │                          staging + intermediate theo đúng grain
  ▼
analyses/                 — 5 câu SQL trả lời business question, query
                             thẳng lên marts (không materialize, chỉ compile)

seeds/funnel_steps.csv    — bảng tra cứu tĩnh (url pattern → tên bước
                             funnel), join vào trong analyses, tự maintain
                             bằng tay thay vì hardcode CASE WHEN
```

**Test 2 lớp:**
- **Generic tests** (khai báo trong `.yml` cạnh mỗi model) — `unique`,
  `not_null`, `relationships`. dbt có sẵn macro sinh SQL tự động, không
  cần viết tay.
- **Singular tests** (`dbt_project/tests/*.sql`) — 5 rule nghiệp vụ tự
  viết, mỗi file 1 câu SELECT phải trả về 0 dòng mới pass (vd: tổng đơn
  hàng phải khớp tổng chi tiết sản phẩm).

---

## 5. Bảng tra cứu docker-compose

| Service | Image/Build | Port ra ngoài | Volume |
|---|---|---|---|
| `postgres` | `postgres:16` | `5432` | `pgdata` (named) + bind-mount CSV/init script |
| `airflow-postgres` | `postgres:16` | — (nội bộ) | `airflow_pgdata` (named) |
| `airflow-init` | build `./airflow` | — (chạy xong tự thoát) | dùng chung volume airflow-common |
| `airflow-webserver` | build `./airflow` | `8080` | dùng chung + `airflow_logs` (named) |
| `airflow-scheduler` | build `./airflow` | — | dùng chung |
| `dbt-docs` | build `./airflow` | `8081` | chỉ mount `dbt_project` + `warehouse` (không cần dags/scripts) |

**Named volume vs bind-mount — vì sao chọn cái nào:**
- **Named volume** (`pgdata`, `airflow_pgdata`, `airflow_logs`) — dùng cho
  dữ liệu cần **bền vững qua container recreate** và không cần con người
  đọc trực tiếp từ host (Postgres data dir, Airflow task logs).
- **Bind-mount** (`./dbt_project`, `./warehouse`, `./airflow/dags`...) —
  dùng khi cần **sửa code từ host và thấy hiệu lực ngay** (dev loop nhanh,
  không cần rebuild image), hoặc cần **mở trực tiếp từ host** (file
  `warehouse.duckdb` mở bằng DuckDB extension của VSCode).

`x-airflow-common` ở đầu file là **YAML anchor** — không phải cú pháp
Docker Compose đặc biệt, chỉ là cách YAML tránh lặp lại y hệt 1 khối
`environment`/`volumes`/`depends_on` cho 3 service `airflow-*` (init,
webserver, scheduler đều cần đúng 1 bộ config đó).

---

## 6. Những sự cố thật đã gặp (đáng nhớ cho phỏng vấn)

| Sự cố | Nguyên nhân gốc | Cách giải quyết |
|---|---|---|
| `dbt-docs` container crash liên tục | Image `apache/airflow` có entrypoint riêng, hiểu nhầm `command: ["dbt", "docs", "serve"...]` thành `airflow dbt docs serve` | Override `entrypoint: ["/bin/bash", "-c"]` để bypass wrapper |
| Airflow admin password đổi random mỗi lần recreate container | `standalone` mode tự sinh user với password random, không persist | Bỏ hẳn `standalone`, chuyển sang kiến trúc multi-container với Postgres metadata riêng (ban đầu thử CeleryExecutor, sau đơn giản hoá về LocalExecutor — xem dòng dưới) |
| `_AIRFLOW_WWW_USER_CREATE` (cơ chế chính thức) không tạo được user cố định | Chạy quá sớm, trước khi bảng FAB (phân quyền) được khởi tạo lần đầu | Ban đầu viết script chờ-rồi-ghi-đè (workaround); sau khi bỏ `standalone`, cơ chế chính thức hoạt động đúng ngay, không cần workaround nữa |
| `pip install astronomer-cosmos` lỗi `ResolutionTooDeep: 200000` | pip cố giải quyết tương thích với hàng trăm package có sẵn trong base image cùng lúc | Dùng constraints file chính thức của Airflow để neo version package có sẵn |
| `Conflicting lock is held` khi chạy DAG qua Cosmos | Nhiều task Cosmos chạy song song, cùng tranh ghi `warehouse.duckdb` (DuckDB không hỗ trợ multi-writer) | Airflow Pool 1 slot (`duckdb_writer`) ép chạy tuần tự |
| `FileNotFoundError` khi dbt ghi log dù thư mục cha tồn tại | Windows Defender/Search Indexer giữ 1 tên file cụ thể ở trạng thái "delete-pending" sau khi bị xoá nhiều lần từ host, Docker Desktop dịch sai thành ENOENT | Chuyển `DBT_LOG_PATH` sang named volume (`airflow_logs`) thay vì bind-mount Windows |
| CeleryExecutor + Redis + worker riêng dư thừa cho project | Dataset lịch sử tĩnh, 1 DAG, trigger thủ công — không có tải thật cần scale ra nhiều worker | Chuyển sang `LocalExecutor`: scheduler tự chạy task bằng subprocess nội bộ, bỏ hẳn Redis + container worker + triggerer, giảm từ 9 xuống 5 container mà vẫn giữ được lịch sử DAG run bền vững (nhờ Postgres metadata riêng) |

---

## 7. Cheat sheet lệnh hay dùng

```bash
# Khởi động toàn bộ stack
docker-compose up -d --build

# Xem log 1 service
docker-compose logs -f airflow-scheduler

# Chạy dbt tay để debug (không qua Airflow)
docker exec fuzzy-factory-project-airflow-webserver-1 \
  dbt build --project-dir /opt/airflow/dbt_project

# Trigger DAG qua CLI (không cần vào UI)
docker exec fuzzy-factory-project-airflow-webserver-1 \
  airflow dags trigger fuzzy_factory_elt

# Xem toàn bộ task trong DAG (kiểm tra cấu trúc Cosmos sinh ra)
docker exec fuzzy-factory-project-airflow-webserver-1 \
  airflow tasks list fuzzy_factory_elt

# Xem trạng thái từng task của 1 lần chạy cụ thể
docker exec fuzzy-factory-project-airflow-webserver-1 \
  airflow tasks states-for-dag-run fuzzy_factory_elt "<execution_date>"

# Reset toàn bộ (mất hết data + lịch sử, dùng khi cần build lại từ đầu)
docker-compose down -v
```

*(Trên Windows Git Bash, thêm `MSYS_NO_PATHCONV=1` trước `docker exec` để
tránh path bị tự động convert sai.)*

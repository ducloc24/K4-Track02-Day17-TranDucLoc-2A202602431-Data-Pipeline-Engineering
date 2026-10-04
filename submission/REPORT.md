# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Trần Đức Lộc - 2A202602431
**Repo:** https://github.com/ducloc24/K4-Track02-Day17-TranDucLoc-2A202602431-Data-Pipeline-Engineering.git
**Commit bài nộp:** 429069049b88b9b360b6b1e7e723d5b9fb30f80a
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Codex - Phạm vi: Hỗ trợ đọc hiểu pineline và tóm tắt task cần làm
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | 24 rows cho 12 tickets; T-91 có 3 rows | u05 ngày 08-12 là (2,1,0), không phải (5,3,1) | T-97 vẫn còn PII và `is_deleted=false` |
| **Nguyên nhân gốc** | Hàm cũ chỉ INSERT; chưa MERGE theo `ticket_id` và chưa chặn LSN cũ | Lookback bằng 0 nên ngày 12 không được tính lại khi event đến ngày 15 | `ticket_changes_sql` chỉ đọc `after.ticket_id`, nên làm mất CDC delete có `after=null` |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: MERGE theo `ticket_id`, UPDATE khi `_lsn` mới hơn | `pipeline/config.py`: đặt `LOOKBACK_DAYS=3` theo `ceil(P99)`; feature dùng `event_time` | `pipeline/staging.py`: lấy khóa bằng `coalesce(after.ticket_id, key.ticket_id)`; tombstone Kafka vẫn bị loại |
| **Khái niệm trên slide** | CDC upsert, idempotency, ordering bằng LSN | Event time vs ingest time; watermark/lookback; overwrite partition | CDC delete, tombstone, privacy propagation, idempotency |

## 2. Các con số

- P99 lateness đo từ Bronze: `3` ngày → `LOOKBACK_DAYS = 3` (`ceil(P99)`)
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY / MISMATCH

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE giữ một trạng thái hiện tại theo `ticket_id` và overwrite-partition tính lại các ngày bị late event ảnh hưởng.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ dấu vết xoá cùng `_lsn` để không hồi sinh dữ liệu khi replay, đồng thời xoá toàn bộ PII.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: bảo đảm kết quả point-in-time tái lập được và snapshot đã phát hành giữ nguyên nội dung.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu cục bộ nhỏ, DuckDB/dbt đủ cho SQL, incremental, microbatch và parity mà không cần cluster.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào? Tôi giữ nguyên các snapshot lịch sử đã phát hành để bảo đảm tính bất biến, nhưng áp dụng delete cho dữ liệu hiện tại và mọi snapshot tạo sau 08-15; nếu chính sách yêu cầu xoá hồi tố thì tạo bản phát hành đã redact riêng, không sửa âm thầm snapshot cũ.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao? Tôi thêm bộ nhận diện tên/NER hoặc danh sách tên theo ngữ cảnh ở lớp Silver trước khi dữ liệu đi sang Gold, rồi kiểm thử bằng dữ liệu tổng hợp có tên, email và số điện thoại; đo recall/precision theo loại PII và chạy assertion rằng không còn PII trong Silver, training set và RAG chunks.

## 5. Output (dán nguyên văn)

```text
$ make verify
(.venv) PS C:\Users\PC\Documents\K4-Track02-Day17-TranDucLoc-2A202602431-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m pytest
..................................                                                                                                     [100%]
34 passed in 2.55s
(.venv) PS C:\Users\PC\Documents\K4-Track02-Day17-TranDucLoc-2A202602431-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
(.venv) PS C:\Users\PC\Documents\K4-Track02-Day17-TranDucLoc-2A202602431-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
(.venv) PS C:\Users\PC\Documents\K4-Track02-Day17-TranDucLoc-2A202602431-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m pytest
..................................                                                                                                    [100%]
34 passed in 2.38s

$ make rerun3
(.venv) PS C:\Users\PC\Documents\K4-Track02-Day17-TranDucLoc-2A202602431-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
(.venv) PS C:\Users\PC\Documents\K4-Track02-Day17-TranDucLoc-2A202602431-Data-Pipeline-Engineering> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
05:40:52  Running with dbt=1.12.5
05:40:52  Registered adapter: duckdb=1.11.0
05:40:53  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:40:53  
05:40:53  Concurrency: 1 threads (target='dev')
05:40:53  
05:40:53  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:40:53  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.08s]
05:40:53  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:40:53  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
05:40:53  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:40:53  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
05:40:53  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:40:53  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.11s]
05:40:53  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:40:53  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.14s]
05:40:53  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:40:53  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
05:40:53  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:40:53  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
05:40:53  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:40:53  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
05:40:53  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:40:53  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
05:40:53  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:40:53  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
05:40:53  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:40:53  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
05:40:53  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:40:53  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
05:40:53  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:40:53  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
05:40:53  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:40:53  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
05:40:53  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:40:53  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
05:40:53  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:40:53  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:40:54  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:54  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:40:54  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:54  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:40:54  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:54  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:40:54  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:54  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:40:54  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
05:40:54  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:40:54  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:54  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:40:54  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
05:40:54  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.27s]
05:40:54  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:40:54  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
05:40:54  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:40:54  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
05:40:54  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:40:54  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
05:40:54  
05:40:54  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.29 seconds (1.29s).
05:40:54  
05:40:54  Completed successfully
05:40:54  
05:40:54  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.

### Output Challenge 4 (PowerShell)

```text
$env:DO_NOT_TRACK = '1'
dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

# 1. Cấu trúc dữ liệu

Số liệu lấy từ server test `10.100.120.64` ngày 2026-10-13. PROD (cluster `sds_cluster`) có thể khác.

## 1.1 Các tầng dữ liệu

```
OLTP (SQL Server)
   │  CDC (Debezium/Flink)  → các cột __source_ts_ms, __deleted
   ▼
eb_raw      bản sao thô của bảng nguồn                 raw_general_ledger, raw_general_ledger_detail, raw_summary_opening_balance, raw_account_list ...
   ▼
eb_staging  làm sạch, đã đổi tên cột/kiểu              stg_general_ledger (GL), stg_general_ledger_detail (GLD), stg_summary_opening_balance, stg_account_list ...
   ▼
eb_dwh      mô hình hoá: fact + dim + bridge           fact_gl_entry_line, dim_account, bridge_org_scope, bridge_account_hierarchy,
                                                       fact_account_opening_period, fact_account_balance_daily, dim_currency, dim_date ...
eb_rpt      view phục vụ báo cáo (tham khảo)           v_gl_entry, v_sctk_detail, v_sctk_sddk ...
mart_finance  bảng dựng sẵn theo báo cáo (tham khảo)   account_ledger_detail, cash_book_detail
eb          kho/vật tư (luồng riêng)                   repository_ledger, repository_ledger_checkpoint, dict_* (dictionary), material_goods ...
```

Có thêm các bảng hậu tố `_legacy`, `_old`, `_v3`, `_shadow_*`, `_broken_*`, `_legacy_atomic_replace`: **bảng lịch sử/thử nghiệm
của team DWH, go-gateway không đọc**. Đừng trỏ query vào chúng.

## 1.2 Các loại bảng và vai trò

| Loại | Ví dụ | Dùng để | Ghi chú |
|---|---|---|---|
| **Fact** | `eb_dwh.fact_gl_entry_line` | Nguồn chính của sổ chi tiết tài khoản, sổ quỹ | Đã denormalize header + detail |
| **Staging** | `eb_staging.stg_general_ledger` + `..._detail` | Nguồn thay thế khi pipeline fact lỗi (xem [04](04-nguon-du-lieu-gl-gld.md)) | Phải JOIN GL với GLD lúc query |
| **Dim** | `eb_dwh.dim_account`, `dim_org_unit` | Tra tên/loại tài khoản | Có SCD: lọc `is_current = 1` |
| **Bridge** | `bridge_org_scope`, `bridge_account_hierarchy` | Mở rộng phạm vi (đơn vị phụ thuộc, tài khoản con) | Quan hệ cha-con |
| **Dictionary** | `eb.dict_accounting_object`, `dict_material_goods`, `dict_unit`, `dict_repository`... | Tra tên/mã theo ID **lúc query** bằng `dictGetOrDefault` | Xem lưu ý "snapshot đóng băng" bên dưới |
| **Ledger kho** | `eb.repository_ledger` | Nguồn của sổ chi tiết vật liệu và tính giá xuất kho | Tên cột PascalCase |
| **Checkpoint/Summary** | `eb.repository_ledger_checkpoint`, `eb_dwh.fact_account_opening_period` | Số dư lũy kế theo kỳ để khỏi quét cả lịch sử | Xem [05](05-so-du-dau-ky.md) |

## 1.3 Grain (một dòng đại diện cho cái gì)

Biết **grain** là điều kiện để viết đúng `GROUP BY`, `JOIN` và để không cộng trùng.

| Bảng | Một dòng = | Engine / khoá sắp xếp |
|---|---|---|
| `eb_dwh.fact_gl_entry_line` | 1 dòng chi tiết bút toán (một dòng GLD), kèm các cột header của chứng từ | `ReplacingMergeTree(__version, is_deleted)` · `ORDER BY (company_id, posted_date, account_number, reference_id, detail_id, src_row_id)` · `PARTITION BY toYYYYMM(posted_date)` |
| `eb_staging.stg_general_ledger` (GL) | 1 dòng sổ cái cấp "header" của chứng từ, nối với GLD qua `(DetailID, ReferenceID)` | `ReplacingMergeTree(__source_ts_ms)` · `ORDER BY (CompanyID, ReferenceID, DetailID, PostedDate, ID)` · `PARTITION BY (toYYYYMM(PostedDate), cityHash64(CompanyID) % 8)` |
| `eb_staging.stg_general_ledger_detail` (GLD) | 1 dòng chi tiết nợ/có theo tài khoản | cùng kiểu với GL; có cột `CompanyID` riêng (xem [04](04-nguon-du-lieu-gl-gld.md)) |
| `eb_staging.stg_summary_opening_balance` | số dư đầu kỳ khai báo theo (công ty, tài khoản, kỳ, sổ, loại tiền) | `MergeTree` · `ORDER BY (CompanyID, PeriodID, Account, TypeLedger, CurrencyID)` |
| `eb_dwh.fact_account_opening_period` | số dư chốt kỳ theo (công ty, tài khoản, kỳ, sổ, loại tiền) | `MergeTree` · cùng khoá như trên |
| `eb_dwh.dim_account` | 1 phiên bản hiệu lực của một tài khoản | `ReplacingMergeTree` · `ORDER BY (company_id, account_number)` |
| `eb_dwh.bridge_org_scope` | 1 cặp (đơn vị gốc, đơn vị con) | `ORDER BY (root_org_id, child_org_id)` |
| `eb_dwh.bridge_account_hierarchy` | 1 cặp (tài khoản tổ tiên, tài khoản hậu duệ) | `ORDER BY (company_id, ancestor_account_number, descendant_account_number)` |
| `eb.repository_ledger` | 1 dòng nhập/xuất kho | `ReplacingMergeTree` · `ORDER BY (CompanyID, PostedDate, ReferenceID, DetailID, ID)` |
| `eb.repository_ledger_checkpoint` | số dư lũy kế **đến hết tháng** `month` của (công ty, kho, vật tư, sổ) | `ReplacingMergeTree(covered_upto_ts)` · `ORDER BY (company_id, repository_id, material_goods_id, type_ledger, month)` |

Cột chính của `fact_gl_entry_line` mà sổ dùng: `line_bk` (= `GLD.ID` dạng chuỗi), `company_id`, `reference_id`, `detail_id`,
`posted_date`, `voucher_date`, `type_id`, `type_ledger`, `account_number`, `account_corresponding`, `currency_code`, `exchange_rate`,
`debit_amount`, `credit_amount`, `debit_amount_original`, `credit_amount_original` (`Decimal(25,10)`, **Nullable**), `no_fbook`, `no_mbook`,
`reason`, `description`, `invoice_no`, `invoice_date`, `order_priority`, `is_unreasonable_cost`, `custom_field1..5`, `custom_field_detail1..5`,
`accounting_object_id`, `cluster_id`, `is_deleted`, `__version`.

## 1.4 Quy ước chung (vi phạm là sai số âm thầm)

1. **`FINAL` + bộ lọc xoá.** Mọi bảng trên là `ReplacingMergeTree`. Truy vấn phải có `FINAL` (hoặc tương đương) **và** lọc xoá:
   fact dùng `is_deleted = 0`; staging dùng `__deleted = 0`; raw ledger dùng `__deleted = 0`.
   Với truy vấn đầu kỳ quét mọi partition từ đầu lịch sử, thiếu `FINAL` nghĩa là tháng nào còn part chưa merge sẽ cộng trùng, và vì quá trình merge
   bất định nên **hai lần chạy cùng tham số có thể ra hai con số khác nhau**.
2. **`type_ledger`.** `0` = sổ tài chính, `1` = sổ quản trị, `2` = bút toán dùng chung **cả hai** sổ.
   Điều kiện luôn là `(type_ledger = X OR type_ledger = 2)`. Ở sổ chi tiết tài khoản: `IsFinancialBook = 1` → `type_ledger 0` và cột số chứng từ `no_fbook`;
   ngược lại → `type_ledger 1` và `no_mbook`. Sổ chi tiết vật liệu hiện cố định `TypeLedger = 0 OR 2`.
3. **Số tiền là `Decimal(25,10)` và Nullable.** Luôn `coalesce(x, 0)` khi cộng. Qua gRPC truyền **dưới dạng chuỗi** (không dùng `double`, mất độ chính xác); Java
   dựng `BigDecimal` từ chuỗi.
4. **Hai cặp cột tiền.** `*_amount_original` = nguyên tệ; `*_amount` = quy đổi. Cách đặt tên cột kết quả theo từng sổ xem trong `types.go` và proto của sổ đó.
5. **UUID bị swap giữa SQL Server và ClickHouse.** SQL Server lưu `uniqueidentifier` với ba nhóm đầu little-endian. Việc swap do **Java** làm
   (`SqlServerUuidSwap`) trước khi gọi Go; Go chỉ nhận UUID chuẩn và không swap lại.
6. **`cluster_id`.** Hệ thống đa cluster (ví dụ `EB99_GR1`, `EB99_GR2`). Có filter `CLUSTER_FILTER`; rỗng = không lọc.
7. **Tra mã/tên bằng dictionary, không đọc cột snapshot.** `accounting_object_code/name` trong fact là ảnh chụp lúc ETL, **không tự cập nhật** khi
   đối tượng đổi mã/tên. Dùng `dictGetOrDefault('eb.dict_accounting_object', 'accounting_object_code', accounting_object_id, '')`.
8. **Tên tài khoản theo năm.** `dim_account` có `account_name_vi` (dữ liệu trước 2026) và `account_name_99` (kỳ từ 2026, theo thông tư mới). Code chọn cột theo năm của
   `FromDate` (`accountNameColumn`). Comment trong code ghi cột `account_name_99` "chưa tồn tại" — **đã cũ**: trên server test hiện cột này có. Hãy xác nhận trên PROD trước khi bật.
9. **Chỉ lọc theo khoá sắp xếp mới nhanh.** Fact sắp theo `company_id, posted_date, account_number` nên luôn có `company_id` và khoảng `posted_date` trong `WHERE`.

## 1.5 Quan hệ giữa các bảng mà code đang dùng

```
fact_gl_entry_line (hoặc staging_source) ──▶ chi tiết + đầu kỳ
dim_account       ──▶ account_group_kind (loại dư Nợ/Có/lưỡng tính), tên tài khoản
bridge_account_hierarchy + dim_account ──▶ mở rộng tài khoản cha xuống tài khoản lá (IsParentNode = true)
bridge_org_scope  ──▶ mở rộng thêm các đơn vị phụ thuộc (IsDependent = 1)
dict_*            ──▶ tên/mã đối tượng, kho, vật tư, đơn vị tính
```

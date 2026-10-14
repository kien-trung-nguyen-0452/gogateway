# 4. Đổi nguồn dữ liệu: fact ↔ staging (GL/GLD)

> **Danh sách công ty chạy staging (4.3) và `queryCtx`/`grace_hash` (4.5) chỉ có ở nhánh `prod`**; nhánh `dev` hiện chỉ có cờ toàn cục `SO_CHI_TIET_TAI_KHOAN_DATA_SOURCE`. Xem [README](README.md).

## 4.1 Vì sao có hai nguồn

| | **fact** (mặc định) | **staging** (dự phòng) |
|---|---|---|
| Bảng | `eb_dwh.fact_gl_entry_line` | `eb_staging.stg_general_ledger` (GL) ⨝ `eb_staging.stg_general_ledger_detail` (GLD) |
| Dựng | DWH denormalize sẵn header + detail | JOIN lúc query theo đúng khoá của proc gốc |
| Chi phí query | rẻ (một bảng, không JOIN) | đắt hơn (JOIN hàng chục triệu dòng) |
| Dùng khi | bình thường | pipeline fact lỗi/trễ, hoặc nhóm tester muốn đọc trực tiếp từ staging |

Khoá JOIN trùng với proc gốc: `GL.DetailID = GLD.DetailID AND GL.ReferenceID = GLD.ReferenceID`.

Mục tiêu thiết kế: **toàn bộ placeholder và logic dựng SQL ở `service.go` dùng chung nguyên vẹn cho cả hai nguồn**, nên code báo cáo không cần biết đang đọc nguồn nào.

## 4.2 Cơ chế "nguồn" trong code

Trong `query.sql` và `query_opening.sql`, mệnh đề `FROM` là một placeholder `{{GL_SOURCE}}` và **bí danh bảng luôn là `f`**:

```sql
FROM {{GL_SOURCE}}
WHERE f.company_id IN ... AND f.posted_date ... AND f.is_deleted = 0
```

`service.go` thay placeholder này bằng một trong hai biểu thức:

```go
const factSourceExpr = "eb_dwh.fact_gl_entry_line AS f FINAL"   // nguồn fact
//go:embed staging_source.sql
var stagingSourceExpr string                                    // subquery (...) AS f
```

`applyCommonPlaceholders(..., useStaging)` chọn một trong hai. `useStaging` do `Service.stagingFor(p)` quyết định **theo từng request**.

## 4.3 Bật nguồn staging

| Cách bật | Phạm vi | Ghi chú |
|---|---|---|
| `SO_CHI_TIET_TAI_KHOAN_DATA_SOURCE=staging` | **toàn bộ** request của sổ tài khoản | khi pipeline fact lỗi diện rộng |
| `SO_CHI_TIET_TAI_KHOAN_STAGING_COMPANY_IDS=<uuid>,<uuid>` | **chỉ các công ty trong danh sách** (so khớp `PrimaryCompanyID`, không phân biệt hoa thường) | cho nhóm tester/công ty thí điểm |
| `SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE=staging` | toàn bộ sổ quỹ | **chưa có** danh sách theo công ty |

Khi cờ toàn cục là `staging`, danh sách theo công ty bị bỏ qua. Đổi biến môi trường rồi khởi động lại; **không cần build lại**. Khi khởi động, service ghi log nguồn đang dùng và số công ty trong danh sách.

Nếu làm sổ mới: **làm luôn danh sách theo công ty ngay từ đầu** (sổ tài khoản làm sau nên mới có), vì đó là cách thí điểm an toàn nhất.

## 4.4 Hợp đồng của một nguồn thay thế

Mọi nguồn phải là một subquery có bí danh `f` và trả **đúng tập cột mà `query*.sql` của sổ đó đang tham chiếu**, cùng tên và cùng kiểu với fact. Với sổ tài khoản (xem `staging_source.sql`):

`line_bk, reference_id, type_id, type_ledger, posted_date, voucher_date, no_fbook, no_mbook, invoice_no, invoice_date, reason, description, account_number,
account_corresponding, debit_amount, credit_amount, debit_amount_original, credit_amount_original, exchange_rate, order_priority, is_unreasonable_cost,
accounting_object_id, custom_field1..5, custom_field_detail1..5, company_id, currency_code, cluster_id, is_deleted`

Các điểm cần đúng:
- `line_bk` = `toString(gld.ID)`; đổi tên cột PascalCase của staging sang snake_case của fact (`gl.TypeLedger AS type_ledger`...).
- `is_deleted` là cột **giả bằng 0** (`0 AS is_deleted`) và việc lọc xoá thật nằm ở `WHERE gl.__deleted = 0 AND gld.__deleted = 0` bên trong subquery. Nhờ vậy điều kiện `f.is_deleted = 0` ở `query*.sql` vẫn chạy mà không phải sửa.
- `FINAL` trên **cả hai** bảng staging vì đều là `ReplacingMergeTree(__source_ts_ms)`.
- **Chỉ lộ `accounting_object_id`**, không đọc `AccountingObjectCode/Name` của GLD (đó là snapshot đóng băng); mã/tên tra bằng dictionary dùng chung cho cả hai nguồn.
- Mỗi sổ có `staging_source.sql` riêng và danh sách cột **có thể khác** (file của sổ quỹ dài hơn). Luôn đối chiếu với `query*.sql` của chính sổ đó.

## 4.5 Hiệu năng của nhánh staging (đã đo, đã thử)

Đo trên server test với công ty thật (khoảng 600 nghìn dòng header, 12 tài khoản):

1. **Chỉ lọc `company_id` ở GL và `account_number` ở GLD không đủ.** Mã `131`, `632`... dùng chung bởi hàng chục công ty (đo được 51 công ty cùng dùng 12 mã), nên lọc `account_number` một mình khớp khoảng **73% toàn bảng GLD** (10,6/14,5 triệu dòng) và ClickHouse dựng hash table JOIN trên ngần ấy dòng.
2. **Cách sửa thật: GLD cũng có cột `CompanyID`**, thêm `gld.CompanyID IN (...)` ngay cạnh `gld.Account` trong `WHERE`. Chọn lọc đủ mạnh trước khi JOIN: từ **4,2-5,0 s** xuống còn **0,8-1,7 s**, kết quả giống hệt (đã đối chiếu `count()`).
3. **Đã thử và không giúp thêm:** đẩy cận dưới ngày vào subquery; `OPTIMIZE TABLE ... FINAL` trên hai bảng (khoảng 980 partition, mỗi partition vốn một part, không còn gì để gộp); bật/tắt `FINAL`.
4. **Bộ nhớ.** JOIN trên prod có thể hàng trăm triệu dòng và từng `MEMORY_LIMIT_EXCEEDED` khi test. `queryCtx(ctx, staging)` gắn vào **chỉ nhánh staging**:
   ```go
   "join_algorithm":    "grace_hash",
   "max_bytes_in_join": 1 << 30,   // 1 GB
   ```
   `grace_hash` chia JOIN thành bucket và spill ra đĩa khi vượt ngưỡng, **không đổi kết quả**. Đã kiểm: kết quả giống hệt mặc định (md5 sau sắp xếp), ngưỡng 1 GB chưa chạm nên chậm hơn ~0,7 s ở truy vấn đầu kỳ; ép ngưỡng 64 MB thì RAM giảm khoảng một nửa nhưng chậm 3-4 lần và spill 8 lần.
   Cần ClickHouse đủ mới (`grace_hash` từ khoảng 22.12). Server test là 26.3.33; **xác nhận phiên bản PROD** trước khi bật, nếu không truy vấn báo "unknown setting".
5. **`ORDER BY` cuối truy vấn chi tiết cũng ăn RAM.** Khi chạy thử trên prod với công ty lớn nhất (10,1 triệu dòng trả về), lần đầu chết `MEMORY_LIMIT_EXCEEDED` ở bước sắp xếp (`MergeSortingTransform`, 6,1 GB), không phải ở JOIN (build chỉ 2,7 triệu dòng). Hướng cần thử (**chưa đo**): giới hạn sắp xếp ra đĩa (`max_bytes_before_external_sort`), hoặc cho client yêu cầu khoảng ngày nhỏ hơn.
6. **Đầu kỳ chỉ cần dữ liệu `< FROM_DATE`**, nhưng `staging_source.sql` hiện cắt `PostedDate <= TO_DATE` (dùng chung cho cả chi tiết lẫn đầu kỳ). Ứng viên tối ưu: truyền cận trên riêng cho nhánh đầu kỳ. **Chưa thực hiện.**

> Hai lưu ý khi đo: công cụ/proxy dùng để chạy truy vấn trên prod có thể tự **thử lại sang node khác** khi lỗi (cùng `query_id`), làm tải nhân đôi mà người dùng không thấy lỗi; và có công cụ chỉ chấp nhận định dạng `RowBinaryWithNamesAndTypes` nên không chạy được `FORMAT Null` (bọc `SELECT count(), sum(cityHash64(tuple(*)))` để chỉ nhận một dòng).

## 4.6 Khi thêm sổ mới có hai nguồn: thứ tự việc nên làm

1. Viết bản fact trước, đối chiếu với proc.
2. Viết `staging_source.sql` với cùng tập cột, **kèm `gld.CompanyID` ngay từ đầu**.
3. Thêm cờ vào `config.go` theo quy ước (mặc định `fact`) và danh sách công ty.
4. Đối chiếu fact và staging trên cùng tham số: số dòng, tổng Nợ/Có theo tài khoản, đầu kỳ.
5. Đo thời gian và RAM đỉnh trên công ty lớn nhất; đảm bảo nhánh staging có `queryCtx`.
6. Ghi log nguồn đang dùng khi khởi động.

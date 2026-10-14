# 3. Chuyển một stored procedure sang Go: quy trình, grain, các bẫy

> Mục về `ORDER BY` có khoá phụ tất định (`orderTailDetail`, `orderNumberExpr`) mô tả code ở nhánh **`prod`**; nhánh `dev` hiện chỉ có đuôi `no`. Xem [README](README.md).

Hai sổ đã chuyển: `Proc_SO_CHI_TIET_CAC_TAI_KHOAN` (sổ chi tiết tài khoản) và `Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT` (sổ quỹ). Sổ vật liệu đọc từ
`eb.repository_ledger` theo cùng khuôn. Đây là quy trình rút ra từ các sổ đó.

## 3.1 Quy trình

1. **Đọc proc, vẽ ra các bước.** Một proc thường trộn 4-5 việc: xác định phạm vi (công ty, tài khoản), tra danh mục, tính đầu kỳ, lấy chi tiết, dựng dòng tổng/số dư cuối. Gạch ra từng việc.
2. **Gán mỗi việc vào một grain** (3.2) và chọn nơi xử lý: SQL hay Go (3.3).
3. **Ghi tên proc và số dòng** vào comment đầu file `.sql`/hàm Go tương ứng (ví dụ "Nguồn tham chiếu: Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT dòng 458-519").
   Người sau cần đối chiếu được. Khi lệch với proc, comment phải nói **lệch ở đâu và vì sao**.
4. **Viết SQL theo khuôn template** ([02](02-go-gateway-cau-truc-code.md) mục 2.4), `service.go` dựng và chạy.
5. **Đối chiếu với proc** trên cùng tham số: đếm dòng, tổng Nợ/Có theo tài khoản, số dư đầu kỳ, số dư cuối kỳ, dòng đầu/cuối (checklist ở [09](09-checklist-so-moi.md)).
6. Đưa vào cờ nguồn dữ liệu nếu có thể phải lùi về nguồn khác ([04](04-nguon-du-lieu-gl-gld.md)).

## 3.2 Tách proc theo grain

**Grain** = một dòng kết quả đại diện cho cái gì. Tách theo grain để mỗi truy vấn làm đúng một việc, chạy được song song, và không cộng trùng.

Ví dụ sổ chi tiết tài khoản:

| Việc | Grain của kết quả | Nơi làm | Ghi chú |
|---|---|---|---|
| Phạm vi công ty | 1 dòng / công ty | SQL nhỏ `resolveCompanyScope` | `bridge_org_scope` |
| Mở rộng tài khoản | 1 dòng / tài khoản lá | SQL nhỏ `expandAccounts` | `bridge_account_hierarchy` + `dim_account` |
| Tên và loại tài khoản | 1 dòng / tài khoản | SQL nhỏ `resolveAccountInfo` | `dim_account`, `is_current = 1` |
| **Đầu kỳ** | **1 dòng / tài khoản** | `query_opening.sql` | tổng hợp trước `FROM_DATE`, xem [05](05-so-du-dau-ky.md) |
| **Chi tiết** (`GroupSameItem = 0`) | **1 dòng / dòng bút toán (GLD)** | `query.sql` | không `GROUP BY` |
| **Chi tiết gộp** (`GroupSameItem = 1`) | **1 dòng / nhóm 19 cột** của proc | `query.sql` + `GROUP BY` + `HAVING` | khớp `GROUP BY` của proc |
| Cộng phát sinh, số dư cuối, tổng cộng | 1 dòng / tài khoản (và 1 dòng toàn báo cáo) | **Go** (walker) | dựng từ luồng dòng chi tiết |

Quy tắc:
- **Việc nào phụ thuộc thứ tự/lũy kế thì làm ở Go trên luồng đã sắp thứ tự**, không ép vào SQL (window function trên hàng triệu dòng từng gây OOM 5,6 GB khi thử gộp đầu kỳ + chi tiết trong một truy vấn).
- **Việc độc lập thì chạy song song** bằng `sync.WaitGroup` (xem hai nhóm `wgScope`, `wgLookup` trong `StreamSoChiTiet`). Đừng cộng dồn các vòng đi-về ClickHouse tuần tự.
- **Đầu kỳ tính riêng một truy vấn, không gộp chung với chi tiết**: grain khác nhau, vùng thời gian khác nhau (`< FROM_DATE` so với `[FROM_DATE, TO_DATE]`), và sau này sẽ đổi nguồn.
- **Đầu kỳ chỉ trả "phát sinh trực tiếp vào đúng mã tài khoản"** (`GROUP BY account_number`). Việc gom tài khoản cha lên tổng của nhánh con làm ở Go (`rollupOpeningBalances` ở sổ quỹ). Làm rollup trong SQL **và** trong Go là đếm hai lần.

## 3.3 SQL hay Go?

| Việc | Nên làm ở |
|---|---|
| Lọc, gộp, sắp xếp theo khoá có sẵn trong bảng | SQL |
| Tra mã/tên theo ID | SQL, `dictGetOrDefault` |
| Lọc phạm vi (công ty, tài khoản, kỳ) | SQL, **lọc sớm theo khoá sắp xếp** |
| Số dư lũy kế theo thứ tự, dòng tổng, dòng số dư cuối | Go (walker) |
| Đổi kiểu hiển thị (chuỗi ngày, `Decimal` → chuỗi) | Go (`rowToProto`, `Row`) |
| Quy tắc phân loại phức tạp theo từng dòng | SQL nếu biểu diễn được bằng `CASE`/`if`; ví dụ `orderNumberExpr` ("thu trước chi sau") |

## 3.4 Các bẫy đã gặp (mỗi cái là một lần sai số hoặc lỗi thật)

**Lọc và gộp**
- **`WHERE` khác `HAVING`.** Proc lọc số tiền khác 0 bằng `HAVING` (sau khi gộp). Nhóm gồm +100 và −100 có tổng 0 bị loại; nếu lọc ở `WHERE` thì cả hai dòng đều khác 0 nên nhóm được giữ lại. Bút toán điều chỉnh/đảo là chỗ hay lệch.
- **`any()` chỉ cho cột không nằm trong `GROUP BY` của proc.** Trước đây dùng `any()` cho cả cột nằm trong `GROUP BY` làm ClickHouse gộp nhiều hơn OLTP. Khớp **chính xác danh sách cột** `GROUP BY` của proc, ghi lại trong comment (sổ tài khoản khớp 18/19 cột; cột còn thiếu là ngày chứng từ `GL.Date`, cần DWH bổ sung).
- **Quy ước cột đối tượng.** Proc `GROUP BY GLD.AccountingObjectID` (không phải theo code/tên), nên đưa `accounting_object_id` vào `GROUP BY` và tra code/name bằng dictionary.
- **Số tiền khác 0 ở bất kỳ cột nào.** Điều kiện lọc dòng chi tiết xét cả 4 cột (Nợ, Có, Nợ nguyên tệ, Có nguyên tệ), còn đầu kỳ chỉ xét hai cột quy đổi. Giữ đúng theo proc, đừng "đồng nhất" cho gọn.

**Kiểu và tên cột**
- **Alias SELECT che cột gốc cùng tên** và gây lỗi ClickHouse `Code 184` khi cột trần xuất hiện ở `WHERE`. Luôn qualify cột nguồn bằng `f.`.
- **`concat()` với đối số Nullable trả về `Nullable(String)`**, khác `String` thuần ở nhánh kia, nên `scanRow` lệch kiểu. `coalesce` từng đối số.
- **Hai chế độ (chi tiết / gộp) phải trả cùng kiểu cột** vì chỉ có một `scanRow` đọc theo vị trí.
- **`Nullable(Date)` so sánh với `NULL` ra `NULL`**: dòng bị loại. Với lọc theo ngày chứng từ (`voucher_date`) điều này **khớp** proc (`NULL BETWEEN ... AND ...` cũng bị loại). Đừng "sửa" thành giữ lại.

**Ngày**
- **Ngày hạch toán (`posted_date`) khác ngày chứng từ (`voucher_date`).** Ở **sổ quỹ tiền mặt**, chế độ chi tiết lọc kỳ theo ngày chứng từ (`voucherDateFilter` trong `service.go`)
  còn chế độ gộp chỉ lọc theo ngày hạch toán, vì proc ở chế độ gộp gán `PostedDate`/`Date` theo tên cột. Đã đối chiếu: phiếu hạch toán 22/07/2026 nhưng chứng từ ngày 12/03/2025 bị proc loại
  khỏi kỳ 07/2026 trong khi DWH vẫn hiện nếu lọc sai cột. Đọc kỹ proc để biết lọc cột nào ở chế độ nào, đừng suy ra từ sổ khác.

**Thứ tự dòng là nghiệp vụ**
- `ORDER BY` quyết định số dư lũy kế của từng dòng. Thứ tự chi tiết của sổ tài khoản theo proc: `AccountNumber, OrderType, PostedDate, Date, OrderNumber, No, OrderPriority`; code thêm `reference_id`, `key_id` (và `account_corresponding` ở chế độ gộp) làm **khoá phụ để thứ tự tất định**: nếu thiếu, các dòng ngang nhau về khoá chính sẽ "hoà" và nhảy vị trí, làm số dư lũy kế lệch ở đúng dòng đó.
- Sổ tài khoản trả theo **thứ tự tài khoản người dùng chọn** (bên Java dùng `LinkedHashSet` để giữ). Walker dựa vào việc các dòng cùng tài khoản nằm liền nhau.
- **Kiểm tra tính tất định** sau khi viết `ORDER BY` mới: chạy cùng tham số 2-3 lần và so dãy `key_id`. (Đã kiểm trên server test ngày 2026-10-13: sổ tài khoản, 1,48 triệu dòng, 3 lần chạy cho dãy `key_id` giống hệt.)
- Muốn đổi `ORDER BY` thì phải đối chiếu lại số dư lũy kế với proc, và báo bên Java (một số consumer dựa vào thứ tự, xem [07](07-be-java.md)).

**Hiệu năng**
- Luôn lọc `company_id` + khoảng ngày để dùng khoá sắp xếp. Mã tài khoản như `131`, `632` **dùng chung giữa hàng chục công ty**, nên chỉ lọc `account_number` gần như không loại được gì.
- Thiếu `FINAL`: sai số; có `FINAL` trên bảng rất lớn: chậm. Cân nhắc `FINAL` chỉ ở tầng cần ([04](04-nguon-du-lieu-gl-gld.md)).

## 3.5 Dùng dim và bridge

- `dim_account`: **luôn** `is_current = 1`; chọn cột tên theo năm báo cáo (`account_name_vi` / `account_name_99`).
- Mở rộng tài khoản cha: nếu bridge chưa build xong thì `expandAccounts` trả về danh sách gốc (giống bản Java cũ) thay vì rỗng. Giữ hành vi này.
- Đơn vị phụ thuộc: `bridge_org_scope` với `child_org_id != root_org_id` rồi hợp với danh sách công ty gốc và khử trùng.

## 3.6 Đối chiếu với proc: nên so những gì

Với một bộ tham số (công ty, kỳ, tài khoản, sổ, loại tiền):
1. Số dòng chi tiết.
2. Tổng Nợ, tổng Có theo từng tài khoản (cả quy đổi và nguyên tệ).
3. Số dư đầu kỳ và số dư cuối kỳ theo từng tài khoản.
4. Dãy 20 dòng đầu và 20 dòng cuối theo thứ tự.
5. Lặp lại ở chế độ gộp (`GroupSameItem = 1`), tài khoản cha (`IsParentNode`), có đơn vị phụ thuộc (`IsDependent = 1`), loại tiền khác `TH`, sổ tài chính và sổ quản trị.
6. Thử công ty lớn nhất và công ty nhỏ/trống (kết quả rỗng phải ra rỗng, không lỗi).

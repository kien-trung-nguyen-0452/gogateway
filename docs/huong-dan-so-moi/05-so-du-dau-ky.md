# 5. Số dư đầu kỳ: hiện trạng, cách tách, và đề xuất bảng `summary_opening_balance`

> **Phần 5.1-5.4 là hiện trạng (đã kiểm trên code và server test ngày 2026-10-13). Phần 5.5 trở đi là ĐỀ XUẤT, chưa triển khai.**
> Nếu đụng vào bảng `repository_ledger_checkpoint` hoặc luồng tính giá xuất kho: **đọc `COSTING.md` trước** và cập nhật nó trong cùng lần sửa, theo `CLAUDE.md`.

## 5.1 Hiện trạng đầu kỳ ở từng sổ

| Sổ | Cách tính đầu kỳ hiện tại | File |
|---|---|---|
| Chi tiết tài khoản | `SUM` Nợ/Có theo từng tài khoản của mọi dòng `posted_date < FROM_DATE`, đọc từ `{{GL_SOURCE}}` (fact hoặc staging) | `query_opening.sql` |
| Quỹ tiền mặt | Như trên, thêm `FOREIGN_AMOUNT_FILTER` khi chọn ngoại tệ. Gom tài khoản cha làm ở Go (`rollupOpeningBalances`) | `query_opening.sql` |
| Chi tiết vật liệu | CTE `opening_balance` trong `query.sql`: cộng dồn `eb.repository_ledger` với `PostedDate < FROM_DATE` (nhánh `raw`, mặc định); hoặc nhánh `checkpoint` | `query.sql`, `opening_balance_checkpoint.sql` |
| Tính giá xuất kho | Tồn đầu kỳ từ raw hoặc checkpoint, có bất biến riêng. **Nhánh checkpoint và `COSTING.md` chỉ có ở nhánh `dev`** (xem [README](README.md)) | `internal/datasets/tinh-gia-xuat-kho/`, `COSTING.md` |

Đặc điểm chung: **mỗi request quét lại toàn bộ lịch sử trước `FROM_DATE`** của các tài khoản/khoá được hỏi. Với dữ liệu nhỏ thì nhanh; trên PROD
(hàng trăm triệu dòng) đây là phần tốn kém nhất của truy vấn và cũng là phần chiếm bộ nhớ JOIN ở nhánh staging.

## 5.2 Hai phương pháp xác định đầu kỳ (lựa chọn của người dùng)

Frontend có tuỳ chọn `openingBalanceMethod`:

1. **"Theo số liệu tổng hợp từ chứng từ"** (mặc định): cộng dồn toàn bộ chứng từ trước kỳ. Đây là cái code đang làm.
2. **"Theo kết chuyển số dư cuối năm trước"**: dùng số dư đã kết chuyển. OLTP có proc riêng (`Proc_SO_QUY_TIEN_MAT_THEO_KET_CHUYEN_SO_DU_CUOI_NAM` cho sổ quỹ).
   Nguồn DWH tương ứng là `eb_dwh.fact_account_opening_period`. **Chưa triển khai** ở go-gateway.

**Hai phương pháp không phải cái đúng cái sai.** Không được "tối ưu" phương pháp [1] bằng cách đọc bảng chốt kỳ của [2]: đó là làm sai phương pháp mặc định.
Mọi nguồn đầu kỳ mới (kể cả `summary_opening_balance` ở 5.6) phải tạo **ra cùng con số** với phương pháp [1] khi dùng để thay thế cách cộng dồn, hoặc phải gắn rõ vào một lựa chọn riêng.

Đã có ghi chú trong code về việc triển khai [2]: thêm `OpeningBalanceMethod` vào `QueryParams`, đọc từ request, rẽ nhánh trong `loadOpeningBalances()`. Lưu ý:
comment ở `query_opening.sql` của sổ quỹ nhắc tới `query_opening_period.sql`, nhưng **file này chưa tồn tại** trong repo.

## 5.3 Các bảng liên quan đầu kỳ đang có (server test)

| Bảng | Số dòng | Khoá | Ghi chú |
|---|---|---|---|
| `eb_raw.raw_summary_opening_balance` | 16.091 | `ReplacingMergeTree` | bản sao thô |
| `eb_staging.stg_summary_opening_balance` | 15.870 (40 công ty) | `(CompanyID, PeriodID, Account, TypeLedger, CurrencyID)` | cột: `CompanyID, Account, PeriodID, TypeID, TypeLedger, DebitAmount, CreditAmount, DebitAmountOriginal, CreditAmountOriginal, CurrencyID, TypeGroupID, cluster_id` |
| `eb_dwh.fact_account_opening_period` | **1.468** | `(company_id, period_id, account_number, type_ledger, currency_code)` | chỉ khoảng 1/10 số dòng của staging: **ETL chưa nạp đủ**, cần kiểm trước khi dựa vào |

Quan sát dữ liệu staging: `TypeLedger` có 0/1/2; `TypeGroupID` có 1/2/3; `PeriodID` dạng `YYYYMM` (ví dụ 202112, 202412) nhưng **có giá trị bất thường** (cực đại 246206). Ý nghĩa chính xác của
`PeriodID` (kỳ chốt hay kỳ khai báo) và `TypeGroupID` **chưa được xác nhận**: cần hỏi team DWH/OLTP trước khi thiết kế bất cứ thứ gì dựa vào hai cột này.

## 5.4 Bài học từ nhánh checkpoint của vật liệu (đã chạy thật)

Sổ chi tiết vật liệu đã thử mô hình "checkpoint theo tháng + phần lẻ" với `eb.repository_ledger_checkpoint`. Những gì đã gặp, nên áp dụng cho mọi bảng đầu kỳ mới:

1. **Cách đọc:** lấy checkpoint của tháng gần nhất `< tháng(FROM_DATE)` (`argMax(..., month)`, **không** `sum`/`max`, **không** so bằng), rồi cộng "phần lẻ" của raw từ sau tháng đó đến `FROM_DATE`. Khoá chưa có checkpoint → quét từ đầu lịch sử.
   (Quy tắc chi tiết ở `COSTING.md` mục 6, 7.)
2. **Phải chặn theo tập khoá của phạm vi** (`requested_keys` từ `ledger_scope`); bảng checkpoint không có các cột để áp filter nghiệp vụ, thiếu chốt chặn là trả đầu kỳ của mọi kho/vật tư trong công ty (`COSTING.md` mục 8).
3. **Đã gặp lỗi sai âm thầm do đổi đơn vị:** số lượng trong checkpoint từng tính bằng `IWQuantity − OWQuantity` (đơn vị giao dịch) thay vì nhánh `if(UnitID = MainUnitID, ...)` theo đơn vị chính; sai hai bậc độ lớn ở khoảng 4% dòng. Đã sửa ở job Flink và đối chiếu lại. Nghĩa là: **bất kỳ đại lượng dẫn xuất nào trong bảng tổng hợp phải dùng đúng công thức của truy vấn gốc**, và phải đối chiếu số học chính xác với truy vấn gốc trước khi tin.
4. **Chưa có "guard" độ cũ.** Nhánh checkpoint hiện không so `covered_upto_ts` với dữ liệu raw; nếu job nạp checkpoint trễ hoặc chết thì **âm thầm thiếu phát sinh mới nhất** thay vì lùi về raw. Vì vậy mặc định vẫn là `raw` và **không được dùng làm mặc định ở PROD** khi chưa có guard.
5. **Khoá sạch/bẩn phải truy vấn tách riêng rồi `UNION ALL`**, không gộp vào một truy vấn dùng chung mốc tháng: bản thử gộp (một mốc tháng toàn cục) chậm hơn quét thẳng.
6. **Ở quy mô test, checkpoint không nhanh hơn raw** (một truy vấn toàn công ty: 1,54 s so với 0,75 s). Lợi ích chỉ xuất hiện khi lịch sử đủ lớn (PROD). Đừng kỳ vọng thấy cải thiện trên server test.
7. ⚠ **Rủi ro đã biết, cần xử lý TRƯỚC khi bật `SO_CHI_TIET_VAT_LIEU_OPENING_BALANCE_SOURCE=checkpoint`:** bảng `eb.repository_ledger_checkpoint` hiện có `type_ledger` **trong khoá sắp xếp** và chứa cả ba giá trị 0, 1, 2
   (server test ngày 2026-10-13: 164.106 / 18.021 / 134.895 dòng; **118.564** cặp (khoá, tháng) có nhiều hơn một `type_ledger`).
   `opening_balance_checkpoint.sql` hiện **không lọc và không tách theo `type_ledger`**, nên `argMax(cumulative_*, month)` gộp lẫn các sổ trong cùng khoá và cho kết quả sai. Phải viết lại theo từng `type_ledger`
   (sổ tài chính lấy 0 và 2, mỗi loại có thể có tháng checkpoint mới nhất khác nhau, nên phần lẻ cũng phải tính theo từng loại; xem `COSTING.md` mục 4). Cờ mặc định là `raw` nên đường chạy hiện tại **không bị ảnh hưởng**.

## 5.5 Tách phần đầu kỳ trong code (làm được ngay, không đổi hành vi)

Mục tiêu: đầu kỳ nằm sau **một điểm vào duy nhất** để cắm nguồn mới mà không đụng phần chi tiết. Hiện sổ tài khoản/quỹ đã có `loadOpeningBalances(ctx, companyIDs, accounts, p, typeLedger) → map[string]balance`; giữ nguyên chữ ký này làm ranh giới.

Việc nên làm khi chuẩn bị:

1. **Giữ `query_opening.sql` như hiện tại là nhánh `raw`** (mặc định, là phương pháp [1]). Không sửa nghiệp vụ trong lần tách này.
2. **Gói thành một hàm chọn nguồn** theo cờ môi trường (cùng quy ước ở [02](02-go-gateway-cau-truc-code.md) mục 2.6: mặc định = hành vi đang chạy):
   ```go
   // Đề xuất, minh hoạ ý tưởng; không chép nguyên văn.
   func (s *Service) loadOpeningBalances(ctx, companyIDs, accounts, p, typeLedger) (map[string]balance, error) {
       switch s.openingSource {            // đọc từ config; mặc định "raw"
       case "summary": return s.openingFromSummary(ctx, ...)   // 5.6, kèm guard + lùi về raw
       default:        return s.openingFromRaw(ctx, ...)       // query_opening.sql hiện tại
       }
   }
   ```
3. **Đầu ra phải đúng grain cũ:** 1 dòng / tài khoản, `balance{Debit, Credit, DebitOrig, CreditOrig}`. Mọi nguồn mới trả đúng cấu trúc này để phần walker và `rollupOpeningBalances` không đổi.
4. **Phần "phát sinh trực tiếp vào đúng mã"** (không rollup) là trách nhiệm của nguồn đầu kỳ; rollup cha-con làm **một lần** ở Go.
5. **Cho nhánh đầu kỳ cận trên riêng** khi đọc staging (xem [04](04-nguon-du-lieu-gl-gld.md) mục 4.5 điểm 6).
6. **Viết bộ đối chiếu** (`raw` vs nguồn mới) dùng chung, chạy được bằng một lệnh (5.8). Có nó trước khi có bảng mới.

## 5.6 ĐỀ XUẤT: bảng `summary_opening_balance`

> **Chưa có quyết định chốt.** Tên "summary_opening_balance" có thể được hiểu theo hai cách, cần anh em xác nhận trước khi làm:
> **(A)** bảng **tổng hợp tính sẵn** từ sổ cái theo tháng để thay phép cộng dồn toàn lịch sử (giống mô hình checkpoint của vật liệu), hoặc
> **(B)** bảng **số dư đầu kỳ khai báo** lấy từ OLTP (`SummaryOpeningBalance`, hiện là `stg_summary_opening_balance`/`fact_account_opening_period`) phục vụ phương pháp [2].
> Phần dưới mô tả **(A)**. (B) đã có bảng nền (5.3) nhưng cần xác nhận ý nghĩa `PeriodID`/`TypeGroupID` và nạp đủ ETL.

**Ý tưởng (A).** Lưu số dư lũy kế **đến hết tháng** `period` cho từng khoá; đầu kỳ của báo cáo = checkpoint gần nhất trước tháng `FROM_DATE` + phần lẻ raw từ sau tháng đó đến `FROM_DATE`.

Schema đề xuất (mô phỏng `repository_ledger_checkpoint`):

```
summary_opening_balance
  company_id              UUID
  account_number          String
  type_ledger             Int32      -- 0 / 1 / 2, giữ ĐÚNG ba giá trị, không gộp
  currency_code           String
  cluster_id              LowCardinality(String)
  period                  UInt32     -- YYYYMM; số dư LŨY KẾ đến HẾT tháng này
  debit_amount, credit_amount, debit_amount_original, credit_amount_original   Decimal(25,10)
  covered_upto_ts         Int64      -- mốc nguồn thật sự đã đọc (không phải now()/watermark)
  ENGINE ReplacingMergeTree(covered_upto_ts)
  ORDER BY (company_id, account_number, type_ledger, currency_code, period)
```

Lý do các quyết định:
- **`currency_code` và `cluster_id` nằm trong khoá** vì `query_opening.sql` đang áp `CURRENCY_FILTER` và `CLUSTER_FILTER`; thiếu thì không tái hiện được số của phương pháp [1] khi người dùng lọc theo loại tiền.
- **Hai cặp cột tiền** (quy đổi và nguyên tệ) vì cả hai cùng được báo cáo.
- **Điều kiện dòng khác 0** của đầu kỳ chỉ xét hai cột quy đổi (khác với chi tiết xét bốn cột): bảng tổng hợp phải cộng **cùng tập dòng** với truy vấn gốc, nếu không số khác ở những dòng chỉ có nguyên tệ khác 0.
- **`covered_upto_ts`** là mốc nguồn đã đọc để làm guard độ cũ (5.4 điểm 4).

Đường đọc (ý tưởng, phải kiểm chứng):

```
latest = argMax(giá trị lũy kế, period)  theo (company, account, type_ledger, currency), với period < toYYYYMM(FROM_DATE)
tail   = SUM raw GL  với  posted_date >= đầu tháng SAU latest.period  AND  posted_date < FROM_DATE
opening(account) = latest + tail            -- theo từng type_ledger rồi mới lấy type_ledger IN (X, 2)
guard  = nếu raw có bản ghi MỚI HƠN covered_upto_ts của khoá  →  khoá "bẩn": lùi về raw đầy đủ
```

Các bất biến cần kế thừa (xem `COSTING.md` mục 4, 6, 7, 8, 9; **không chép sang đây**, tham chiếu số mục): `type_ledger = X OR 2`; `argMax` chứ không `sum`/`max`; tra `month < ...` chứ không `=` (tháng khuyết = tháng không phát sinh);
chặn theo tập khoá của phạm vi; `FINAL` trên cả bảng nguồn và bảng tổng hợp.

## 5.7 Kế hoạch triển khai gợi ý (từng bước, mỗi bước có thể dừng)

1. Chốt nghĩa (A) hay (B) và các câu hỏi mở (5.9).
2. Tách `loadOpeningBalances` thành điểm vào duy nhất (5.5), **không đổi hành vi**.
3. Tạo bảng và job nạp (ngoài repo này: Flink/Airflow của team DWH), nạp thử một vài công ty lớn.
4. Viết `openingFromSummary` sau cờ (mặc định `raw`), **có guard + lùi về raw**.
5. Đối chiếu chính xác với `raw` (5.8) trên nhiều công ty × nhiều `FROM_DATE`.
6. Đo thời gian/RAM đỉnh trên dữ liệu cỡ PROD (ở test sẽ không thấy lợi).
7. Bật theo công ty (danh sách như [04](04-nguon-du-lieu-gl-gld.md) mục 4.3) trước khi bật diện rộng.
8. Cập nhật `COSTING.md` nếu đụng bảng/luồng liên quan tính giá, theo `CLAUDE.md`.

## 5.8 Tiêu chí nghiệm thu đối chiếu

- So **`Decimal` chính xác, dung sai bằng 0**, không làm tròn, giữa nguồn mới và `raw` cho từng (công ty, tài khoản, `type_ledger`, loại tiền).
- Nhiều `FROM_DATE`: đầu tháng, giữa tháng, cuối tháng, đầu năm, một mốc trước checkpoint đầu tiên, một mốc trong tháng đang nhận dữ liệu CDC.
- Cả ba `type_ledger`, loại tiền `TH` và một loại tiền cụ thể, có và không có `cluster_id`.
- Khoá không có checkpoint nào, khoá chỉ có phát sinh trong tháng `FROM_DATE`, khoá không có phát sinh nào.
- Mô phỏng job nạp trễ: guard phải lùi về `raw` và ra đúng số.
- Ở ngưỡng sai khác trong tháng đang nhận CDC: sai khác nếu có phải **giải thích được** bằng `covered_upto_ts` trễ, không phải sai công thức.

## 5.9 Câu hỏi mở cần chốt trước khi làm

1. `summary_opening_balance` là (A) tổng hợp tính sẵn hay (B) số dư khai báo từ OLTP? Hay cả hai với vai trò khác nhau?
2. `PeriodID` và `TypeGroupID` trong `stg_summary_opening_balance` nghĩa là gì? Vì sao có `PeriodID = 246206`?
3. Vì sao `fact_account_opening_period` chỉ có 1.468 dòng so với 15.870 ở staging? ETL lọc điều kiện gì?
4. Ai chịu trách nhiệm job nạp (Flink hay Airflow), và chu kỳ nạp/độ trễ chấp nhận được là bao nhiêu?
5. Phiên bản ClickHouse trên PROD (ảnh hưởng `grace_hash` và các tính năng khác)?
6. Sổ quản trị (`type_ledger = 1`) và loại tiền ngoại tệ có cần tối ưu đầu kỳ ngay không, hay chỉ sổ tài chính/VND?

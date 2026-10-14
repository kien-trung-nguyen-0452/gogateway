# Hướng dẫn triển khai sổ mới trên go-gateway

Tài liệu cho các anh em squad chuẩn bị chuyển các sổ/báo cáo từ stored procedure SQL Server sang
đọc DWH ClickHouse qua **go-gateway** (Go) và **BE Java (ebwebbe)**.

> **Nguyên tắc dùng tài liệu này.** Mọi điều ghi ở đây được rút ra từ code và từ truy vấn thật trên
> server test `10.100.120.64` (ngày 2026-10-13). Chỗ nào là **đề xuất** hoặc **chưa xác nhận** thì ghi
> rõ như vậy. Số liệu bảng (số dòng, tên bảng) lấy từ server test; **PROD có thể khác**, hãy kiểm tra
> lại trước khi dựa vào.

## ⚠ Lưu ý về nhánh của go-gateway (đọc trước khi sao chép code)

Tài liệu này được viết khi thư mục làm việc đang ở nhánh **`prod`** (commit `b28d53f`, "Merge branch 'dev' into prod-staging"). **`dev` và `prod` đã rẽ nhau** (tại 2026-10-13: `dev` có 3 commit
`prod` chưa có, `prod` có 12 commit `dev` chưa có; điểm chung `056e880`). Những gì mô tả ở các file sau thuộc nhánh nào:

| Tính năng | `dev` | `prod` |
|---|---|---|
| Danh sách công ty chạy staging (`SO_CHI_TIET_TAI_KHOAN_STAGING_COMPANY_IDS`, `stagingFor`) | ❌ chưa có | ✅ |
| `grace_hash` + `max_bytes_in_join` cho nhánh staging (`queryCtx`) | ❌ | ✅ |
| `ORDER BY` chi tiết có khoá phụ tất định (`orderTailDetail/Grouped`, `orderNumberExpr`) | ❌ (đuôi chỉ là `no`) | ✅ |
| `staging_source.sql` lộ cột `voucher_date` | ❌ | ✅ |
| Nhánh checkpoint cho **tính giá xuất kho** (`opening_stock_raw.sql`, `opening_stock_checkpoint.sql`, biến `TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE`) | ✅ | ❌ |
| `COSTING.md`, `CLAUDE.md` | ✅ | ❌ |
| `query.sql` vật liệu: `dictHas(...)`, `enable_optimize_predicate_expression` | khác nhau | khác nhau (3 dòng) |

Khi một mục trong tài liệu dựa vào tính năng ở một nhánh, mục đó có ghi chú. **Trước khi làm, `git branch` để biết đang ở nhánh nào**, và hỏi người phụ trách nhánh nào là nền của squad.
Muốn so lại: `git diff dev prod -- . ':!docs'`.

## Đọc theo thứ tự

| # | File | Nội dung |
|---|---|---|
| 1 | [01-cau-truc-du-lieu.md](01-cau-truc-du-lieu.md) | Các tầng dữ liệu, các loại bảng, grain của từng bảng, quy ước chung (`FINAL`, `type_ledger`, UUID swap) |
| 2 | [02-go-gateway-cau-truc-code.md](02-go-gateway-cau-truc-code.md) | Cây thư mục Go, vai trò từng file, luồng một request, cấu hình, cách đấu nối |
| 3 | [03-chuyen-proc-sang-go-va-grain.md](03-chuyen-proc-sang-go-va-grain.md) | Quy trình chuyển một proc sang Go, tách theo grain, các bẫy đã gặp |
| 4 | [04-nguon-du-lieu-gl-gld.md](04-nguon-du-lieu-gl-gld.md) | Đổi nguồn `fact` ↔ `staging` (GL/GLD), cơ chế bật theo công ty, hiệu năng JOIN |
| 5 | [05-so-du-dau-ky.md](05-so-du-dau-ky.md) | Tách phần đầu kỳ, các phương pháp, đề xuất bảng `summary_opening_balance` |
| 6 | [06-grpc-va-luong-stream.md](06-grpc-va-luong-stream.md) | Hợp đồng gRPC, luồng streaming, gửi theo lô + nén zstd |
| 7 | [07-be-java.md](07-be-java.md) | Cấu trúc code BE Java, các file dùng chung, cách code một sổ mới |
| 8 | [08-tach-service-rieng.md](08-tach-service-rieng.md) | Quy ước để sau này tách phần báo cáo thành service riêng |
| 9 | [09-checklist-so-moi.md](09-checklist-so-moi.md) | Checklist từng bước làm một sổ mới, từ proc đến go-live |

## Bản đồ nhanh

```
Frontend ──HTTP──▶ BE Java (ebwebbe) ──gRPC server-streaming──▶ go-gateway (Go) ──native :9000──▶ ClickHouse
                    │  build request, đổi UUID,                   │  dựng SQL từ template,           │  eb_dwh / eb_staging / eb
                    │  map proto → DTO, ghi response              │  chạy song song các truy vấn phụ, │
                    ▼                                             │  "walker" dựng số dư lũy kế       │
             StreamingResponseBody                                ▼
                                                        stream từng dòng (hoặc lô) về Java
```

## Những điều quan trọng nhất (đọc nếu chỉ có 2 phút)

1. **go-gateway chỉ đọc**, không ghi dữ liệu. Sai ở đây làm lệch số trên sổ mà **không có exception nào báo ra**.
2. Truy vấn được dựng bằng **thay chuỗi** `{{PLACEHOLDER}}`, không phải tham số ràng buộc. Chỗ nào nhận chuỗi từ client đều phải qua bộ kiểm tra định dạng (UUID, số hiệu tài khoản...) trước khi vào SQL.
3. Mọi bảng ClickHouse ở đây là `ReplacingMergeTree`: thiếu `FINAL` và bộ lọc xoá (`is_deleted = 0` / `__deleted = 0`) là tính trùng hoặc hiện dòng đã xoá.
4. `type_ledger = 2` là bút toán dùng chung cho **cả hai** sổ (tài chính và quản trị): luôn lấy kèm.
5. Tính giá xuất kho (`internal/datasets/tinh-gia-xuat-kho`) có bất biến riêng trong `COSTING.md` cùng thư mục gốc repo. **Không** chép nội dung đó sang tài liệu khác; ai đụng vào luồng đó phải đọc `COSTING.md` trước.

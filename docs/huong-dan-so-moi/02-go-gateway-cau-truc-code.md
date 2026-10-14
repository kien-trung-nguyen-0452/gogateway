# 2. go-gateway: cấu trúc code Go

> Một số mục dưới đây chỉ có ở một trong hai nhánh `dev`/`prod`; xem bảng đối chiếu ở [README](README.md).

Module `softdream.vn/go-gateway`. Chỉ đọc ClickHouse qua native protocol (`:9000`), phục vụ báo cáo và dataset.

## 2.1 Cây thư mục

```
cmd/server/main.go                 Điểm vào: nạp cấu hình, nối ClickHouse, dựng các service, mở HTTP + gRPC, tắt êm
internal/
  config/config.go                 Biến môi trường → struct Config (cờ nguồn dữ liệu, cổng, thông tin ClickHouse)
  chclient/client.go               Tạo kết nối ClickHouse dùng chung (pool, nén ZSTD, ping khi khởi động)
  api/router.go                    Đăng ký một số route HTTP (xem 2.5 về sự không đồng nhất)
  grpczstd/zstd.go                 Compressor zstd cho gRPC (xem 06)
  reports/                         MỖI BÁO CÁO MỘT PACKAGE
    so-chi-tiet-cac-tai-khoan/     Sổ chi tiết tài khoản
    so-ke-toan-chi-tiet-quy-tien-mat/   Sổ quỹ tiền mặt
    so-chi-tiet-vat-lieu-hang-hoa/ Sổ chi tiết vật liệu, hàng hoá (nguồn: eb.repository_ledger)
    tichluy/                       Báo cáo tích lũy
  datasets/tinh-gia-xuat-kho/      Dữ liệu thô cho ebinventory tính giá vốn (có bất biến riêng, xem COSTING.md)
  so-chi-tiet-cac-tai-khoan/pb/    Mã sinh từ proto (tài khoản)
  so_ke_toan_chi_tiet_quy_tien_mat/pb/   Mã sinh từ proto (quỹ)
  so_chi_tiet_vat_lieu_hang_hoa/pb/      Mã sinh từ proto (vật liệu)
docs/                              Tài liệu (thư mục này)
COSTING.md                         Bất biến nghiệp vụ của luồng tính giá xuất kho   (chỉ có ở nhánh dev)
CLAUDE.md                          Quy tắc làm việc với repo (quy trình sửa luồng tính giá)   (chỉ có ở nhánh dev)
gogateway.service, .github/workflows/deploy.yml   Triển khai (xem 2.7)
```

Thư mục `pb/` đặt tên **không thống nhất** (gạch ngang vs gạch dưới) và khác `go_package` trong proto ở một số sổ. Khi sinh mã, **xuất thẳng vào đúng thư mục `pb`
đang có** (lệnh ở [06](06-grpc-va-luong-stream.md)) và build lại để chắc import vẫn đúng. Đừng tự đổi tên thư mục nếu không sửa luôn `main.go`.

## 2.2 Một package báo cáo gồm những file nào

Tất cả sổ theo cùng một khuôn. Làm sổ mới = sao khuôn này.

| File | Vai trò |
|---|---|
| `types.go` | `Row` (một dòng kết quả, có tag JSON cho đường HTTP), hằng loại dòng (`OrderType*`) |
| `query.sql` | Template SQL **dòng chi tiết**, nhúng vào binary bằng `//go:embed` |
| `query_opening.sql` | Template SQL **số dư đầu kỳ** (xem [05](05-so-du-dau-ky.md)) |
| `staging_source.sql` | Subquery nguồn **thay thế** cho fact (xem [04](04-nguon-du-lieu-gl-gld.md)) |
| `service.go` | `QueryParams`, `Service`, dựng SQL từ template, `StreamSoChiTiet`, "walker", truy vấn phụ (phạm vi công ty, mở rộng tài khoản, tên tài khoản) |
| `handler.go` | HTTP: nhận JSON, trả JSON / NDJSON / chỉ đếm |
| `grpc-server.go` | gRPC: nhận proto request, gọi cùng `StreamSoChiTiet`, `Send` từng dòng (hoặc lô) |
| `proto/<ten>.proto` | Hợp đồng gRPC của sổ |

Sổ vật liệu hơi khác: không có `query_opening.sql` riêng vì đầu kỳ là một CTE trong `query.sql`, và có `opening_balance_checkpoint.sql` là nhánh thay thế
của CTE đó.

## 2.3 Luồng một request (sổ chi tiết tài khoản)

```
handler.go / grpc-server.go
   │ đổi request → QueryParams            (chỉ chuyển trường; KHÔNG chứa nghiệp vụ)
   ▼
service.StreamSoChiTiet(ctx, params, onRow)
   1. validateParams                      chặn đầu vào xấu trước khi vào SQL
   2. song song (sync.WaitGroup):
        resolveCompanyScope               IsDependent=1 → thêm đơn vị con từ bridge_org_scope
        expandAccounts                    IsParentNode=true → mở tài khoản cha xuống lá (bridge_account_hierarchy)
   3. cleanAccounts + kiểm accountPattern
   4. song song:
        resolveAccountInfo                dim_account → loại dư (account_group_kind) + tên
        loadOpeningBalances               query_opening.sql → map số dư đầu kỳ theo tài khoản
   5. buildDetailQuery                    chọn nhánh Chi tiết / Gộp (GroupSameItem), thay placeholder, chọn nguồn fact/staging
   6. conn.Query → rows.Next()
        scanRow → walker.push(row)        walker phát các dòng ra onRow ngay lập tức
   7. walker.finish                       dòng cuối + tổng cộng
```

`onRow` là điểm nối duy nhất giữa tầng nghiệp vụ và tầng truyền tải:
HTTP truyền `onRow` ghi JSON, gRPC truyền `onRow` gọi `stream.Send`. **Đừng** viết logic báo cáo ở handler hay grpc-server.

### Walker (máy trạng thái một lượt)

Dữ liệu đến đã sắp theo `account_number, posted_date, ...`. Walker duyệt tuần tự: gặp đổi tài khoản thì đóng tài khoản cũ, mở tài khoản mới. Loại dòng:

| `OrderType` | Ý nghĩa |
|---|---|
| 0 | Số dư đầu kỳ |
| 1 | Dòng chi tiết (kèm số dư lũy kế sau dòng đó) |
| 2 | Cộng phát sinh theo từng tài khoản |
| 3 | Số dư cuối kỳ |
| 4 | Tổng cộng toàn báo cáo |

Hai điều rút ra từ lỗi đã gặp:
- Số dư lũy kế (`running`) phải **bao gồm dòng hiện tại** và khởi tạo bằng số dư đầu kỳ. Lần đầu code dùng thẳng `opening` không đổi nên mọi dòng hiện cùng một số dư.
- Dòng Tổng cộng chỉ cộng **phát sinh**, không cộng số dư cuối kỳ (cộng dư Nợ của tài sản với dư Có của nợ phải trả không có ý nghĩa kế toán).

Vì walker dựa vào việc "các dòng cùng tài khoản nằm liền nhau" và vào **thứ tự dòng**, đổi `ORDER BY` là đổi nghiệp vụ (xem [03](03-chuyen-proc-sang-go-va-grain.md)).

## 2.4 Khuôn dựng SQL: template + `strings.ReplaceAll`

SQL là file `.sql` nhúng sẵn, chứa các placeholder dạng `{{TEN}}`. `service.go` thay từng placeholder bằng một đoạn SQL (hoặc giá trị đã kiểm).

Điều này **không dùng tham số ràng buộc**, nên:
1. **Mọi giá trị đi vào template đều phải qua bộ kiểm trước**: UUID qua `uuidPattern`, số hiệu tài khoản qua `accountPattern`, mã tiền tệ qua `currencyPattern`,
   `cluster_id` qua `clusterPattern`, ngày qua parse. Thêm tham số mới = thêm bộ kiểm mới.
2. **Đừng viết nguyên văn một placeholder trong comment SQL.** `ReplaceAll` không phân biệt comment với code, nên token trong comment cũng bị thay (từng
   gây lỗi khó thấy khi mô tả `{{OPENING_BALANCE_BODY}}` trong comment).
3. Placeholder không có trong file thì `ReplaceAll` bỏ qua, nên một hàm thay chung dùng cho cả `query.sql` lẫn `query_opening.sql` là an toàn.
4. Kiểu trả về giữa các nhánh (Chi tiết / Gộp, fact / staging) phải **giống hệt nhau** vì chỉ có một `scanRow` đọc **theo chỉ số cột**. Thêm/bớt/đảo cột
   trong một nhánh mà không sửa `scanRow` là gán nhầm giá trị sang trường khác, không báo lỗi. `concat()` với một đối số Nullable trả về `Nullable(String)`:
   luôn `coalesce` từng đối số.

## 2.5 Đăng ký route và dịch vụ

`cmd/server/main.go` dựng mọi `Service`/`Handler` rồi:
- **HTTP** (`HTTP_PORT`, mặc định `8089`): `api.NewRouter` đăng ký `/internal/v1/reports/tich-luy`, `/internal/v1/reports/so-chi-tiet-vat-lieu-hang-hoa`,
  `/internal/v1/datasets/repository-ledger`, `/healthz`. Hai sổ **mới hơn được đăng ký thẳng trong `main.go`**: `/api/so-ke-toan-chi-tiet-quy-tien-mat`, `/api/so-chi-tiet-tai-khoan`.
  Comment trong code ghi là sẽ dồn vào `NewRouter` khi ổn định. Sổ mới nên đăng ký ở `NewRouter` để chỗ này không phình thêm.
  Có thêm `/health` (ping ClickHouse thật, trả 503 khi đang tắt hoặc mất kết nối).
- **gRPC** (`GRPC_PORT`, mặc định `9090`): đăng ký ba service sổ + `grpc.health.v1` + **reflection** (cho phép dò toàn bộ API; cân nhắc tắt khi service lộ ra ngoài mạng nội bộ).
- Tắt êm: nhận SIGTERM → `isShuttingDown` → health báo NOT_SERVING → `GracefulStop` tối đa 30 s (phải nhỏ hơn `TimeoutStopSec` của systemd là 45 s) → `Stop`.

## 2.6 Cấu hình (`internal/config/config.go`)

| Biến môi trường | Mặc định | Tác dụng |
|---|---|---|
| `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE` | `10.100.112.47`, `9000`, `eb_admin`, (bắt buộc), `eb` | Kết nối ClickHouse. Thiếu `CH_PASSWORD` thì không khởi động |
| `HTTP_PORT`, `GRPC_PORT` | `8089`, `9090` | Cổng |
| `SO_CHI_TIET_TAI_KHOAN_DATA_SOURCE` | `fact` | `staging` → toàn bộ sổ tài khoản đọc staging |
| `SO_CHI_TIET_TAI_KHOAN_STAGING_COMPANY_IDS` | rỗng | Danh sách `CompanyID` (phẩy) chạy staging dù cờ trên là `fact`. **Chỉ có ở nhánh `prod`** (xem README) |
| `SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE` | `fact` | Như trên cho sổ quỹ (chưa có danh sách theo công ty) |
| `SO_CHI_TIET_VAT_LIEU_OPENING_BALANCE_SOURCE` | `raw` | `checkpoint` → đầu kỳ vật liệu đọc từ bảng checkpoint |
| `TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE` | `raw` | Nguồn tồn đầu kỳ của tính giá xuất kho, xem `COSTING.md` mục 5. **Chỉ có ở nhánh `dev`** (xem README) |

**Quy ước cờ:** giá trị mặc định = hành vi an toàn đang chạy; chỉ một giá trị đặc biệt mới bật nhánh mới; đổi cờ = khởi động lại, **không build lại**.
Thêm cờ mới theo đúng quy ước này và ghi vào bảng trên.

Kết nối ClickHouse (`chclient`): pool tối đa 20 kết nối mở, 10 rảnh; nén ZSTD; `max_block_size = 10000`; ping khi khởi động.

## 2.7 Triển khai

- GitHub Actions `.github/workflows/deploy.yml` chạy khi **push nhánh `dev`**: build, ghi file env từ secrets, cài `gogateway.service` (systemd, user `gogateway`, `Restart=on-failure`).
- Nghĩa là **push lên `dev` = triển khai**. Kiểm tra kỹ trước khi push; cờ mặc định phải giữ nguyên hành vi hiện tại.

## 2.8 Việc phải làm khi thêm một sổ mới (tóm tắt)

1. Tạo package `internal/reports/<ten-so>/` theo khuôn 2.2.
2. Viết `proto`, sinh mã vào `pb` (xem [06](06-grpc-va-luong-stream.md)).
3. Viết `query.sql` (+ `query_opening.sql`), `service.go`, `handler.go`, `grpc-server.go`.
4. Đăng ký route HTTP ở `NewRouter`, đăng ký gRPC service ở `main.go`.
5. Nếu có nguồn thay thế hoặc cờ: thêm vào `config.go`, theo quy ước 2.6.
6. Đối chiếu kết quả với proc gốc (checklist ở [09](09-checklist-so-moi.md)).

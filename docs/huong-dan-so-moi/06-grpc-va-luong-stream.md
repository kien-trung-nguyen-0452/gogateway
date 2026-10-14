# 6. gRPC và luồng stream

## 6.1 Vì sao streaming

Báo cáo lớn có thể vài trăm nghìn đến hàng triệu dòng. Bản Java cũ gom toàn bộ vào `List` rồi mới trả: treo và đầy RAM. Thiết kế hiện tại **không bao giờ giữ cả lô trong RAM**:

```
ClickHouse ──block 10 000 dòng──▶ rows.Next() ──▶ scanRow ──▶ walker.push ──▶ onRow(Row)
                                                                                 │
                                       gRPC: stream.Send(proto)  ◀───────────────┘   (hoặc HTTP: ghi một dòng JSON)
                                                │
Java: blocking stub, Iterator.hasNext()/next() ─┘  → toDto → ghi thẳng ra response (NDJSON)
```

**Backpressure tự nhiên:** Java dùng blocking stub; nếu consumer ghi chậm (mạng client yếu) thì `it.next()` không được gọi, cửa sổ HTTP/2 đầy, `stream.Send` ở Go chặn, Go ngừng đọc
`rows.Next()`, ClickHouse ngừng đẩy block. Không có chỗ nào phình bộ nhớ.

**Huỷ truyền ngược:** client ngắt thì `stream.Context()` bị huỷ; Go truyền `ctx` vào `conn.Query` (và sổ tài khoản còn kiểm `ctx.Done()` trong vòng lặp) nên dừng quét ngay.
Phía Java dùng `Context.CancellableContext` để huỷ RPC khi consumer ném lỗi (ví dụ người dùng đóng tab).

## 6.2 Hợp đồng proto

Ba service, mỗi sổ một proto, đều **server-streaming**:

| Service | RPC | Java package sinh ra |
|---|---|---|
| `sochitiettaikhoan.SoChiTietTaiKhoanService` | `GetSoChiTietTaiKhoan` | `vn.softdreams.ebweb.grpc.sochitiettaikhoan` |
| `soketoanchitietquytienmat....Service` | `GetSoKeToanChiTietQuyTienMat` | `vn.softdreams.ebweb.grpc.soketoanchitietquytienmat` |
| `sochitietvatlieu.SoChiTietVatLieuService` | `GetSoChiTietVatLieu` | `vn.softdream.reports.sochitietvatlieu` (lưu ý `softdream` không `s`) |

Quy ước:
- **Số tiền, số lượng, đơn giá là `string`**, không dùng `double`: nguồn là `Decimal(25,10)`, 25 chữ số vượt xa độ chính xác `double` (khoảng 15-17), mà với sổ kế toán lệch một đồng cũng là lỗi. Go dùng `decimal.String()`, Java `new BigDecimal(s)`.
  Đường HTTP/NDJSON thì ra số JSON vì frontend dùng pipe `ebcurrency`: hai đường phục vụ hai consumer nên khác nhau là đúng.
- Ngày là `string` (`yyyy-MM-dd`, hoặc RFC3339 tuỳ sổ); dòng tổng không có ngày thì chuỗi rỗng. Java chấp nhận cả hai dạng.
- Danh sách ID là `repeated string`. UUID **đã swap** (Java làm), Go chỉ nhận UUID chuẩn.
- **Không đổi tên `package` và tên service** của proto: tên đầy đủ (`sochitiettaikhoan.SoChiTietTaiKhoanService`) là tên trên đường truyền; đổi là client nhận `UNIMPLEMENTED`.
- **Chỉ thêm trường mới (số mới), không đổi/xoá/đổi số trường cũ.** Java và Go chạy hai chu kỳ phát hành khác nhau; thêm trường là tương thích hai chiều.
- proto3 không phân biệt "0" với "không gửi" cho `int32`: `type_id = 0` coi như null phía Java.

Hợp đồng nằm ở **hai chỗ phải đồng bộ**: `internal/reports/<so>/proto/*.proto` (Go) và repo `core-grpc-contract` (artifact `com.core:grpc-contract`, Java lấy stub từ đây, dùng chung với `dbtservice`/`ebqueue`).
Đổi proto = sửa cả hai, build lại jar contract, và triển khai theo thứ tự ở 6.6.

### Sinh mã Go

```bash
cd internal/reports/<ten-so>/proto
protoc -I . \
  --go_out=<đường dẫn thư mục pb đang có>      --go_opt=paths=source_relative \
  --go-grpc_out=<đường dẫn thư mục pb đang có> --go-grpc_opt=paths=source_relative \
  <ten>.proto
```

Phiên bản đang dùng (ghi ở đầu file sinh ra): `protoc 3.21.12`, `protoc-gen-go v1.36.11`, `protoc-gen-go-grpc v1.6.2`. **Dùng đúng bản này** để diff sinh lại chỉ chứa phần bạn đổi.
Kiểm: sinh lại từ proto **chưa sửa** phải ra file giống hệt hiện có; nếu không, môi trường sinh của bạn khác.

## 6.3 Server (phía Go)

- Mỗi sổ có `GRPCServer` nhúng `Unimplemented...Server`, đổi request → `QueryParams`, gọi `service.StreamSoChiTiet`, trong callback `stream.Send(rowToProto(row))`.
- **Quên gán một trường trong `rowToProto` là lỗi âm thầm:** query và `scanRow` vẫn có dữ liệu nhưng proto message không mang theo, Java nhận chuỗi rỗng, không có lỗi build hay runtime.
  Hai lần đã gặp: `UnitType` (thiếu → báo cáo luôn chạy nhánh "không quy đổi đơn vị" dù người dùng chọn đơn vị chuyển đổi) và `MainUnitName` (thiếu → cột đơn vị trống). Mỗi khi thêm trường phải rà cả ba nơi: **proto, `rowToProto`, `toDto`/`toGoRow` ở Java**.
- Đăng ký trong `main.go`: ba service sổ, `grpc.health.v1` (trạng thái đổi sang `NOT_SERVING` lúc tắt), và reflection (cân nhắc tắt khi service lộ ra ngoài mạng nội bộ).
- Mặc định gRPC giới hạn message **4 MB**. Gửi từng dòng thì không bao giờ chạm; gửi theo lô (6.5) thì phải tính (lô 600 dòng khoảng 0,1-0,3 MB).
- Lỗi trả ra theo code hiện tại là `fmt.Errorf(...)` thường, tức gRPC code `UNKNOWN`: bên Java có nhánh dịch `INVALID_ARGUMENT` nhưng **theo code hiện tại Go không tạo ra mã đó**. Nếu cần Java phân biệt lỗi tham số với lỗi hệ thống, trả `status.Error(codes.InvalidArgument, ...)` ở Go cho lỗi từ `validateParams`.

## 6.4 Đường HTTP/NDJSON (song song với gRPC)

`handler.go` của mỗi sổ nhận JSON và có ba chế độ theo query string:

| Query | Hành vi |
|---|---|
| (mặc định) | gom toàn bộ vào `Response{rowCount, elapsedMs, data}`, **chỉ dùng cho báo cáo nhỏ** |
| `?stream=true` | NDJSON (`application/x-ndjson`), mỗi dòng một object, `Flush` mỗi 500 dòng; **dòng cuối là `{"rowCount":..,"elapsedMs":..}`** hoặc `{"error":...}` |
| `?metaOnly=true` | chỉ đếm, không giữ dòng |

NDJSON không đặt được HTTP status sau khi đã ghi dòng đầu, nên **client phải kiểm dòng cuối**. Mọi hợp đồng "báo lỗi giữa chừng bằng một dòng trong luồng" đều có cùng đánh đổi này; phía Java ghi `{"type":"error"}`.

## 6.5 Gửi theo lô + nén zstd (đã đo, đã làm cho sổ vật liệu)

**Vấn đề đo được:** 1,48 triệu dòng sổ tài khoản = **495 MB** trên dây (334 B/dòng, mỗi dòng một message). Nén gzip từng dòng chỉ được **1,13x** và tốn CPU gấp 30 lần.
gRPC nén **từng message**, nên nén chỉ có tác dụng khi **gộp nhiều dòng vào một message**.

Đo trên 1 triệu dòng thật, loopback có giới hạn băng thông, một stream:

| Phương án | 100 Mbps | Byte trên dây | CPU (srv + cli) |
|---|---|---|---|
| 1 dòng/message, không nén (hiện tại) | 33,6 s | 343 MB | 1,7-2,0 s |
| 1 dòng/message + gzip | 29,2 s | 302 MB | **31-61 s** |
| Lô 500, không nén | 32,2 s | 332 MB | 0,4-1,2 s |
| Lô 500 + gzip Default | 3,7 s | 38,5 MB | 3,4-3,6 s |
| Lô 500 + gzip BestSpeed | 4,4 s | 45 MB | 2,1-2,4 s |
| **Lô 500 + zstd (Fastest)** | **3,65 s** | **36,4 MB** | **1,0-1,3 s** |
| Lô 500 + lz4 | 6,4 s | 65 MB | 0,75-1,0 s |

- **zstd thắng:** nén chặt hơn gzip, nhanh hơn gzip Default khoảng 3,8 lần. lz4 nhanh hơn chút nhưng wire lớn gấp 1,8 lần zstd, chỉ hợp khi mạng rất rộng.
- **Cỡ lô:** từ 500 dòng trở lên gần như phẳng (wire 36,3-36,6 MB; thời gian ±0,05 s); dưới 250 dòng bắt đầu thiệt (43 MB ở 50 dòng); lô 10 000 dòng đã sát giới hạn 4 MB. Dùng **600 dòng**, không giới hạn byte (dòng lớn nhất đã gặp 460 B).
- **Thời gian tổng trên loopback gần như không đổi** vì phần lớn là ClickHouse chạy query; lợi ích nằm ở byte và CPU, chỉ thấy rõ khi qua mạng thật hoặc VPN.

**Hiện trạng triển khai:**
- Sổ **chi tiết vật liệu**: đã thêm RPC `GetSoChiTietVatLieuBatch` (lô 600 dòng + zstd) song song với RPC cũ trong go-gateway. Đo: 185,5 MB → **24,5 MB** (337.017 dòng), nội dung và thứ tự dòng giống hệt RPC cũ (đã so qua client Java thật).
- Phía Java và contract: file mẫu và hướng dẫn ở `docs/java-vat-lieu-batch-zstd/`.
- Sổ tài khoản và sổ quỹ: **chưa** có RPC batch trong repo (đã thử cho sổ tài khoản, cùng kết quả tương tự, rồi gỡ khỏi repo theo yêu cầu).

**Cách làm một RPC batch cho sổ khác (theo mẫu `grpc-server.go` của sổ vật liệu):**
1. Proto: thêm `rpc GetXxxBatch(Request) returns (stream XxxBatch)` và `message XxxBatch { repeated XxxRow rows = 1; }`. **Giữ nguyên RPC cũ.**
2. Server: gọi `grpc.SetSendCompressor(stream.Context(), grpczstd.Name)` (lỗi nghĩa là client không khai báo zstd: vẫn gửi bình thường, không nén); gom `batchSize` dòng rồi `stream.Send`; **flush phần dư ở cuối**
   (quên bước này là mất các dòng cuối, kể cả dòng tổng cộng). Tách hàm `paramsFromRequest` dùng chung với RPC cũ.
3. Client Java: khai báo `Decompressor` zstd với `advertised = true`, lặp `getRowsList()` và vẫn đẩy **từng dòng một** cho consumer, đúng thứ tự.
4. Kiểm: số dòng, thứ tự, dòng cuối, và `grpc-encoding = zstd` ở header phản hồi.

## 6.6 Thứ tự triển khai khi đổi hợp đồng

1. Cập nhật `core-grpc-contract`, build và phát hành jar.
2. Triển khai go-gateway bản có RPC mới (RPC cũ vẫn chạy nên không ai hỏng).
3. Triển khai BE Java bản dùng RPC mới, **cờ mặc định tắt** (ví dụ `go-gateway.grpc.batch=false`).
4. Bật cờ theo môi trường/công ty; đo lại byte nhận và thời gian.

Làm ngược thứ tự (Java bật trước khi Go có RPC) là lỗi `UNIMPLEMENTED`.

## 6.7 Vận hành

- Cổng: HTTP `8089`, gRPC `9090`. `/health` (HTTP) và `grpc.health.v1` đều báo lỗi khi mất ClickHouse hoặc đang tắt.
- Kết nối tới Go từ Java hiện là **plaintext** (mạng nội bộ). Không đưa ra ngoài mạng nội bộ khi chưa có TLS.
- Deadline từ phía Java hiện là 180 s cho mỗi RPC: báo cáo lớn hơn sẽ nhận `DEADLINE_EXCEEDED` và Java đổi thành thông báo "Báo cáo quá lớn hoặc hệ thống đang tải cao".
- Tắt êm tối đa 30 s cho các stream đang chạy (nhỏ hơn `TimeoutStopSec` 45 s của systemd).

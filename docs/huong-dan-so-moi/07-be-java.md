# 7. BE Java (ebwebbe): cấu trúc code và cách code một sổ mới

Nguồn: nhánh `origin/dev-99-mix-200` của repo `200133ebwebbe` (đường dẫn tính từ `src/main/java/vn/softdreams/ebweb/`, viết tắt `B/`).
**Nhánh `prod` hiện chỉ có phần sổ chi tiết tài khoản**; sổ quỹ và sổ vật liệu mới chỉ có ở các nhánh dev. Kiểm tra nhánh bạn đang làm có file nào trước khi sao khuôn.

## 7.1 Cây file (một dòng mỗi file)

**Sổ chi tiết tài khoản** (package `B/service/datawarehouse/accoutledger/`; `accoutledger` là **chính tả thật** trong code, đừng "sửa")

| File | Vai trò |
|---|---|
| `AccountLedgerGrpcRepository.java` | Gọi gRPC (blocking stub), `streamDirect`, dịch lỗi `translate`, ánh xạ `toDto` |
| `SoChiTietTaiKhoanService` / `...Impl` | `stream`, `count`, `resolve` (đổi `RequestReport` thành tham số) |
| `LedgerGrpcProperties` / `LedgerGrpcConfig` | Cấu hình `ledger.grpc.*` và bean kênh `ledgerChannel` |
| `LedgerNdjsonJacksonConfig` | `ObjectMapper` cho NDJSON (BigDecimal ra chuỗi `toPlainString`, không indent) |
| `DwhReportMetadataService` | Dựng header/footer/kỳ/chữ ký cho `ResDynamicReportDTO` |
| `B/web/rest/SoChiTietTaiKhoanResource` | REST `POST /api/dwh/accountLedgers` (và `/count`) |

**Sổ quỹ tiền mặt** (`B/service/datawarehouse/cashledger/`): `SoKeToanChiTietQuyTienMatController` (REST `/api/dwh/cashLedgers`, `/count`), `SoKeToanChiTietQuyTienMatService` (class cụ thể, không interface), `CashLedgerGrpcRepository`. Cấu trúc y hệt sổ tài khoản.

**Sổ chi tiết vật liệu** (`B/service/reportclickhouse/` và rải ở `B/service`, `B/web/rest`)

| File | Vai trò |
|---|---|
| `reportclickhouse/gateway/GoGatewayClient` | `@GrpcClient("go-gateway")`; `soChiTietVatLieu`, `buildProtoRequest`, `streamRows`, `toGoRow` |
| `B/web/rest/SoChiTietVatLieuChResource` | REST `/api/reports/kho/...` (`/so-chi-tiet-vat-lieu`, `/stream`, `/xem-toi-uu/duoc-phep`) |
| `B/service/SoChiTietVatLieuChService` + `impl/...Impl` | Luồng stream NDJSON gzip, cờ "xem tối ưu" |
| `B/service/SoChiTietVatLieuGoRequestService` + `impl/...Impl`, `B/web/rest/dto/SoChiTietVatLieuGoRequest(Mapper)` | Dựng request (có truy vấn DB) |
| `B/service/dto/SoChiTietVatLieuStreamContext` | Gói protoRequest, user, requestReport sang thread stream |
| `reportclickhouse/kho/excel/*` | Xuất Excel đồng bộ và async (`ChunkFileWriter`, `ReportExportExecutorConfig`, `SoChiTietVatLieuGoRowMapper`) |
| `reportclickhouse/kho/pdf/*`, `B/web/rest/reportclickhouse/kho/{excel,pdf}/*` | Xuất PDF (Jasper) và REST |
| `reportclickhouse/config/GoGatewayRestTemplateConfig` | Bean `RestTemplate`; **không thấy nơi nào dùng** |

**Dùng chung:** `B/service/datawarehouse/support/SqlServerUuidSwap`.

Proto/contract: stub lấy từ artifact `com.core:grpc-contract:1.0` (`pom.xml`), không sinh trong repo này (repo có thêm hai file trong `src/main/proto/` nhưng không thấy plugin protobuf trong `pom.xml`, chưa rõ có được compile).
Các DTO (`SoChiTietTaiKhoanDTO`, `SoChiTietVatLieuGoRow`, `RequestReport`, `ResDynamicReportDTO`...) nằm trong **jar nội bộ `core-*`**, không có source trong repo.

## 7.2 Luồng end-to-end

### Sổ tài khoản / sổ quỹ (mẫu NDJSON, chuẩn để sao chép)

```
POST /api/dwh/accountLedgers   (produces application/x-ndjson)
  Thread REQUEST GỐC (còn ThreadLocal + SecurityContext):
    buildMeta → DwhReportMetadataService.enrich...
    typeRepository.findAllByIsActive(), userService.getAccount().getDomain()     ← mọi truy vấn DB làm ở đây
    return StreamingResponseBody
  Thread STREAMING (KHÔNG còn ThreadLocal):
    writeStream:  ghi dòng meta
      Service.stream  (@Transactional NOT_SUPPORTED)
        resolve(RequestReport) → tham số
        AccountLedgerGrpcRepository.streamDirect(...)   // vòng it.hasNext()
          callback: đặt linkRef, ghi từng dòng, flush mỗi 500 dòng
      ghi {"type":"end"}   (lỗi giữa chừng: ghi {"type":"error"})
  Header: X-Accel-Buffering: no, Cache-Control: no-cache
```

`POST .../count` chạy cùng luồng nhưng consumer rỗng (chỉ đếm).

### Sổ vật liệu

- `POST /so-chi-tiet-vat-lieu`: gom `List` rồi nén zip (không phải NDJSON); chỉ dùng cho báo cáo nhỏ.
- `POST /so-chi-tiet-vat-lieu/stream`: thread gốc gọi `service.prepareStream` (dựng proto request + user + types), thread stream `writeNdjson` bọc `GZIPOutputStream(syncFlush=true)`, `gateway.streamRows` đẩy vào `NdjsonGroupWriter`
  **gom theo nhóm (kho, vật tư)** vì tổng gán lên từng dòng; flush mỗi 2000 dòng; `Content-Encoding: gzip`.
- Excel/PDF async: `buildProtoRequest` + tạo `TaskLog` + lưu `yearContext` ở thread request, rồi `reportExportExecutor` (pool riêng) chạy `streamRows` → `ChunkFileWriter` (lô 5000 dòng cho Excel, 1000 cho PDF) → phát RabbitMQ.

## 7.3 Quy ước bắt buộc khi code BE (đã rút ra từ code và comment)

1. **Mọi việc chạm DB phải xong ở thread request gốc**, trước khi trả `StreamingResponseBody` hoặc đẩy vào pool. `YearContextHolder` (routing datasource theo năm, `ThreadLocal`) và `SecurityContext` **không sang thread khác**.
   Thread nền phải `YearContextHolder.setYearContext(...)` lại và `clearBranchContext()` trong `finally`. Với `RabbitMQMessage` dùng constructor có tham số (constructor rỗng đọc `SecurityContext`).
2. **Chỉ có API stream, không có biến thể trả `List`.** Nếu có thì người gọi sẽ dùng cho tiện và lợi ích streaming mất. Ai cần `List` thì tự gom trong callback và tự chịu trách nhiệm về RAM.
3. **Không `@Transactional` quanh lời gọi gRPC dài** (dùng `NOT_SUPPORTED`): tránh giữ connection pool DB trong lúc stream.
4. **Giữ thứ tự dòng, cấm sort/gom nhóm/song song ở Java.** Số dư lũy kế do Go cộng dồn theo thứ tự; sổ tài khoản trả theo thứ tự tài khoản người dùng chọn (dùng `LinkedHashSet`); sổ vật liệu đã `ORDER BY` để Java chỉ giữ một nhóm trong RAM, **đổi `ORDER BY` ở Go là vỡ logic cắt lô** của `ChunkFileWriter`/`NdjsonGroupWriter`.
5. **Lỗi giữa luồng báo bằng một dòng trong luồng** (`{"type":"error"}`), vì header đã gửi. `UncheckedIOException` khi ghi = client đã ngắt: đừng bọc lại thành lỗi hệ thống.
6. **Huỷ RPC khi client ngắt:** dùng `Context.CancellableContext`, `ctx.cancel(e)` trong `catch`, `detach`/`close` trong `finally`, để Go không quét nốt hàng trăm nghìn dòng vô ích.
7. **Một `ManagedChannel` dùng chung cả app**, không tạo mỗi request (mỗi lần tạo phải bắt tay TCP lại).
8. **Số tiền là `BigDecimal` từ chuỗi** (`new BigDecimal(s)`, rỗng/sai → `ZERO` có log cảnh báo); khi ra JSON cũng ra **chuỗi** bằng `toPlainString`. Không dùng `float`/`double`.
9. **NDJSON phải một JSON một dòng:** tắt `SerializationFeature.INDENT_OUTPUT` (profile dev bật indent sẽ phá giao ước). gzip dùng `syncFlush=true` thì mới stream thật. Header `X-Accel-Buffering: no` để nginx không gom buffer.
10. **UUID phải swap trước khi gửi xuống Go** (`SqlServerUuidSwap.swap`), và việc này ở lại Java. Sổ vật liệu hiện dùng `Common.revertUUID(...)` (chưa kiểm package/ngữ nghĩa) thay vì `SqlServerUuidSwap`: **có vẻ là hai hàm cho cùng một việc, cần xác nhận**.
    Sổ mới dùng `SqlServerUuidSwap`, và đừng swap hai lần.
11. **Quy ước ngược trực giác:** `currentBook = 0` hoặc không gửi mới là **sổ tài chính** (`isFinancialBook = 1`). Giữ đúng như bản cũ, frontend đang phụ thuộc.
12. **Dòng số dư đầu kỳ của vật liệu có giá trị giả** (`PostedDate = 1970-01-01`, `ReferenceID` UUID rỗng) vì cột không nullable bên ClickHouse; phải map về `null` sang DTO để Excel không in `01/01/1970`.
13. **Pool xuất báo cáo có giới hạn** (core 2, max 4, queue 20, `AbortPolicy`): từ chối rõ ràng thay vì xếp hàng vô hạn. Gắn tag log để lọc (`[SCTVL-ASYNC]`, `[XEM-TOI-UU]`).

## 7.4 Cấu hình (`application-dev.yml`, `application-prod.yml`)

```yaml
ledger:                      # kênh dùng chung cho sổ tài khoản và sổ quỹ
  grpc:
    enabled: true            # xem cảnh báo bên dưới
    target: "dns:///<host>:9090"
    deadline-seconds: 180
    max-inbound-message-mb: 32
    plaintext: true
    keep-alive-seconds: 30

grpc:                        # kênh KHÁC, dùng cho sổ vật liệu (@GrpcClient("go-gateway"))
  client:
    go-gateway:
      address: ${GOGATEWAY_GRPC_ADDRESS:static://<host>:9090}
      negotiation-type: plaintext
      max-inbound-message-size: 64MB
      keep-alive-time: 30s
```

- **Hai cơ chế kết nối song song cho cùng một service Go** (`ledgerChannel` tự tạo; `@GrpcClient("go-gateway")` của grpc-client-spring-boot-starter 2.14.0). Sổ mới nên **chọn một**; hướng đề xuất ở [08](08-tach-service-rieng.md).
- **`ledger.grpc.enabled=false` làm Spring context không khởi động được**, vì `AccountLedgerGrpcRepository`/`CashLedgerGrpcRepository` nhận `@Qualifier("ledgerChannel")` không điều kiện. Nó **không phải** cờ lùi về proc cũ.
- Mặc định trong `LedgerGrpcProperties`: `enabled=false`, `target=localhost:50051`, `deadlineSeconds=180`, `maxInboundMessageMb=32`, `plaintext=true`, `keepAliveSeconds=30`, `shadowCompare=false`.
- Các cấu hình **không còn code Java nào đọc**: `ledger.grpc.shadow-compare`, `clickhouse.mart.*`, `clickhouse.dwh-detail.enabled`, `gogateway.base-url`. Đừng dựa vào chúng.

## 7.5 Chọn luồng cũ (proc) hay luồng mới (go-gateway)

Trong Java **không có công tắc** chọn proc hay go-gateway cho từng sổ, và **không có fallback tự động**. Luồng mới là **endpoint khác** (`/api/dwh/...`, `/api/reports/kho/so-chi-tiet-vat-lieu/stream`); các endpoint cũ vẫn gọi proc (qua `ReportBusinessRepository*`). Frontend chọn endpoint nào.

Cờ duy nhất theo công ty: `GET /api/reports/kho/xem-toi-uu/duoc-phep` (`SoChiTietVatLieuChServiceImpl.duocXemToiUu`), đọc JSON ở Redis key `xem_toi_uu_cau_hinh`, trường `cheDo`:

| `cheDo` | Nghĩa |
|---|---|
| 1 | tắt hết |
| 2 | bật hết |
| 3 | theo danh sách `congTy` |
| 4 | chỉ danh sách `debug` |

Giá trị lạ, JSON hỏng hoặc không có key → `false`. So khớp `CompanyID` của người dùng với danh sách, cả dạng gốc lẫn dạng `revertUUID`. Cờ này chỉ trả bool để frontend quyết định có hiện nút "xem tối ưu";
**BE không tự chặn `/stream`**.

## 7.6 Ánh xạ proto → DTO (quy ước đang dùng)

- `bd(s)`: rỗng hoặc sai → `BigDecimal.ZERO` (log cảnh báo).
- `parseDate`: nhận cả `yyyy-MM-dd` lẫn RFC3339; sai → `null` (log). **`GoGatewayClient.parseLocalDateOrNull` chưa bọc `try/catch`** như repository sổ tài khoản.
- `nullIfEmpty`: chuỗi rỗng → `null`. `typeId == 0` → `null`.
- `customField1..5` và `customFieldDetail1..5` lấy từ danh sách proto (luôn 5 phần tử).
- Mỗi trường mới thêm vào proto phải thêm ở **ba nơi**: proto, `rowToProto` (Go), `toDto`/`toGoRow` (Java).
- Dịch lỗi gRPC (`translate`: `DEADLINE_EXCEEDED`, `UNAVAILABLE`, `INVALID_ARGUMENT`, `CANCELLED`, mặc định) được **sao chép y hệt** ở từng repository, và `GoGatewayClient.streamRows` bọc mọi lỗi thành `RuntimeException`. Đây là chỗ nên gom chung (xem [08](08-tach-service-rieng.md)).

## 7.7 Điểm cần xác minh (chưa kiểm chứng)

- `SoChiTietTaiKhoanServiceImpl.resolve` gọi `req.getAccountList().toString()` rồi tách theo `[,;]`. Nếu `accountList` là **tập hợp** thay vì chuỗi, `toString()` sinh `"[a, b]"` và dấu ngoặc vuông dính vào mã tài khoản đầu và cuối.
  Javadoc của hàm nói đó là chuỗi phân cách bằng phẩy/chấm phẩy; kiểu thật nằm trong jar `core-model`. **Hãy kiểm kiểu của `RequestReport.accountList`** (và thử với danh sách có hơn một tài khoản).

## 7.8 Cách code một sổ mới ở Java (khuôn)

1. **Contract:** thêm proto vào `core-grpc-contract`, build jar, nâng phiên bản dependency ở `pom.xml` (xem [06](06-grpc-va-luong-stream.md)).
2. **Repository gRPC** (`XxxGrpcRepository`): chỉ một phương thức `streamDirect(..., RowConsumer)`; `Context.CancellableContext`; `translate` lỗi; `toDto`. Nhận kênh dùng chung, không tự tạo kênh.
3. **Service** (`@Transactional(NOT_SUPPORTED)`): `resolve(RequestReport)` đổi sang tham số thuần (không logic nghiệp vụ nặng), `stream`, `count`.
4. **Resource/Controller:** ở thread gốc dựng meta/types/domain; trả `StreamingResponseBody`; ghi NDJSON theo mẫu 7.2; header `X-Accel-Buffering`, `Cache-Control`.
5. **Không** tạo biến thể trả `List`. **Không** sort/gom nhóm ở Java.
6. **Cờ bật/tắt** theo quy ước hiện có (ví dụ Redis theo công ty hoặc cờ môi trường); mặc định giữ luồng cũ.
7. **Kiểm:** số dòng, dòng đầu/cuối, tổng theo tài khoản so với proc; thử client ngắt giữa chừng (Go phải dừng quét); thử báo cáo rất lớn (RAM Java không phình).

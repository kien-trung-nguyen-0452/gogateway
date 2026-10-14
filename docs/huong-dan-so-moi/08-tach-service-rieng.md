# 8. Quy ước code để sau này tách phần báo cáo thành service riêng

Mục tiêu: **code sổ mới hôm nay đặt sao cho chuyển nó sang một service độc lập chỉ là việc dời package và đổi cách nối, không phải viết lại.**
Hiện phần báo cáo Java gắn chặt vào monolith `ebwebbe` ở các điểm dưới đây (đã kiểm trên nhánh `dev-99-mix-200`).

## 8.1 Những gì đang gắn vào monolith

| Chỗ gắn | Ở đâu | Vì sao vướng khi tách |
|---|---|---|
| **Routing datasource theo năm** (`YearContextHolder`, `ThreadLocal`) | `core.eb.config.multirouting`, `DataSourceInterceptor`, dùng ở các service Excel/PDF async | Service riêng không có cơ chế này; thread nền phải tự set/clear |
| **SecurityContext / người dùng** (`SecurityUtils.getCurrentUserLoginAndOrg()`, `userService.getAccount()`) | Resource sổ tài khoản/quỹ, `SoChiTietVatLieuChServiceImpl`, `SoChiTietVatLieuGoRequestServiceImpl`, các service Excel/PDF | Cần xác thực/uỷ quyền riêng hoặc truyền danh tính xuống |
| **Metadata báo cáo từ JPA** | `DwhReportMetadataService` (`UserService`, `DRDisplayReportConfigDetailsRepository`, `OrganizationUnitOptionReportRepository`, `OrganizationUnitRepository`) | Phụ thuộc cả một lớp bảng cấu hình của monolith |
| **Dựng request có truy vấn DB** | `SoChiTietVatLieuGoRequestServiceImpl` (`MaterialGoodsRepository`, `RepositoryRepository`, `SystemOptionRepository`, `OrganizationUnitRepository`) | Logic nghiệp vụ lẫn với truy cập dữ liệu của monolith |
| **Bảng `Type` + domain người dùng** để dựng link chứng từ | `SoChiTietTaiKhoanResource` (`TypeRepository`, `Utils.getRefLinkForReport`) | Link chứng từ thuộc về ứng dụng chính |
| **Xuất Excel/PDF async** | `TaskLogRepository` (JPA), `RabbitMQSenderService`, `RabbitMQMessage` (đọc SecurityContext trong constructor rỗng), `Redis`, `ChunkFileWriter` ghi file xuống đĩa | Cần storage dùng chung với consumer; consumer nằm ngoài các package này |
| **DTO nằm trong jar nội bộ** | `core-model`, `core-config`, `core-report` (`SoChiTietTaiKhoanDTO`, `SoChiTietVatLieuGoRow`, `RequestReport`, `ResDynamicReportDTO`) | Service mới phải kéo các jar này hoặc sao chép |
| **Hai cơ chế kênh gRPC** | `ledgerChannel` (tự tạo) và `@GrpcClient("go-gateway")` (starter) | Hai nơi cấu hình cho cùng một service |
| **Lệch tên / sao chép** | `translate` sao chép ở từng repository; `SqlServerUuidSwap` và `Common.revertUUID`; `RowConsumer` định nghĩa lại | Phải gom về một chỗ trước khi tách |

## 8.2 Kiến trúc đích (đề xuất)

Chia phần báo cáo thành các lớp với **hướng phụ thuộc một chiều**: ngoài → trong.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ ① Web (REST/NDJSON)         Resource: auth, meta, ghi response               │  ← ở lại monolith / cổng API
├──────────────────────────────────────────────────────────────────────────────┤
│ ② Ứng dụng                  dựng ReportRequest (thuần), gọi Port, ghi luồng   │  ← có thể dời cùng service
├──────────────────────────────────────────────────────────────────────────────┤
│ ③ Port (interface thuần)    ReportStreamPort<Req, Row>: stream(req, consumer) │  ← ranh giới tách
├──────────────────────────────────────────────────────────────────────────────┤
│ ④ Adapter gRPC              GoGatewayXxxAdapter: kênh, deadline, huỷ, dịch lỗi │  ← không phụ thuộc Spring monolith
└──────────────────────────────────────────────────────────────────────────────┘
```

**Luật phụ thuộc** (kiểm bằng ArchUnit hoặc review):
- Lớp ③ và ④ **không** import: `YearContextHolder`, `SecurityUtils`, repository JPA, `UserService`, `RabbitMQ*`, `core.*` DTO của monolith.
- Lớp ② không đọc `ThreadLocal`/`SecurityContext`; mọi thứ nó cần nhận qua tham số.
- **Mọi thứ phụ thuộc ngữ cảnh người dùng/năm được giải quyết một lần ở rìa (lớp ①) và đóng gói thành giá trị bất biến** truyền xuống, ví dụ:

```java
// Đề xuất, minh hoạ ý tưởng.
record ReportContext(UUID companyId, List<UUID> companyIds, String yearKey, Locale locale /* ... */) {}
record AccountLedgerRequest(ReportContext ctx, LocalDate from, LocalDate to, List<String> accounts,
                            boolean financialBook, boolean dependent, boolean groupSameItem, boolean parentNode, String currency) {}
interface ReportStreamPort<Req, Row> { long stream(Req req, RowConsumer<Row> consumer); }
```

## 8.3 Quy tắc code ngay từ bây giờ (để sau này tách rẻ)

1. **Mỗi sổ một `Port` + một `Adapter`**, không để Resource/Service gọi stub gRPC trực tiếp.
2. **Request/Row là DTO riêng của lớp báo cáo**, không dùng `RequestReport`/DTO `core-*` ở lớp ③④. Chuyển đổi nằm ở lớp ② (một mapper mỗi sổ).
3. **Gom chung** (một lần, cho mọi sổ): một `GoGatewayChannelProvider` (một kênh, cấu hình ở một nơi), một `GrpcErrorTranslator`, một `RowConsumer<T>`, một `UuidSwap`.
   Đây là việc dọn dẹp nên làm đầu tiên; mỗi sổ mới sao thêm một bản `translate` là nợ kỹ thuật mới.
4. **Không để ThreadLocal đi qua ranh giới thread.** Truyền giá trị tường minh (`ReportContext`). Việc đọc `YearContextHolder`/`SecurityContext` chỉ ở lớp ①.
5. **Dựng request có truy vấn DB (kiểu `SoChiTietVatLieuGoRequestServiceImpl`) phải tách khỏi việc gọi go-gateway:** nó xuất ra một `Request` thuần. Sau này phần này hoặc ở lại monolith (và gọi qua API), hoặc chuyển thành một lệnh gọi tra cứu.
6. **Xuất Excel/PDF async không nằm trong lớp ③④.** Nó là một "consumer" của luồng dòng; tách vì nó cần storage dùng chung và hàng đợi.
7. **Mỗi sổ có cờ bật/tắt** theo công ty ngay từ đầu (xem cờ `xem_toi_uu_cau_hinh`), để tách service xong còn chuyển lưu lượng dần.
8. **Contract đi qua jar/proto, không qua class nội bộ.** Thêm trường theo quy ước tương thích ([06](06-grpc-va-luong-stream.md)).

## 8.4 Các lộ trình tách (chọn dần, không loại trừ nhau)

| Mức | Làm gì | Lợi | Giá |
|---|---|---|---|
| **A. Dọn trong monolith** | 8.3 điểm 1-5; ArchUnit chặn import ngược | Ranh giới rõ, vẫn một ứng dụng | Thấp |
| **B. Module Maven riêng** | `report-contract` (proto + DTO), `report-client` (③④, không Spring Web), phần còn lại ở monolith | Dùng lại được ở service khác; build độc lập | Trung bình |
| **C. Service Java riêng** | Dời ①②③④ vào một service, giữ API REST/NDJSON y hệt; monolith (hoặc cổng API) chuyển tiếp | Scale riêng, không dùng chung pool/datasource của monolith | Cao: xác thực, metadata, storage cho Excel/PDF, DTO `core-*` |
| **D. Để chính go-gateway làm API** | Go đã có HTTP NDJSON (`/api/so-chi-tiet-tai-khoan`, `/api/so-ke-toan-chi-tiet-quy-tien-mat`); Java chỉ còn xác thực + metadata | Bớt một chặng truyền | Phải thêm xác thực/uỷ quyền ở Go (trong các file đã đọc **không thấy** middleware xác thực, hiện chỉ tin mạng nội bộ) |

Khuyến nghị: **A ngay**, **B** khi có sổ thứ ba, quyết định giữa **C** và **D** theo nơi muốn đặt xác thực/metadata.

## 8.5 Kiểm tra "đã sẵn sàng tách" cho một sổ

- [ ] Lớp ③④ không import gì từ danh sách cấm ở 8.2.
- [ ] Chạy được `Adapter` trong một unit test chỉ với địa chỉ go-gateway, không cần Spring context đầy đủ.
- [ ] Không có `ThreadLocal`/`SecurityContext` được đọc bên trong luồng stream.
- [ ] Kênh gRPC, deadline, dịch lỗi lấy từ phần dùng chung.
- [ ] Request và Row là DTO riêng, có mapper hai chiều ở lớp ②.
- [ ] Có cờ bật/tắt theo công ty và đường lùi về luồng cũ.
- [ ] Có kiểm thử đối chiếu với proc (số dòng, tổng, đầu/cuối kỳ).

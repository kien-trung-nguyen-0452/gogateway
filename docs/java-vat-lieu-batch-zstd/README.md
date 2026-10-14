# Sổ chi tiết vật liệu: nhận dữ liệu theo lô + nén zstd (phía Java)

Mục đích: giảm băng thông go-gateway → ebwebbe. Đo trên dữ liệu thật (server test `.64`,
công ty `62f7bf76-…`, 2025, 337.017 dòng):

| | RPC cũ `GetSoChiTietVatLieu` | RPC mới `GetSoChiTietVatLieuBatch` |
|---|---|---|
| Byte trên dây | 185,5 MB | **24,5 MB** (giảm 7,6 lần) |
| Số message | 337.017 | 562 (600 dòng/message) |
| Nội dung / thứ tự dòng | | **giống hệt từng dòng** (so hash có xét thứ tự, qua client Java thật) |

RPC cũ **giữ nguyên**, client chưa nâng cấp vẫn chạy. Thời gian tổng trên loopback gần như
không đổi vì phần lớn là ClickHouse chạy query; lợi ích nằm ở byte qua mạng.

## Thứ tự triển khai (quan trọng)

1. **core-grpc-contract**: áp `core-grpc-contract.patch`, build và cài lại jar (`install.bat`).
   Patch (nền: nhánh `prod` của contract) chỉ thêm RPC `GetSoChiTietVatLieuBatch` + message
   `SoChiTietVatLieuBatch`, không đổi RPC/field cũ. Contract dùng chung với
   dbtservice/ebqueue nên các bên đó phải build lại được với jar mới.
2. **go-gateway**: triển khai bản có RPC batch (đã có trong repo này).
3. **ebwebbe** (nền: `origin/dev-99-mix-200`; `GoGatewayClient.java` giống hệt trên dev-99 và
   các nhánh task/namnhp, **không có trên prod**):
   - `git apply GoGatewayClient.patch`
   - chép `ZstdGrpcDecompressor.java`, `GoGatewayGrpcConfig.java` vào package
     `vn.softdreams.ebweb.service.reportclickhouse.gateway`
   - `pom.xml` thêm:
     ```xml
     <dependency>
         <groupId>com.github.luben</groupId>
         <artifactId>zstd-jni</artifactId>
         <version>1.5.5-11</version>
     </dependency>
     ```
4. Bật ở môi trường muốn thử: `go-gateway.grpc.batch=true` (mặc định **tắt** = vẫn gọi RPC cũ).

## Ghi chú cho người review

- `GoGatewayGrpcConfig` dùng `GrpcChannelConfigurer` (grpc-client-spring-boot-starter 2.14.0) vì kênh
  `@GrpcClient("go-gateway")` do starter tạo, không phải do code của mình. Đăng ký decompressor
  với `advertised = true` để client gửi `grpc-accept-encoding: zstd`; không khai báo thì Go
  vẫn gửi bình thường, không nén.
- Kích thước message ~0,1–0,3 MB/lô 600 dòng, xa giới hạn 4 MB mặc định của gRPC, không cần
  chỉnh `maxInboundMessageSize`.
- `onRow` vẫn nhận **từng dòng một** theo đúng thứ tự, nên phần ghi ra response không đổi.
- Đã kiểm tra: patch áp sạch lên `origin/dev-99-mix-200`; `GoGatewayClient`, `GoGatewayGrpcConfig`,
  `ZstdGrpcDecompressor` biên dịch được với contract mới; chạy client Java thật vào go-gateway
  cho `grpc-encoding = zstd` và kết quả giống RPC cũ. Chưa chạy qua mạng/VPN thật.

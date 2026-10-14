# 9. Checklist làm một sổ mới (từ proc đến go-live)

Đánh dấu từng mục. Mỗi mục có tham chiếu tới tài liệu giải thích.

## A. Phân tích proc
- [ ] Đã đọc proc, liệt kê các bước và tham số; ghi tên proc + số dòng vào comment ([03](03-chuyen-proc-sang-go-va-grain.md) mục 3.1).
- [ ] Mỗi bước gán vào một **grain** và nơi xử lý (SQL/Go) ([03](03-chuyen-proc-sang-go-va-grain.md) mục 3.2-3.3).
- [ ] Biết sổ này đọc bảng nào: GL/GLD (fact/staging), `repository_ledger`, hay bảng khác ([01](01-cau-truc-du-lieu.md)).
- [ ] Xác định phương pháp đầu kỳ (cộng dồn hay kết chuyển) và nó là lựa chọn của người dùng hay cố định ([05](05-so-du-dau-ky.md) mục 5.2).
- [ ] Nếu sổ chạm luồng tính giá xuất kho hoặc `repository_ledger_checkpoint`: **đã đọc `COSTING.md`** và sẽ cập nhật nó trong cùng lần sửa (`CLAUDE.md`).

## B. Dữ liệu và SQL
- [ ] Mọi truy vấn có `FINAL` + bộ lọc xoá, `company_id` + khoảng ngày để dùng khoá sắp xếp ([01](01-cau-truc-du-lieu.md) mục 1.4).
- [ ] `type_ledger` đúng quy ước `(= X OR = 2)` ([01](01-cau-truc-du-lieu.md)).
- [ ] Mã/tên tra bằng dictionary, không dùng cột snapshot.
- [ ] Số tiền `coalesce(.., 0)`; hai chế độ (nếu có) trả **cùng kiểu cột**; cột qualify bằng `f.`.
- [ ] `GROUP BY`/`HAVING` khớp proc (nếu có chế độ gộp); `ORDER BY` khớp proc **và có khoá phụ tất định** ([03](03-chuyen-proc-sang-go-va-grain.md) mục 3.4).
- [ ] Đầu kỳ là truy vấn riêng, trả "phát sinh trực tiếp" (không rollup trong SQL) ([05](05-so-du-dau-ky.md) mục 5.5).
- [ ] Nguồn thay thế (staging) có cùng tập cột, **có `gld.CompanyID`**, `FINAL` hai bảng ([04](04-nguon-du-lieu-gl-gld.md)).

## C. Go
- [ ] Package theo khuôn `types / query / query_opening / staging_source / service / handler / grpc-server / proto` ([02](02-go-gateway-cau-truc-code.md) mục 2.2).
- [ ] Mọi giá trị vào template qua bộ kiểm định dạng; không có placeholder nguyên văn trong comment SQL ([02](02-go-gateway-cau-truc-code.md) mục 2.4).
- [ ] Các truy vấn phụ độc lập chạy song song; walker dựng dòng tổng/lũy kế.
- [ ] Cờ nguồn dữ liệu theo quy ước (mặc định = hành vi hiện tại), có danh sách theo công ty; ghi log nguồn khi khởi động ([02](02-go-gateway-cau-truc-code.md) mục 2.6, [04](04-nguon-du-lieu-gl-gld.md)).
- [ ] Route HTTP đăng ký ở `NewRouter`; gRPC service đăng ký ở `main.go`.
- [ ] `go build ./...` và `go vet ./...` sạch.

## D. Proto và gRPC
- [ ] Proto chỉ **thêm** trường; không đổi tên package/service ([06](06-grpc-va-luong-stream.md) mục 6.2).
- [ ] Số tiền/số lượng là `string`.
- [ ] Sinh mã bằng đúng phiên bản công cụ; sinh lại proto chưa sửa ra file giống hệt.
- [ ] `rowToProto` gán **đủ mọi trường** (rà proto, `rowToProto`, `toDto` Java) ([06](06-grpc-va-luong-stream.md) mục 6.3).
- [ ] Lỗi tham số trả `InvalidArgument` nếu Java cần phân biệt.
- [ ] Cân nhắc RPC theo lô + zstd nếu báo cáo có thể rất lớn ([06](06-grpc-va-luong-stream.md) mục 6.5).

## E. Java
- [ ] Contract (`core-grpc-contract`) đã cập nhật, build, phát hành; `pom.xml` trỏ đúng phiên bản.
- [ ] Repository gRPC chỉ có `streamDirect`; dùng kênh chung; huỷ RPC khi client ngắt; dịch lỗi dùng bản chung ([07](07-be-java.md) mục 7.3, 7.8).
- [ ] Mọi truy vấn DB làm ở thread request gốc; không đọc ThreadLocal trong luồng stream.
- [ ] Không có biến thể trả `List`; không sort/gom nhóm ở Java.
- [ ] NDJSON một JSON một dòng; gzip `syncFlush`; header `X-Accel-Buffering`; lỗi giữa luồng là một dòng `{"type":"error"}`.
- [ ] Số tiền `BigDecimal` từ chuỗi, ra JSON bằng chuỗi; UUID swap ở Java (một hàm).
- [ ] Cờ bật/tắt theo công ty, **mặc định giữ luồng cũ**.
- [ ] Lớp ③④ theo luật tách service ([08](08-tach-service-rieng.md)).

## F. Kiểm thử đối chiếu với proc
- [ ] Số dòng chi tiết khớp; tổng Nợ/Có theo tài khoản (cả quy đổi và nguyên tệ) khớp.
- [ ] Đầu kỳ và cuối kỳ theo tài khoản khớp; dòng đầu/cuối khớp.
- [ ] Đã thử: chế độ gộp, tài khoản cha, đơn vị phụ thuộc, loại tiền khác `TH`, hai sổ, nhiều `cluster_id`.
- [ ] Công ty lớn nhất và công ty rỗng (rỗng phải ra rỗng, không lỗi).
- [ ] Chạy cùng tham số 2-3 lần: dãy dòng giống hệt nhau (tính tất định).
- [ ] Đo thời gian và RAM đỉnh ở công ty lớn nhất; nhánh staging có `grace_hash`.
- [ ] Client ngắt giữa chừng: Go dừng quét; RAM Java không phình khi báo cáo rất lớn.

## G. Rollout
- [ ] Cờ mặc định giữ hành vi cũ; bật cho một vài công ty trước.
- [ ] Thứ tự triển khai khi đổi hợp đồng: contract → go-gateway → Java ([06](06-grpc-va-luong-stream.md) mục 6.6).
- [ ] **Nhớ: push lên nhánh `dev` của go-gateway là tự động triển khai** ([02](02-go-gateway-cau-truc-code.md) mục 2.7).
- [ ] Có đường lùi (đổi biến môi trường rồi khởi động lại; hoặc quay về endpoint cũ ở frontend).
- [ ] Theo dõi: log nguồn dữ liệu, `elapsedMs`, tỉ lệ lỗi, `DEADLINE_EXCEEDED`.

## H. Việc cuối
- [ ] Cập nhật tài liệu này nếu phát hiện bẫy mới. Mỗi mục bẫy phải trả lời đủ ba ý: **X phải là Y, vì sao, vi phạm thì triệu chứng gì**. Không ghi kiểu "hãy cẩn thận".

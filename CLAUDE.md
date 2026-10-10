# gogateway

Satellite đọc dữ liệu từ DWH ClickHouse qua native protocol, phục vụ báo cáo và các dataset
của hệ thống kế toán. Không ghi dữ liệu, chỉ đọc.

---

## Tính giá xuất kho

Quy tắc nghiệp vụ của luồng tính giá vốn hàng xuất kho nằm ở `COSTING.md`, **cùng thư mục với file này**. Không chép nội dung đó vào đây.

### Trước khi sửa

1. **Chưa rõ phải sửa ở repo nào, đường dẫn nào thì DỪNG và hỏi người giao việc.** Luồng này trải trên nhiều repo, đoán sai repo là sửa nhầm chỗ mà vẫn biên dịch được, không có gì báo lỗi.
2. Đọc `COSTING.md`.
3. Việc được giao **mâu thuẫn** với một bất biến trong đó thì **DỪNG, nêu rõ mâu thuẫn, hỏi lại**. Không tự ý đổi bất biến.

### Báo cáo đã đọc — và chỉ báo khi thật sự đã đọc

Đọc rồi thì ghi vào câu trả lời, kèm **số hiệu mục đã áp dụng và tóm tắt nội dung mục đó**:

    Đã đọc COSTING.md — áp dụng mục 3: thành tiền lấy MainQuantity, không dựng lại từ Quantity.

**Chưa mở file thì tuyệt đối không ghi dòng này.** Ghi mà chưa đọc là nói dối, và nguy hiểm hơn im lặng vì người đọc sẽ tin rằng đã có người kiểm tra. Chưa đọc thì nói thẳng là chưa đọc.

Lý do phải trích số hiệu mục kèm nội dung: đó là thứ **mở file ra đối chiếu trong vài giây là biết thật hay bịa**. Câu "tôi đã đọc" suông thì không kiểm được gì.

### Sau khi sửa — bắt buộc

Thay đổi nào **chạm vào nghiệp vụ** thì phải cập nhật `COSTING.md` **trong cùng lần sửa**, không để lần sau:

| Tình huống | Làm gì với `COSTING.md` |
|---|---|
| Thêm loại chứng từ, thêm trường hợp nghiệp vụ | Thêm mục mới vào phần bất biến |
| Đổi công thức, đổi thứ tự xử lý, đổi khoá ghép | Sửa mục tương ứng |
| Sửa xong một rủi ro đang nằm ở mục *Rủi ro đã biết* | Xoá khỏi mục đó |
| Vừa sửa một bug mà nguyên nhân **không suy ra được từ code** | Thêm bất biến mới, **kèm ngày và triệu chứng thật** |
| Làm xong một case trước đây ghi là chưa làm | Cập nhật bảng phạm vi |

**Không** cập nhật khi chỉ đổi tên biến, format, refactor nội bộ, thêm log, sửa chính tả. `COSTING.md` chứa điều **không suy ra được từ code**, không phải bản mô tả code.

### Viết một mục mới theo dạng nào

Mỗi mục phải trả lời đủ ba ý: **X phải là Y** — **vì sao** — **vi phạm thì triệu chứng gì**.

Câu kiểu *"hãy cẩn thận"*, *"nhớ kiểm tra kỹ"* là vô dụng, không đưa vào.

### Thay đổi ở đâu thì sửa file nào

| Thay đổi thuộc về | Sửa `COSTING.md` của |
|---|---|
| Query DWH, ClickHouse | `gogateway` |
| Công thức, phân loại trường hợp nghiệp vụ | `ebinventory` |
| Ghi kết quả, làm tròn, ranh giới giao dịch | `ebqueue` |
| Store cập nhật chứng từ | `ebwebbe` |

**Không chép nội dung sang repo khác.** Ngoại lệ duy nhất là hợp đồng giữa `ebinventory` và `ebqueue`: chủ sở hữu là `ebinventory`, đổi thì phải sửa cả hai phía và nói rõ trong câu trả lời.

### Kết thúc mỗi lần sửa

Ghi một trong hai dòng:

- `Đã cập nhật COSTING.md: <mục nào, thêm hay sửa>`
- `Không cập nhật COSTING.md vì <lý do>`

Thiếu dòng này nghĩa là chưa làm xong.

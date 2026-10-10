# Tính giá xuất kho — tầng đọc DWH

Tầng này trả dữ liệu thô cho `ebinventory` tính giá vốn hàng xuất kho. Sai ở đây làm lệch tiền trên sổ kế toán, và **không có exception nào báo ra** — chỉ ra số khác.

Code liên quan trong repo này: `internal/datasets/tinh-gia-xuat-kho/` — `query.sql`, `opening_stock_raw.sql`, `opening_stock_checkpoint.sql`, `service.go`.

Dưới đây là các bất biến **không suy ra được từ code**. Đổi thì phải đổi cả bên tiêu thụ.

**Quy trình sửa và nghĩa vụ cập nhật file này**: xem mục *Tính giá xuất kho* trong `CLAUDE.md` cùng thư mục.

---

## 1. `ORDER BY` ở cuối `query.sql` là ngữ nghĩa

```sql
ORDER BY MaterialGoodsID, RepositoryID, IsOpeningStock DESC, PostedDate
```

`ebinventory` **không gom nhóm bằng Map**. Nó duyệt tuần tự, thấy đổi cặp `(MaterialGoodsID, RepositoryID)` là chốt nhóm cũ và tính ra một đơn giá.

Bỏ hoặc đổi thứ tự hai cột đầu → một cặp vật tư × kho bị tách thành nhiều nhóm, mỗi nhóm một đơn giá khác nhau. Không lỗi, không cảnh báo, chỉ sai tiền.

`IsOpeningStock DESC` phải đứng trước `PostedDate` để dòng tồn đầu kỳ ra trước dòng phát sinh.

## 2. Thứ tự cột trong `SELECT` là hợp đồng vị trí

`scanRow` trong `service.go` đọc theo **chỉ số cột**, không theo tên. Thêm, bớt hay đảo cột trong bất kỳ nhánh `UNION ALL` nào mà không sửa `scanRow` sẽ gán nhầm giá trị sang trường khác.

Ba nhánh `UNION ALL` phải có **cùng số cột, cùng thứ tự, cùng kiểu**.

## 3. Dòng tồn đầu kỳ được cộng dồn, không phải gán

`CostingServiceImpl.calculateByGroup` dùng `.add()` cho mọi dòng `IsOpeningStock = true`. Vì vậy **trả nhiều dòng tồn đầu kỳ cho cùng một cặp vật tư × kho là hợp lệ** — nhánh checkpoint đang trả hai dòng.

Đừng "tối ưu" bằng cách gộp lại thành một dòng nếu không có lý do, và tuyệt đối đừng đổi bên tiêu thụ sang phép gán.

## 4. `TypeLedger = 2` là bút toán của cả hai sổ

```sql
AND (TypeLedger = {{TYPE_LEDGER}} OR TypeLedger = 2)
```

0 là sổ tài chính, 1 là sổ quản trị, 2 thuộc **cả hai**. Quy tắc này có từ hệ thống cũ, lặp ở mọi query sổ kho. Bỏ vế `OR ... = 2` là mất một phần phát sinh.

Bảng `eb.repository_ledger_checkpoint` dùng cột `type_ledger` với **cùng quy ước** 0/1/2, nên cũng phải `OR = 2`.

## 5. Tồn đầu kỳ có hai nguồn, nguồn raw là đường lùi bắt buộc giữ

| Biến môi trường | Thân dùng |
|---|---|
| không set, hoặc giá trị khác | `opening_stock_raw.sql` |
| `TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE=checkpoint` | `opening_stock_checkpoint.sql` |

`eb.repository_ledger` và `eb.repository_ledger_checkpoint` được ghi bằng **hai thao tác tách rời, không cùng đúng cùng sai**. Checkpoint lệch thì tồn đầu kỳ sai âm thầm. Xoá biến môi trường rồi khởi động lại là quay về tính toàn bộ, không phải build lại.

**Không xoá nhánh raw vì tưởng là code chết.**

## 6. Checkpoint phải dùng `argMax`, không dùng `max` hay `sum`

```sql
argMax(ckp.cumulative_qty, ckp.month)
```

Nhóm gồm **nhiều tháng**, cần giá trị của tháng mới nhất.

- `sum()` sai vì `cumulative_*` đã là luỹ kế, cộng lại là tính lịch sử nhiều lần.
- `max(cumulative_qty)` sai vì tồn kho **giảm được** — giá trị lớn nhất không phải giá trị mới nhất.

Số lượng và trị giá phải lấy **cùng một mốc tháng**, nên không tách thành hai hàm khác nhau được.

## 7. Tháng khuyết nghĩa là tháng không phát sinh

Nhờ vậy dòng checkpoint mới nhất còn trước tháng `FROM_DATE` đã bao trọn tới **hết tháng liền trước**, và phần lẻ luôn chỉ là khoảng từ đầu tháng `FROM_DATE` đến `FROM_DATE`.

Phải tra bằng `month < toYYYYMM(FROM_DATE)` rồi lấy tháng lớn nhất. **So bằng là ra rỗng** khi tháng đó không phát sinh.

## 8. Nhánh checkpoint phải chặn theo tập key của `ledger_scope`

```sql
AND (ckp.repository_id, ckp.material_goods_id) IN
    (SELECT RepositoryID, MaterialGoodsID FROM ledger_scope)
```

Nhánh này đọc thẳng bảng checkpoint nên **không đi qua** `REPOSITORY_FILTER` và `MATERIAL_GOODS_FILTER`. Bỏ chốt chặn là trả tồn đầu kỳ của mọi kho và vật tư trong công ty, rồi `ebinventory` tính giá cho cả vật tư không ai yêu cầu và ghi đè chứng từ của chúng.

## 9. `FINAL` là bắt buộc trên cả hai bảng

`eb.repository_ledger` và `eb.repository_ledger_checkpoint` đều là `ReplacingMergeTree`. Không `FINAL` thì các bản chưa merge bị tính trùng.

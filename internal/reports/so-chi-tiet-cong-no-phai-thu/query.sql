-- Sổ chi tiết công nợ phải thu — thay Proc_Chi_Tiet_Cong_No_Phai_Thu.
--
-- Một truy vấn duy nhất trả cả hai loại dòng, đã sắp theo đối tượng:
--   order_type = 0  Số dư đầu kỳ  (một dòng mỗi đối tượng, nếu số dư khác 0)
--   order_type = 1  Phát sinh trong [FROM_DATE, TO_DATE]
-- Số dư luỹ kế từng dòng KHÔNG tính ở đây mà ở walker (service.go) vì cần cộng dồn
-- theo thứ tự dòng, như bước "Tính số tồn" cuối proc.
--
-- 19 cột của hai nhánh phải CÙNG tên/kiểu/thứ tự với nhau và với scanRow.
{{AO_CTE}}
SELECT
    order_type, object_id, object_code, object_name,
    reference_id, type_id, posted_date, voucher_date, no,
    invoice_date, invoice_no, description, account_corresponding,
    debit_amount_nt, debit_amount, credit_amount_nt, credit_amount,
    order_priority, tiebreak
FROM (
{{OPENING_BRANCH}}
UNION ALL
{{DETAIL_BRANCH}}
)
{{ORDER_BY}}
{{QUERY_SETTINGS}}

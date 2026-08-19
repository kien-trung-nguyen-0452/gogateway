-- =============================================================================
-- SỔ KẾ TOÁN CHI TIẾT QUỸ TIỀN MẶT — phát sinh trong kỳ
-- =============================================================================
-- Nguồn tham chiếu: Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT
--
-- ── VÌ SAO TÁCH THU/CHI Ở SQL CHỨ KHÔNG Ở GO ────────────────────────────────
-- Proc sắp xếp TRƯỚC khi tính số dư luỹ kế (dòng 1309):
--     ORDER BY Account, PositionOrder, PostedDate, Date, CAType,
--              ReceiptRefNo, PaymentRefNo, OrderPriority
--
-- CAType đứng TRƯỚC số hiệu chứng từ. Nghĩa là trong cùng một ngày, TẤT CẢ
-- phiếu thu chạy trước rồi mới tới TẤT CẢ phiếu chi:
--     đúng:  bt1-thu, bt2-thu, bt1-chi, bt2-chi
--     sai:   bt1-thu, bt1-chi, bt2-thu, bt2-chi
--
-- Số dư CUỐI CÙNG giống nhau (phép cộng giao hoán), nhưng SoTon ở TỪNG DÒNG
-- khác nhau — mà đó chính là cột người dùng nhìn. Nếu để Go tách thì mỗi bút
-- toán phát ngay hai dòng liền nhau, sai thứ tự. UNION ALL để ClickHouse sắp
-- đúng, Go chỉ cộng dồn theo thứ tự nhận được.
--
-- ── BÍ DANH `f` LÀ BẮT BUỘC ─────────────────────────────────────────────────
-- Ở chế độ gộp, alias của SELECT che mất cột gốc cùng tên và ClickHouse cho
-- phép tham chiếu alias SELECT trong WHERE:
--     Code 184: Aggregate function sum(...) is found in WHERE
--
-- ── LỌC SỐ TIỀN ─────────────────────────────────────────────────────────────
-- Proc gốc lọc trong WHERE ở CẢ HAI chế độ (khác Proc_SO_CHI_TIET_CAC_TAI_KHOAN
-- vốn dùng HAVING khi gộp). Riêng điều kiện tách nhánh thu/chi phải áp SAU khi
-- gộp, nên đặt ở lớp ngoài của CTE.
-- =============================================================================

WITH base AS (
    SELECT
   {{REF_ID_EXPR}}                              AS ref_id,
   {{TYPE_ID_EXPR}}                             AS type_id,
   {{DATE_EXPR}}                                AS voucher_date,
   {{POSTED_DATE_EXPR}}                         AS posted_date,
   {{NO_EXPR}}                                  AS ref_no,
   {{REASON_EXPR}}                              AS reason,
   {{JOURNAL_MEMO_EXPR}}                        AS journal_memo,
   {{ACCOUNT_EXPR}}                             AS account,
   {{ACCOUNT_CORRESPONDING_EXPR}}               AS account_corresponding,
-- Cột hiển thị, chọn theo typeShowCurrency
   {{DEBIT_EXPR}}                               AS debit_show,
   {{CREDIT_EXPR}}                              AS credit_show,
-- Luôn là bản quy đổi, dùng cho SoTonQD
   {{DEBIT_QD_EXPR}}                            AS debit_qd,
   {{CREDIT_QD_EXPR}}                           AS credit_qd,
   {{EXCHANGE_RATE_EXPR}}                       AS exchange_rate,
   {{ORDER_PRIORITY_EXPR}}                      AS order_priority,
   {{AO_CODE_EXPR}}                             AS accounting_object_code,
   {{AO_NAME_EXPR}}                             AS accounting_object_name,
   {{CF1}} AS custom_field1, {{CF2}} AS custom_field2, {{CF3}} AS custom_field3,
   {{CF4}} AS custom_field4, {{CF5}} AS custom_field5,
   {{CFD1}} AS custom_field_detail1, {{CFD2}} AS custom_field_detail2,
   {{CFD3}} AS custom_field_detail3, {{CFD4}} AS custom_field_detail4,
   {{CFD5}} AS custom_field_detail5
FROM eb_dwh.fact_gl_entry_line AS f
WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
  AND f.posted_date >= toDate('{{FROM_DATE}}')
  AND f.posted_date <= toDate('{{TO_DATE}}')
-- type_ledger = 2 là bút toán dùng chung cho cả sổ tài chính lẫn quản trị
  AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
  AND f.account_number IN ({{ACCOUNT_NUMBERS}})
-- Fact là ReplacingMergeTree(__version, is_deleted). Không lọc thì dòng
-- đã xoá vẫn xuất hiện.
  AND f.is_deleted = 0
-- Proc gốc chỉ kiểm hai cột quy đổi, KHÔNG kiểm cột nguyên tệ.
  AND (coalesce(f.debit_amount, 0) != 0 OR coalesce(f.credit_amount, 0) != 0)
    {{CURRENCY_FILTER}}
    {{CLUSTER_FILTER}}
    {{GROUP_BY_CLAUSE}}
    )
SELECT * FROM (
                  -- ── NHÁNH THU (CAType = 0) ──────────────────────────────────────────────
                  SELECT
                      ref_id, type_id, voucher_date, posted_date,
                      coalesce(ref_no, '')                       AS receipt_ref_no,
                      ''                           AS payment_ref_no,
                      toInt32(0)                            AS ca_type,
                      reason, journal_memo, account, account_corresponding,
                      debit_show                   AS phat_sinh_no,
                      toDecimal128(0, 10)          AS phat_sinh_co,
                      debit_qd                     AS phat_sinh_no_qd,
                      toDecimal128(0, 10)          AS phat_sinh_co_qd,
                      exchange_rate, order_priority,
                      accounting_object_code, accounting_object_name,
                      custom_field1, custom_field2, custom_field3, custom_field4, custom_field5,
                      custom_field_detail1, custom_field_detail2, custom_field_detail3,
                      custom_field_detail4, custom_field_detail5
                  FROM base
                  WHERE debit_show != 0

                  UNION ALL

                  -- ── NHÁNH CHI (CAType = 1) ──────────────────────────────────────────────
                  SELECT
                      ref_id, type_id, voucher_date, posted_date,
                      ''                           AS receipt_ref_no,
                      coalesce(ref_no, '')                       AS payment_ref_no,
                      toInt32(1)                            AS ca_type,
                      reason, journal_memo, account, account_corresponding,
                      toDecimal128(0, 10)          AS phat_sinh_no,
                      credit_show                  AS phat_sinh_co,
                      toDecimal128(0, 10)          AS phat_sinh_no_qd,
                      credit_qd                    AS phat_sinh_co_qd,
                      exchange_rate, order_priority,
                      accounting_object_code, accounting_object_name,
                      custom_field1, custom_field2, custom_field3, custom_field4, custom_field5,
                      custom_field_detail1, custom_field_detail2, custom_field_detail3,
                      custom_field_detail4, custom_field_detail5
                  FROM base
                  WHERE credit_show != 0
              )
-- Khớp chính xác ORDER BY dòng 1309 của proc. PositionOrder bỏ qua vì Go tự
-- chèn dòng tồn đầu kỳ và cộng nhóm đúng chỗ.
-- indexOf() giữ đúng thứ tự tài khoản người dùng chọn.
ORDER BY
    indexOf([{{ACCOUNT_ORDER}}], account),
    account,
    posted_date,
    voucher_date,
    ca_type,
    receipt_ref_no,
    payment_ref_no,
    order_priority
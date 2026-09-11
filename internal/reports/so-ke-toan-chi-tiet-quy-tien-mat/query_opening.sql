-- =============================================================================
-- TỒN ĐẦU KỲ — cộng dồn GL trước from_date
-- =============================================================================
-- Nguồn tham chiếu: Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT dòng 458-519
--
-- ── ĐÂY LÀ ĐÚNG, CHO PHƯƠNG PHÁP MẶC ĐỊNH ───────────────────────────────────
-- Frontend có tuỳ chọn "Phương pháp xác định số dư đầu kỳ" (openingBalanceMethod):
--
--   [1] "Theo số liệu tổng hợp từ chứng từ"  ← MẶC ĐỊNH, file này phục vụ
--       "Hệ thống xác định số dư đầu kỳ bằng cách tổng hợp toàn bộ chứng từ
--        phát sinh trước kỳ báo cáo."
--       → cộng dồn GL, ĐÚNG như file này làm.
--
--   [2] "Theo kết chuyển số dư cuối năm trước"
--       "Hệ thống sử dụng số dư cuối kỳ đã được kết chuyển từ năm trước."
--       → dùng bảng số dư chốt kỳ. OLTP có PROC RIÊNG cho nhánh này:
--         Proc_SO_QUY_TIEN_MAT_THEO_KET_CHUYEN_SO_DU_CUOI_NAM
--         ("thay đổi cách tính số dư đầu kỳ").
--       → bản DWH tương ứng: query_opening_period.sql (CHƯA nối vào luồng).
--
-- ĐỪNG "sửa" file này thành đọc fact_account_opening_period. Hai phương pháp
-- là LỰA CHỌN CỦA NGƯỜI DÙNG, không phải cái này đúng cái kia sai. Đổi nguồn
-- ở đây là làm sai phương pháp [1].
--
-- Việc còn thiếu: đọc openingBalanceMethod từ request rồi rẽ nhánh sang
-- query_opening_period.sql khi = 2. Xem ghi chú ở file đó.
--
-- ── PHÂN VAI HAI CẶP CỘT TIỀN ───────────────────────────────────────────────
-- Khớp query.sql và cách frontend bind cột:
--     debit_oc / credit_oc  → NGUYÊN TỆ → SoTon
--     debit_qd / credit_qd  → QUY ĐỔI   → SoTonQD
--
-- Proc dùng đúng phân vai này cho tồn đầu kỳ (dòng 1065-1095): SoTon lấy
-- DebitAmountOriginal, SoTonQD lấy DebitAmount — KHÔNG theo @typeShowCurrency.
--
-- ── KẾT QUẢ CHƯA PHẢI SỐ CUỐI ───────────────────────────────────────────────
-- GROUP BY account_number nên mỗi tài khoản một dòng, chỉ gồm phần hạch toán
-- TRỰC TIẾP vào đúng mã đó. Tài khoản CHA ("111") phải gom cả nhánh con — việc
-- đó do Service.rollupOpeningBalances() làm sau khi đọc file này. ĐỪNG thêm
-- rollup vào đây, sẽ đếm trùng hai lần.
--
-- ── FINAL LÀ BẮT BUỘC ───────────────────────────────────────────────────────
-- fact_gl_entry_line là ReplacingMergeTree và PARTITION BY tháng, còn câu này
-- quét MỌI partition từ đầu lịch sử. Không FINAL thì tháng nào còn part chưa
-- merge là cộng trùng version ở đó, sai số tích luỹ qua hàng chục tháng — và
-- vì merge bất định, hai lần chạy cùng tham số có thể ra hai con số khác nhau.
-- =============================================================================

SELECT
    f.account_number                                AS account,
    sum(coalesce(f.debit_amount_original, 0))       AS debit_oc,
    sum(coalesce(f.credit_amount_original, 0))      AS credit_oc,
    sum(coalesce(f.debit_amount, 0))                AS debit_qd,
    sum(coalesce(f.credit_amount, 0))               AS credit_qd
FROM {{GL_SOURCE}}
WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
  AND f.posted_date < toDate('{{FROM_DATE}}')
  AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
  AND f.account_number IN ({{ACCOUNT_NUMBERS}})
  AND f.is_deleted = 0
  AND (coalesce(f.debit_amount, 0) != 0 OR coalesce(f.credit_amount, 0) != 0)
  {{CURRENCY_FILTER}}
  {{CLUSTER_FILTER}}
GROUP BY f.account_number
-- =============================================================================
-- TỒN ĐẦU KỲ — tổng phát sinh TRƯỚC from_date
-- =============================================================================
-- ── VÌ SAO KHỚP CHÍNH XÁC MÃ, KHÔNG CỘNG DỒN THEO CÂY ───────────────────────
-- Proc gốc (dòng 1045–1095) dựng cây đệ quy cho TỪNG tài khoản rồi cộng cả
-- nhánh. Nhưng danh sách tài khoản truyền vào proc ĐÃ ĐƯỢC MỞ RỘNG từ trước
-- bởi DynamicReportQuyServiceImpl (getListChildAccount), nên mỗi phần tử là
-- một tài khoản lá và orderedTree(lá) chỉ là chính nó — rollup thành no-op.
--
-- Ở bản DWH, việc mở rộng do Service.expandAccounts() làm qua
-- bridge_account_hierarchy. Nếu file này CŨNG cộng dồn theo cây thì tồn đầu kỳ
-- của tài khoản cha sẽ ĐẾM TRÙNG phần đã có ở các tài khoản con — vì cả cha
-- lẫn con đều nằm trong danh sách sau khi expand.
--
-- Chọn một trong hai, không được cả hai. Đây là khớp chính xác mã.
--
-- ── LƯU Ý CHO DEV SAU: PROC TRỘN LOẠI TIỀN ──────────────────────────────────
-- Proc dùng SUM(DebitAmountOriginal) cho SoTon và SUM(DebitAmount) cho SoTonQD
-- BẤT KỂ @typeShowCurrency, trong khi dòng chi tiết lại đổi theo tham số đó:
--     (CASE WHEN @typeShowCurrency = 0 THEN DebitAmount ELSE DebitAmountOriginal END)
--
-- Với typeShowCurrency = 0, số dư luỹ kế thành "tồn đầu kỳ nguyên tệ + phát
-- sinh quy đổi" — trộn hai loại tiền. Công ty chỉ dùng VND thì hai cột bằng
-- nhau nên không lộ; có ngoại tệ thì sai.
--
-- File này SAO CHÉP Y HỆT hành vi đó để khớp OLTP. Khi quyết định sửa, đổi
-- debit_oc/credit_oc thành cột theo typeShowCurrency giống query.sql.
--
-- Kết quả một dòng mỗi tài khoản nên nạp hết vào map an toàn — khác hẳn phần
-- chi tiết vốn phải stream.
--
-- Bí danh `f` dùng chung với query.sql để {{CURRENCY_FILTER}} và
-- {{CLUSTER_FILTER}} — do cùng một hàm sinh ra — chèn được vào cả hai file.
-- =============================================================================

SELECT
    f.account_number                                AS account,
    -- SoTon: proc dùng nguyên tệ, KHÔNG theo typeShowCurrency (xem ghi chú)
    sum(coalesce(f.debit_amount_original, 0))       AS debit_oc,
    sum(coalesce(f.credit_amount_original, 0))      AS credit_oc,
    -- SoTonQD: luôn quy đổi
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
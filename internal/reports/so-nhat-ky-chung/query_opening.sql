-- "Số lũy kế kỳ trước chuyển sang" (@IsShowAccumAmount = 1): tổng Nợ/Có của MỌI
-- bút toán trước FROM_DATE.
--
-- Proc đọc SummaryOpeningBalance (các kỳ trước) rồi cộng thêm GL từ đầu kỳ đến
-- ngày trước FROM_DATE. DWH không có bảng tổng hợp tương đương nên cộng thẳng từ
-- fact: cùng ý nghĩa nếu SummaryOpeningBalance đúng là tổng GL các kỳ trước.
-- KHÔNG lọc tài khoản đối ứng (proc cũng không lọc ở bước này).
--
-- Loại TypeID 740, 741, 742 (chứng từ kết chuyển) như proc.
--
-- posted_date >= 1990-01-01: fact có dòng posted_date rác (1970, 2106) mang số
-- tiền; ngày hạch toán thật không bao giờ trước năm 1990.
SELECT
    sum(coalesce(f.debit_amount, 0))              AS debit_sum,
    sum(coalesce(f.credit_amount, 0))             AS credit_sum,
    sum(coalesce(f.debit_amount_original, 0))     AS debit_orig_sum,
    sum(coalesce(f.credit_amount_original, 0))    AS credit_orig_sum
FROM {{GL_SOURCE}}
WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
  AND f.posted_date >= toDate('1990-01-01')
  AND f.posted_date < toDate('{{FROM_DATE}}')
  AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
  AND ifNull(f.type_id, 0) NOT IN (740, 741, 742)
  AND f.is_deleted = 0
  {{CLUSTER_FILTER}}

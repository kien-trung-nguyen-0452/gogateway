-- SỐ DƯ ĐẦU KỲ: tổng Nợ/Có của mọi bút toán trước FROM_DATE, mỗi đối tượng một dòng.
--
-- Proc đọc SummaryOpeningBalance (các kỳ trước) rồi cộng thêm GL từ đầu tháng đến
-- ngày trước FROM_DATE. DWH không có bảng tổng hợp theo đối tượng nên cộng thẳng từ
-- fact: cùng ý nghĩa nếu SummaryOpeningBalance đúng là tổng GL các kỳ trước.
--
-- posted_date >= 1990-01-01: fact có dòng posted_date rác (1970, 2106) mang số
-- tiền; ngày hạch toán thật không bao giờ trước năm 1990.
--
-- HAVING: proc bỏ dòng đầu kỳ có số dư bằng 0 (cả nguyên tệ lẫn quy đổi).
    SELECT
        toInt32(0)                                              AS order_type,
        f.accounting_object_id                                  AS object_id,
        ao.code                                                 AS object_code,
        ao.name                                                 AS object_name,
        CAST(NULL AS Nullable(UUID))                            AS reference_id,
        CAST(NULL AS Nullable(Int32))                           AS type_id,
        CAST(NULL AS Nullable(Date))                            AS posted_date,
        CAST(NULL AS Nullable(Date))                            AS voucher_date,
        ''                                                      AS no,
        CAST(NULL AS Nullable(Date))                            AS invoice_date,
        ''                                                      AS invoice_no,
        ''                                                      AS description,
        ''                                                      AS account_corresponding,
        toDecimal128(sum(coalesce(f.debit_amount_original, 0)), 10)  AS debit_amount_nt,
        toDecimal128(sum(coalesce(f.debit_amount, 0)), 10)           AS debit_amount,
        toDecimal128(sum(coalesce(f.credit_amount_original, 0)), 10) AS credit_amount_nt,
        toDecimal128(sum(coalesce(f.credit_amount, 0)), 10)          AS credit_amount,
        toInt32(0)                                              AS order_priority,
        ''                                                      AS tiebreak
    FROM {{GL_SOURCE_OPENING}}
    INNER JOIN ao ON ao.id = f.accounting_object_id
    WHERE f.posted_date >= toDate('1990-01-01')
      AND f.posted_date < toDate('{{FROM_DATE}}')
      {{BASE_FILTER}}
      {{HAS_DATA_FILTER}}
    GROUP BY f.accounting_object_id, ao.code, ao.name
    HAVING sum(coalesce(f.credit_amount, 0)) - sum(coalesce(f.debit_amount, 0)) != 0
        OR sum(coalesce(f.credit_amount_original, 0)) - sum(coalesce(f.debit_amount_original, 0)) != 0

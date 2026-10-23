-- CHẾ ĐỘ GỘP (@IsSimilarSum = 1): cộng các dòng cùng đối tượng / chứng từ / ngày /
-- số hoá đơn / diễn giải / tài khoản đối ứng / tỷ giá — đúng khoá GROUP BY của proc.
-- Khoá gộp cắt khoảng trắng CUỐI của chuỗi (trimRight): SQL Server so sánh chuỗi bỏ qua
-- khoảng trắng cuối nên 'Bia cao' và 'Bia cao ' vào cùng một nhóm; ClickHouse thì không.
-- Diễn giải ở nhánh gộp là AOL.Reason (lý do của chứng từ), KHÁC nhánh không gộp dùng
-- ISNULL(GLD.Description, AOL.Reason): hai dòng cùng chứng từ khác Description vẫn gộp.
-- Proc gộp xong mới loại dòng có cả bốn cột tiền bằng 0, nên HAVING ở đây.
    SELECT
        toInt32(1)                                              AS order_type,
        f.accounting_object_id                                  AS object_id,
        ao.code                                                 AS object_code,
        ao.name                                                 AS object_name,
        f.reference_id                                          AS reference_id,
        f.type_id                                               AS type_id,
        f.posted_date                                           AS posted_date,
        f.voucher_date                                          AS voucher_date,
        trimRight(ifNull(f.{{NO_COL}}, ''))                                AS no,
        f.invoice_date                                          AS invoice_date,
        trimRight(ifNull(f.invoice_no, ''))                                AS invoice_no,
        trimRight(ifNull(f.reason, ''))             AS description,
        trimRight(ifNull(f.account_corresponding, ''))                     AS account_corresponding,
        toDecimal128(sum(coalesce(f.debit_amount_original, 0)), 10)  AS debit_amount_nt,
        toDecimal128(sum(coalesce(f.debit_amount, 0)), 10)           AS debit_amount,
        toDecimal128(sum(coalesce(f.credit_amount_original, 0)), 10) AS credit_amount_nt,
        toDecimal128(sum(coalesce(f.credit_amount, 0)), 10)          AS credit_amount,
        toInt32(min(ifNull(f.order_priority, 0)))               AS order_priority,
        toString(f.reference_id)                                AS tiebreak
    FROM {{GL_SOURCE_RANGE}}
    INNER JOIN ao ON ao.id = f.accounting_object_id
    WHERE f.posted_date >= toDate('{{FROM_DATE}}')
      AND f.posted_date <= toDate('{{TO_DATE}}')
      {{BASE_FILTER}}
    GROUP BY f.accounting_object_id, ao.code, ao.name,
             f.reference_id, f.type_id, f.posted_date, f.voucher_date,
             trimRight(ifNull(f.{{NO_COL}}, '')), f.invoice_date, trimRight(ifNull(f.invoice_no, '')),
             trimRight(ifNull(f.reason, '')),
             trimRight(ifNull(f.account_corresponding, '')), f.exchange_rate
    HAVING sum(coalesce(f.debit_amount, 0)) != 0
        OR sum(coalesce(f.credit_amount, 0)) != 0
        OR sum(coalesce(f.debit_amount_original, 0)) != 0
        OR sum(coalesce(f.credit_amount_original, 0)) != 0

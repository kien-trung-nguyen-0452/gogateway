-- CHẾ ĐỘ KHÔNG GỘP (@IsSimilarSum = 0): mỗi dòng fact là một dòng sổ.
-- Proc loại dòng có cả bốn cột tiền bằng 0 (HAVING SUM(...) <> 0).
    SELECT
        toInt32(1)                                              AS order_type,
        f.accounting_object_id                                  AS object_id,
        ao.code                                                 AS object_code,
        ao.name                                                 AS object_name,
        f.reference_id                                          AS reference_id,
        f.type_id                                               AS type_id,
        f.posted_date                                           AS posted_date,
        f.voucher_date                                          AS voucher_date,
        ifNull(f.{{NO_COL}}, '')                                AS no,
        f.invoice_date                                          AS invoice_date,
        ifNull(f.invoice_no, '')                                AS invoice_no,
        ifNull(f.description, ifNull(f.reason, ''))             AS description,
        ifNull(f.account_corresponding, '')                     AS account_corresponding,
        toDecimal128(coalesce(f.debit_amount_original, 0), 10)  AS debit_amount_nt,
        toDecimal128(coalesce(f.debit_amount, 0), 10)           AS debit_amount,
        toDecimal128(coalesce(f.credit_amount_original, 0), 10) AS credit_amount_nt,
        toDecimal128(coalesce(f.credit_amount, 0), 10)          AS credit_amount,
        toInt32(ifNull(f.order_priority, 0))                    AS order_priority,
        f.line_bk                                               AS tiebreak
    FROM {{GL_SOURCE_RANGE}}
    INNER JOIN ao ON ao.id = f.accounting_object_id
    WHERE f.posted_date >= toDate('{{FROM_DATE}}')
      AND f.posted_date <= toDate('{{TO_DATE}}')
      {{BASE_FILTER}}
      AND (coalesce(f.debit_amount, 0) != 0
        OR coalesce(f.credit_amount, 0) != 0
        OR coalesce(f.debit_amount_original, 0) != 0
        OR coalesce(f.credit_amount_original, 0) != 0)

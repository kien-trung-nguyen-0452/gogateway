SELECT
    {{KEY_ID_EXPR}}                                  AS key_id,
    {{REFERENCE_ID_EXPR}}                            AS reference_id,
    {{TYPE_ID_EXPR}}                                 AS type_id,
    {{POSTED_DATE_EXPR}}                             AS posted_date,
    {{NO_EXPR}}                                      AS no,
    {{INVOICE_NO_EXPR}}                              AS invoice_no,
    {{INVOICE_DATE_EXPR}}                            AS invoice_date,
    {{REASON_EXPR}}                                  AS reason,
    {{JOURNAL_MEMO_EXPR}}                            AS journal_memo,
    {{ACCOUNT_NUMBER_EXPR}}                          AS account_number,
    {{ACCOUNT_CORRESPONDING_EXPR}}                   AS account_corresponding,
    {{DEBIT_EXPR}}                                   AS debit_amount,
    {{CREDIT_EXPR}}                                  AS credit_amount,
    {{DEBIT_ORIG_EXPR}}                              AS debit_amount_original,
    {{CREDIT_ORIG_EXPR}}                             AS credit_amount_original,
    {{EXCHANGE_RATE_EXPR}}                           AS exchange_rate,
    {{ORDER_PRIORITY_EXPR}}                          AS order_priority,
    {{IS_UNREASONABLE_COST_EXPR}}                    AS is_unreasonable_cost,
    {{AO_CODE_EXPR}}                                 AS accounting_object_code,
    {{AO_NAME_EXPR}}                                 AS accounting_object_name,
    {{CF1}} AS custom_field1, {{CF2}} AS custom_field2, {{CF3}} AS custom_field3,
    {{CF4}} AS custom_field4, {{CF5}} AS custom_field5,
    {{CFD1}} AS custom_field_detail1, {{CFD2}} AS custom_field_detail2,
    {{CFD3}} AS custom_field_detail3, {{CFD4}} AS custom_field_detail4,
    {{CFD5}} AS custom_field_detail5
FROM eb_dwh.fact_gl_entry_line Final AS f
WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
  AND f.posted_date >= toDate('{{FROM_DATE}}')
  AND f.posted_date <= toDate('{{TO_DATE}}')
-- type_ledger = 2 là bút toán dùng chung cho cả sổ tài chính lẫn quản trị,
-- nên luôn lấy kèm bất kể đang xem sổ nào.
  AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
  AND f.account_number IN ({{ACCOUNT_NUMBERS}})
-- Fact là ReplacingMergeTree(__version, is_deleted). Không có FINAL thì dòng
-- đã xoá VẪN xuất hiện — bản Java cũ thiếu bộ lọc này.
  AND f.is_deleted = 0
    {{AMOUNT_FILTER}}
    {{CURRENCY_FILTER}}
    {{CLUSTER_FILTER}}
    {{GROUP_BY_CLAUSE}}
    {{HAVING_CLAUSE}}
ORDER BY
    indexOf([{{ACCOUNT_ORDER}}], account_number),
    account_number,
    posted_date,
    {{ORDER_TAIL}}
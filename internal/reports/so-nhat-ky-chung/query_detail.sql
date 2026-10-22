-- CHẾ ĐỘ KHÔNG GỘP (@GroupTheSameItem = 0): mỗi dòng fact là một dòng sổ, có CẢ
-- hai bên Nợ/Có trên cùng dòng (khác chế độ gộp, nơi mỗi bút toán tách hai dòng).
-- Các biểu thức cột phân tích do buildDetailQuery điền theo @IsDefaultType: mẫu
-- mặc định để chuỗi rỗng / NULL cho cột hoá đơn, đối tượng, khoản mục, trường tuỳ chỉnh.
{{WITH_CLAUSE}}
SELECT
    f.reference_id                                   AS reference_id,
    f.type_id                                        AS type_id,
    f.posted_date                                    AS posted_date,
    f.voucher_date                                   AS voucher_date,
    ifNull(f.{{NO_COL}}, '')                         AS no,
    {{INVOICE_NO_EXPR}}                              AS invoice_no,
    {{INVOICE_SERIES_EXPR}}                          AS invoice_series,
    {{INVOICE_DATE_EXPR}}                            AS invoice_date,
    ifNull(f.description, '')                        AS description,
    ifNull(f.reason, '')                             AS reason,
    f.account_number                                 AS account_number,
    ifNull(f.account_corresponding, '')              AS account_corresponding,
    coalesce(f.exchange_rate, 0)                     AS exchange_rate,
    coalesce(f.debit_amount_original, 0)             AS debit_amount_nt,
    coalesce(f.debit_amount, 0)                      AS debit_amount,
    coalesce(f.credit_amount_original, 0)            AS credit_amount_nt,
    coalesce(f.credit_amount, 0)                     AS credit_amount,
    {{AO_CODE_EXPR}}                                 AS accounting_object_code,
    {{AO_NAME_EXPR}}                                 AS accounting_object_name,
    {{EXPENSE_CODE_EXPR}}                            AS expense_item_code,
    {{EXPENSE_NAME_EXPR}}                            AS expense_item_name,
    {{BUDGET_CODE_EXPR}}                             AS budget_item_code,
    {{BUDGET_NAME_EXPR}}                             AS budget_item_name,
    {{ORG_CODE_EXPR}}                                AS organization_unit_code,
    {{ORG_NAME_EXPR}}                                AS organization_unit_name,
    {{COST_SET_CODE_EXPR}}                           AS cost_set_code,
    {{COST_SET_NAME_EXPR}}                           AS cost_set_name,
    {{CONTRACT_EXPR}}                                AS em_contract,
    {{STATISTICS_CODE_EXPR}}                         AS statistics_code,
    {{STATISTICS_NAME_EXPR}}                         AS statistics_code_name,
    {{CF1}} AS custom_field1, {{CF2}} AS custom_field2, {{CF3}} AS custom_field3,
    {{CF4}} AS custom_field4, {{CF5}} AS custom_field5,
    {{CFD1}} AS custom_field_detail1, {{CFD2}} AS custom_field_detail2,
    {{CFD3}} AS custom_field_detail3, {{CFD4}} AS custom_field_detail4,
    {{CFD5}} AS custom_field_detail5,
    if(ifNull(f.is_unreasonable_cost, 0) = 0, 'Chi phí hợp lý', 'Chi phí không hợp lý') AS is_unreasonable_cost,
    toInt32(0)                                       AS sort_order,
    toInt32(ifNull(f.order_priority, 0))             AS order_priority
FROM {{GL_SOURCE}}
{{AO_JOIN}}
WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
  AND f.posted_date >= toDate('{{FROM_DATE}}')
  AND f.posted_date <= toDate('{{TO_DATE}}')
-- Proc chỉ lấy dòng có tài khoản đối ứng: dòng không đối ứng không phải bút toán
-- kép nên không thuộc sổ nhật ký.
  AND ifNull(f.account_corresponding, '') != ''
  AND (coalesce(f.debit_amount, 0) != 0
    OR coalesce(f.credit_amount, 0) != 0
    OR coalesce(f.debit_amount_original, 0) != 0
    OR coalesce(f.credit_amount_original, 0) != 0)
-- type_ledger = 2 là bút toán dùng chung cho cả sổ tài chính lẫn quản trị.
  AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
-- Fact là ReplacingMergeTree(__version, is_deleted): dòng đã xoá phải bị loại.
  AND f.is_deleted = 0
  {{CLUSTER_FILTER}}
{{ORDER_BY}}
{{QUERY_SETTINGS}}

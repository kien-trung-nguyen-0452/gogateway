-- CHẾ ĐỘ GỘP (@GroupTheSameItem = 1): mỗi bút toán (chứng từ, tài khoản, tài
-- khoản đối ứng) tách thành HAI dòng — dòng ghi Nợ (side = 0) và dòng ghi Có
-- (side = 1) — rồi cộng gộp từng dòng. Proc làm bằng UNION ALL hai lần quét;
-- ARRAY JOIN [0, 1] cho cùng kết quả mà chỉ quét nguồn một lần.
--
-- Mỗi dòng chỉ mang số của MỘT bên; bên kia là 0 (proc trả NULL).
--
-- Dòng Nợ lọc debit_amount != 0, dòng Có lọc credit_amount != 0 — ĐÚNG như proc,
-- KHÔNG xét số nguyên tệ: một dòng chỉ có nguyên tệ khác 0 sẽ bị loại ở nhánh gộp
-- (khác nhánh không gộp).
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
    ifNull(f.reason, '')                             AS description,
    ifNull(f.reason, '')                             AS reason,
    f.account_number                                 AS account_number,
    ifNull(f.account_corresponding, '')              AS account_corresponding,
    toDecimal64(0, 10)                               AS exchange_rate,
    if(side = 0, sum(coalesce(f.debit_amount_original, 0)), 0)  AS debit_amount_nt,
    if(side = 0, sum(coalesce(f.debit_amount, 0)), 0)           AS debit_amount,
    if(side = 1, sum(coalesce(f.credit_amount_original, 0)), 0) AS credit_amount_nt,
    if(side = 1, sum(coalesce(f.credit_amount, 0)), 0)          AS credit_amount,
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
    toInt32(side)                                    AS sort_order,
    toInt32(ifNull(max(f.order_priority), 0))        AS order_priority
FROM {{GL_SOURCE}}
ARRAY JOIN [0, 1] AS side
{{CS_JOIN}}
{{AO_JOIN}}
WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
  AND f.posted_date >= toDate('{{FROM_DATE}}')
  AND f.posted_date <= toDate('{{TO_DATE}}')
  AND ifNull(f.account_corresponding, '') != ''
  AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
  AND f.is_deleted = 0
  AND ((side = 0 AND coalesce(f.debit_amount, 0) != 0)
    OR (side = 1 AND coalesce(f.credit_amount, 0) != 0))
  {{CLUSTER_FILTER}}
GROUP BY f.reference_id, f.type_id, f.posted_date, f.voucher_date,
         f.no_fbook, f.no_mbook, f.reason, f.is_unreasonable_cost,
         f.account_number, f.account_corresponding, side
         {{EXTRA_GROUP_BY}}
{{ORDER_BY}}
{{QUERY_SETTINGS}}

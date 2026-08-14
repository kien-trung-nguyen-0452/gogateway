SELECT
    f.account_number                              AS account_number,
    sum(coalesce(f.debit_amount, 0))              AS debit_sum,
    sum(coalesce(f.credit_amount, 0))             AS credit_sum,
    sum(coalesce(f.debit_amount_original, 0))     AS debit_orig_sum,
    sum(coalesce(f.credit_amount_original, 0))    AS credit_orig_sum
FROM eb_dwh.fact_gl_entry_line Final AS f
WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
  AND f.posted_date < toDate('{{FROM_DATE}}')
  AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
  AND f.account_number IN ({{ACCOUNT_NUMBERS}})
  AND f.is_deleted = 0
  AND (coalesce(f.debit_amount, 0) != 0 OR coalesce(f.credit_amount, 0) != 0)
  {{CURRENCY_FILTER}}
  {{CLUSTER_FILTER}}
GROUP BY f.account_number
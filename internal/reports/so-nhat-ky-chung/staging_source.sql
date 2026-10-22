-- Nguồn thay thế cho eb_dwh.fact_gl_entry_line: dựng lại đúng các cột fact từ
-- eb_staging.stg_general_ledger(_detail). Bật tạm bằng SO_NHAT_KY_CHUNG_DATA_SOURCE
-- = staging khi pipeline fact bị lỗi — xem config.SoNhatKyChungUseStaging.
--
-- Cùng quy ước với so-chi-tiet-cac-tai-khoan/staging_source.sql: chỉ lộ ID của
-- các chiều (đối tượng, khoản mục, ...); mã/tên do query tra bằng dictionary/CTE
-- nên dùng chung cho cả hai nguồn. Cặp (company_id, ngày) được lọc NGAY trong
-- truy vấn con, trước JOIN, để cắt phân vùng thay vì quét cả lịch sử công ty.
(
    SELECT
        toString(gld.ID)         AS line_bk,
        gl.ReferenceID            AS reference_id,
        gl.TypeID                 AS type_id,
        gl.TypeLedger             AS type_ledger,
        toDate(gl.PostedDate)     AS posted_date,
        toDate(gl.Date)           AS voucher_date,
        gl.NoFBook                AS no_fbook,
        gl.NoMBook                AS no_mbook,
        gl.InvoiceNo              AS invoice_no,
        gl.InvoiceSeries          AS invoice_series,
        toDate(gl.InvoiceDate)    AS invoice_date,
        gl.Reason                 AS reason,
        gld.Description           AS description,
        gld.Account               AS account_number,
        gld.AccountCorresponding  AS account_corresponding,
        gld.DebitAmount           AS debit_amount,
        gld.CreditAmount          AS credit_amount,
        gld.DebitAmountOriginal   AS debit_amount_original,
        gld.CreditAmountOriginal  AS credit_amount_original,
        gl.ExchangeRate           AS exchange_rate,
        gld.OrderPriority         AS order_priority,
        gl.IsUnreasonableCost     AS is_unreasonable_cost,
        gld.AccountingObjectID    AS accounting_object_id,
        gl.DepartmentID           AS department_id,
        gl.ExpenseItemID          AS expense_item_id,
        gl.BudgetItemID           AS budget_item_id,
        gl.CostSetID              AS cost_set_id,
        gl.ContractID             AS contract_id,
        gl.StatisticsCodeID       AS statistics_code_id,
        gl.CustomField1 AS custom_field1, gl.CustomField2 AS custom_field2,
        gl.CustomField3 AS custom_field3, gl.CustomField4 AS custom_field4,
        gl.CustomField5 AS custom_field5,
        gl.CustomFieldDetail1 AS custom_field_detail1,
        gl.CustomFieldDetail2 AS custom_field_detail2,
        gl.CustomFieldDetail3 AS custom_field_detail3,
        gl.CustomFieldDetail4 AS custom_field_detail4,
        gl.CustomFieldDetail5 AS custom_field_detail5,
        gl.CompanyID   AS company_id,
        gl.CurrencyID  AS currency_code,
        gl.cluster_id  AS cluster_id,
        0              AS is_deleted
    FROM eb_staging.stg_general_ledger AS gl FINAL
    INNER JOIN eb_staging.stg_general_ledger_detail AS gld FINAL
        ON gl.DetailID = gld.DetailID AND gl.ReferenceID = gld.ReferenceID
    WHERE gl.__deleted = 0 AND gld.__deleted = 0
      AND gl.CompanyID IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
      AND gld.CompanyID IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
      {{STAGING_DATE_BOUND}}
) AS f

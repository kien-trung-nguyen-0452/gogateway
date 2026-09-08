-- Nguồn thay thế cho eb_dwh.fact_gl_entry_line khi pipeline fact bị lỗi tạm
-- thời (bật bằng SO_CHI_TIET_TAI_KHOAN_DATA_SOURCE=staging, xem config.go).
--
-- fact_gl_entry_line vốn được DWH denormalize sẵn từ hai bảng header/detail.
-- Subquery này JOIN lại đúng cặp khóa mà Proc_SO_CHI_TIET_CAC_TAI_KHOAN gốc
-- dùng để nối GeneralLedger (GL) với GeneralLedgerDetail (GLD):
--
--	GL.DetailID = GLD.DetailID AND GL.ReferenceID = GLD.ReferenceID
--
-- rồi đặt lại tên/kiểu cột cho khớp CHÍNH XÁC với fact_gl_entry_line, để mọi
-- placeholder trong query.sql/query_opening.sql (và toàn bộ logic build ở
-- service.go) dùng chung nguyên vẹn, không cần biết đang đọc fact hay staging.
--
-- FINAL trên cả hai bảng vì đều là ReplacingMergeTree(__source_ts_ms).
-- __deleted thay cho is_deleted (fact); is_deleted synth = 0 vì dòng đã bị
-- loại bởi WHERE bên dưới nếu __deleted != 0, giữ nguyên downstream
-- "AND f.is_deleted = 0" ở query.sql/query_opening.sql mà không phải sửa gì.
--
-- CHƯA đẩy filter company_id/posted_date/account_number vào bên trong subquery
-- này — nhờ ClickHouse pushdown predicate xuống derived table. Nếu staging
-- chậm bất thường, cân nhắc nhân bản filter vào WHERE bên dưới.
(
    SELECT
        toString(gld.ID)         AS line_bk,
        gl.ReferenceID            AS reference_id,
        gl.TypeID                 AS type_id,
        gl.TypeLedger             AS type_ledger,
        toDate(gl.PostedDate)     AS posted_date,
        gl.NoFBook                AS no_fbook,
        gl.NoMBook                AS no_mbook,
        gl.InvoiceNo              AS invoice_no,
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
        gld.AccountingObjectCode  AS accounting_object_code,
        gld.AccountingObjectName AS accounting_object_name,
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
) AS f

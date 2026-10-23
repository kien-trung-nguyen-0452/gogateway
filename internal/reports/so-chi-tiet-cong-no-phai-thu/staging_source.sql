-- Nguồn thay thế cho eb_dwh.fact_gl_entry_line khi pipeline fact bị lỗi tạm thời
-- (bật bằng SO_CHI_TIET_CONG_NO_PHAI_THU_DATA_SOURCE=staging, xem config.go).
--
-- JOIN lại đúng cặp khoá mà proc gốc dùng để nối GeneralLedger (gl) với
-- GeneralLedgerDetail (gld):  gl.DetailID = gld.DetailID AND gl.ReferenceID = gld.ReferenceID
-- rồi đặt lại tên/kiểu cột cho khớp CHÍNH XÁC với fact_gl_entry_line, để query*.sql
-- dùng chung nguyên vẹn cho cả hai nguồn. Chỉ lấy các cột sổ này cần.
--
-- FINAL trên cả hai bảng vì đều là ReplacingMergeTree(__source_ts_ms).
-- is_deleted synth = 0: dòng đã xoá bị loại bởi WHERE (__deleted = 0), giữ nguyên
-- "AND f.is_deleted = 0" ở query*.sql.
--
-- gld.CompanyID + gld.Account lọc CÙNG vế gld TRƯỚC khi JOIN: mã tài khoản như
-- '131' dùng chung bởi hàng chục công ty, lọc Account một mình khớp ~73% bảng và
-- buộc ClickHouse dựng hash table JOIN trên hàng chục triệu dòng (số đo ở
-- so-chi-tiet-cac-tai-khoan/staging_source.sql).
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
        gld.AccountingObjectID    AS accounting_object_id,
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
      AND gld.Account LIKE '{{ACCOUNT_NUMBER}}%'
      {{STAGING_DATE_BOUND}}
) AS f

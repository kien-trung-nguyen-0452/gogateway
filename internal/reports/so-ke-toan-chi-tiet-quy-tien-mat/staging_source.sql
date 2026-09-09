-- Nguồn thay thế cho eb_dwh.fact_gl_entry_line khi pipeline fact bị lỗi tạm
-- thời (bật bằng SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE=staging, xem
-- config.go).
--
-- fact_gl_entry_line vốn được DWH denormalize sẵn từ hai bảng header/detail.
-- Subquery này JOIN lại đúng cặp khóa mà Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT
-- gốc dùng để nối GeneralLedger (GL) với GeneralLedgerDetail (GLD):
--
--	GL.DetailID = GLD.DetailID AND GL.ReferenceID = GLD.ReferenceID
--
-- rồi đặt lại tên/kiểu cột cho khớp CHÍNH XÁC với fact_gl_entry_line, để mọi
-- placeholder trong query.sql/query_opening.sql (và toàn bộ logic build ở
-- service.go) dùng chung nguyên vẹn, không cần biết đang đọc fact hay staging.
--
-- Cùng một derived table với internal/reports/so-chi-tiet-cac-tai-khoan —
-- xem ghi chú đầy đủ (đo đạc, quyết định đẩy filter) ở bản đó. Sao chép thay
-- vì dùng chung để hai báo cáo không phụ thuộc chéo lẫn nhau.
--
-- FINAL trên cả hai bảng vì đều là ReplacingMergeTree(__source_ts_ms).
-- __deleted thay cho is_deleted (fact); is_deleted synth = 0 vì dòng đã bị
-- loại bởi WHERE bên dưới nếu __deleted != 0, giữ nguyên downstream
-- "AND f.is_deleted = 0" ở query.sql/query_opening.sql mà không phải sửa gì.
--
-- Đo thực tế trên ClickHouse (company thật ~600k dòng header, 12 tài khoản,
-- cửa sổ ~2 tháng, 2026-09-09): JOIN chỉ lọc company_id ở vế `gl` và
-- account_number ở vế `gld` (KHÔNG có company_id) → ổn định ~4.2-5.0s, KHÔNG
-- đổi dù bật/tắt FINAL, dù tách filter ra subquery riêng trước khi JOIN. Gốc
-- rễ: mã tài khoản kiểu '131'/'632' dùng CHUNG bởi hàng chục công ty khác
-- (đo được 51 công ty cùng dùng 12 mã trong lần test) → lọc account_number
-- một mình ở gld khớp ~73% toàn bảng (10.6tr/14.5tr dòng), ClickHouse phải
-- dựng hash table JOIN trên ngần ấy dòng bất kể gl đã lọc company_id.
--
-- FIX THẬT: gld CŨNG có sẵn cột CompanyID (denormalize song song với gl —
-- đã đối chiếu, dữ liệu khớp đúng cặp DetailID/ReferenceID). Thêm
-- gld.CompanyID vào WHERE ngay cạnh gld.Account (xem bên dưới) → còn
-- ~0.8-1.7s (nhanh hơn 4-5 lần so với chỉ lọc company_id/account_number như
-- cũ). Kết quả trả về giống hệt (đã đối chiếu count()), chỉ nhanh hơn — vì
-- giờ company_id + account_number được lọc CÙNG một vế (gld) TRƯỚC khi JOIN,
-- nên chọn lọc đủ mạnh để tránh dựng hash table trên 10tr+ dòng dư thừa.
--
-- ĐÃ THỬ và KHÔNG giúp thêm (một khi đã có fix trên): đẩy cận dưới ngày
-- (FromDate) vào đây tương tự cận trên (ToDate) — thời gian đo không đổi
-- (dao động trong sai số, có lúc còn chậm hơn một chút vì thêm một phép so
-- sánh mỗi dòng). Bottleneck đã được giải quyết ở filter company_id/
-- account_number phía trên nên KHÔNG đẩy cận dưới vào đây nữa — giữ WHERE
-- ngoài (query.sql/query_opening.sql) làm việc đó như bản gốc.
--
-- OPTIMIZE TABLE ... FINAL trên cả hai bảng nguồn cũng ĐÃ THỬ — không giúp
-- gì (partition key theo (tháng, hash(company_id)) tạo ~980 partition, mỗi
-- partition vốn đã chỉ 1 part, không còn gì để gộp). Kết luận: fix thật nằm
-- ở filter phía trên, không phải dọn part.
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
      AND gl.CompanyID IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
      -- gld.CompanyID: cột chọn lọc quan trọng nhất trong subquery này — xem
      -- ghi chú "FIX THẬT" ở trên. Thiếu dòng này thì gld.Account một mình
      -- không đủ chọn lọc (mã tài khoản dùng chung nhiều công ty).
      AND gld.CompanyID IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
      AND gld.Account IN ({{ACCOUNT_NUMBERS}})
      AND toDate(gl.PostedDate) <= toDate('{{TO_DATE}}')
      AND (gl.TypeLedger = {{TYPE_LEDGER}} OR gl.TypeLedger = 2)
) AS f

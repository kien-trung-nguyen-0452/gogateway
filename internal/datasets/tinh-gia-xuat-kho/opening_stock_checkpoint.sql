/*
Nhánh CHECKPOINT của tồn đầu kỳ — bật bằng biến môi trường
TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE=checkpoint, xem
config.TinhGiaXuatKhoOpeningStockUseCheckpoint.

Trả về HAI dòng IsOpeningStock = true cho mỗi cặp kho - vật tư: luỹ kế đọc thẳng từ bảng
checkpoint, và phần lẻ tính lại như nhánh raw. Bên tiêu thụ cộng dồn mọi dòng tồn đầu kỳ nên
không cần gộp trước, xem CostingServiceImpl.calculateByGroup.

Vì sao nhánh raw vẫn phải giữ làm mặc định: eb.repository_ledger và
eb.repository_ledger_checkpoint được ghi bằng hai thao tác tách rời, không cùng đúng cùng sai.
Checkpoint lệch thì tồn đầu kỳ sai âm thầm, không có lỗi nào báo ra. Lúc đó xoá biến môi
trường rồi khởi động lại là quay về tính toàn bộ, không phải build lại.
*/
    -- Phần luỹ kế: tới hết tháng liền trước tháng FROM_DATE. Tháng khuyết là tháng không phát
    -- sinh, nên dòng checkpoint mới nhất còn trước tháng FROM_DATE đã bao trọn tới hết tháng đó.
    -- Mỗi sổ một dòng vì type_ledger 0, 1 và 2 tách nhau, bên tiêu thụ tự cộng lại.
    -- Lọc theo đúng tập kho - vật tư của ledger_scope, thiếu sẽ trả về mọi kho và vật tư của công ty.
    SELECT
        true                                                    AS IsOpeningStock,
        ckp.material_goods_id                                   AS MaterialGoodsID,
        ckp.repository_id                                       AS RepositoryID,
        NULL                                                    AS DetailID,
        NULL                                                    AS ReferenceID,
        NULL                                                    AS TypeID,
        NULL                                                    AS PostedDate,
        CAST(argMax(ckp.cumulative_qty, ckp.month)
             AS Decimal128(10))                                 AS MainIWQuantity,
        CAST(0 AS Decimal128(10))                               AS MainOWQuantity,
        CAST(argMax(ckp.cumulative_amount, ckp.month)
             AS Decimal128(10))                                 AS IWAmount,
        CAST(0 AS Decimal128(10))                               AS OWAmount
    FROM eb.repository_ledger_checkpoint AS ckp FINAL
    WHERE ckp.company_id = toUUID('{{COMPANY_ID}}')
      AND (ckp.type_ledger = {{TYPE_LEDGER}} OR ckp.type_ledger = 2)
      AND ckp.month < toYYYYMM(toDateTime('{{FROM_DATE}}'))
      AND (ckp.repository_id, ckp.material_goods_id) IN
          (SELECT RepositoryID, MaterialGoodsID FROM ledger_scope)
    GROUP BY ckp.repository_id, ckp.material_goods_id, ckp.type_ledger

    UNION ALL

    -- Phần lẻ: từ đầu tháng FROM_DATE đến FROM_DATE, phần mà checkpoint chưa phủ.
    -- Cận dưới là hằng số nên ClickHouse bỏ qua được phần dữ liệu cũ hơn.
    SELECT
        true                                                    AS IsOpeningStock,
        MaterialGoodsID                                         AS MaterialGoodsID,
        RepositoryID                                            AS RepositoryID,
        NULL                                                    AS DetailID,
        NULL                                                    AS ReferenceID,
        NULL                                                    AS TypeID,
        NULL                                                    AS PostedDate,
        CAST(sum(MainIWQty - MainOWQty) AS Decimal128(10))      AS MainIWQuantity,
        CAST(0                          AS Decimal128(10))      AS MainOWQuantity,
        CAST(sum(IWAmt - OWAmt)         AS Decimal128(10))      AS IWAmount,
        CAST(0                          AS Decimal128(10))      AS OWAmount
    FROM ledger_scope
    WHERE ledger_scope.PostedDate >= toDateTime(toStartOfMonth(toDateTime('{{FROM_DATE}}')))
      AND ledger_scope.PostedDate <  toDateTime('{{FROM_DATE}}')
    GROUP BY MaterialGoodsID, RepositoryID
    HAVING sum(MainIWQty - MainOWQty) <> 0
        OR sum(IWAmt - OWAmt) <> 0

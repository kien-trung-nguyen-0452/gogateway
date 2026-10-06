/*
19/08/2026: truongtd: Query lấy dữ liệu thô để Tính giá xuất kho - Bình quân cuối kỳ
- COMPANY_ID             Lọc cột CompanyID
- TYPE_LEDGER            Lọc cột TypeLedger, sổ đang làm việc
- FROM_DATE              Mốc chia tồn đầu kỳ và phát sinh, so với PostedDate
- TO_DATE                Giới hạn cuối kỳ, so với PostedDate
- REPOSITORY_FILTER      Lọc cột RepositoryID, rỗng nghĩa là lấy tất cả
- MATERIAL_GOODS_FILTER  Lọc cột MaterialGoodsID, rỗng nghĩa là lấy tất cả
*/
WITH ledger_scope AS
(
    SELECT
        MaterialGoodsID                                         AS MaterialGoodsID,
        RepositoryID                                            AS RepositoryID,
        DetailID                                                AS DetailID,
        ReferenceID                                             AS ReferenceID,
        toInt32(TypeID)                                         AS TypeID,
        PostedDate                                              AS PostedDate,
        if(ifNull(UnitID = MainUnitID, 0),
           ifNull(IWQuantity, 0), ifNull(MainIWQuantity, 0))    AS MainIWQty,
        if(ifNull(UnitID = MainUnitID, 0),
           ifNull(OWQuantity, 0), ifNull(MainOWQuantity, 0))    AS MainOWQty,
        ifNull(IWAmount, 0)                                     AS IWAmt,
        ifNull(OWAmount, 0)                                     AS OWAmt
    FROM eb.repository_ledger FINAL
    WHERE CompanyID  = toUUID('{{COMPANY_ID}}')
      AND __deleted  = 0
      AND PostedDate <= toDateTime('{{TO_DATE}}')
      AND (TypeLedger = {{TYPE_LEDGER}} OR TypeLedger = 2)
      {{REPOSITORY_FILTER}}
      {{MATERIAL_GOODS_FILTER}}
)
SELECT *
FROM
(
    -- IsOpeningStock = true (1/2): Luỹ kế tồn tới hết tháng liền trước tháng FROM_DATE, đọc thẳng
    -- từ bảng checkpoint thay vì cộng dồn lại toàn bộ lịch sử.
    SELECT
        true                                                                AS IsOpeningStock,
        rlc.material_goods_id                                               AS MaterialGoodsID,
        rlc.repository_id                                                   AS RepositoryID,
        NULL                                                                AS DetailID,
        NULL                                                                AS ReferenceID,
        NULL                                                                AS TypeID,
        NULL                                                                AS PostedDate,
        CAST(argMax(rlc.cumulative_qty, rlc.month) AS Decimal128(10))       AS MainIWQuantity,
        CAST(0 AS Decimal128(10))                                           AS MainOWQuantity,
        CAST(argMax(rlc.cumulative_amount, rlc.month) AS Decimal128(10))    AS IWAmount,
        CAST(0 AS Decimal128(10))                                           AS OWAmount
    FROM eb.repository_ledger_checkpoint AS rlc FINAL
    WHERE rlc.company_id = toUUID('{{COMPANY_ID}}')
      AND (rlc.type_ledger = {{TYPE_LEDGER}} OR rlc.type_ledger = 2)
      AND rlc.month < toYYYYMM(toDateTime('{{FROM_DATE}}'))
      AND (rlc.repository_id, rlc.material_goods_id) IN
          (SELECT RepositoryID, MaterialGoodsID FROM ledger_scope)
    GROUP BY rlc.repository_id, rlc.material_goods_id, rlc.type_ledger

    UNION ALL

    -- IsOpeningStock = true (2/2): Phần lẻ từ đầu tháng FROM_DATE đến FROM_DATE, phần mà checkpoint
    -- chưa phủ. Cận dưới là hằng số nên ClickHouse bỏ qua được phần dữ liệu cũ hơn.
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

    UNION ALL

    -- IsOpeningStock = false: Phát sinh trong kỳ, lấy hết dữ liệu sau FROM_DATE
    SELECT
        false                                                 AS IsOpeningStock,
        MaterialGoodsID                                       AS MaterialGoodsID,
        RepositoryID                                          AS RepositoryID,
        DetailID                                              AS DetailID,
        ReferenceID                                           AS ReferenceID,
        TypeID                                                AS TypeID,
        PostedDate                                            AS PostedDate,
        CAST(MainIWQty AS Decimal128(10))                     AS MainIWQuantity,
        CAST(MainOWQty AS Decimal128(10))                     AS MainOWQuantity,
        CAST(IWAmt     AS Decimal128(10))                     AS IWAmount,
        CAST(OWAmt     AS Decimal128(10))                     AS OWAmount
    FROM ledger_scope
    WHERE ledger_scope.PostedDate >= toDateTime('{{FROM_DATE}}')
)
-- Gom theo (MaterialGoodsID, RepositoryID) để xử lý từng nhóm
-- IsOpeningStock DESC để dòng tồn đầu kỳ (true) đứng trước dòng phát sinh (false)
ORDER BY MaterialGoodsID, RepositoryID, IsOpeningStock DESC, PostedDate

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
        MaterialGoodsID                                                     AS MaterialGoodsID,
        RepositoryID                                                        AS RepositoryID,
        ifNull(DetailID, toUUID('00000000-0000-0000-0000-000000000000'))    AS DetailID,
        ReferenceID                                                         AS ReferenceID,
        toInt32(ifNull(TypeID, 0))                                          AS TypeID,
        PostedDate                                                          AS PostedDate,
        if(UnitID = ifNull(MainUnitID, toUUID('00000000-0000-0000-0000-000000000000')),
           ifNull(IWQuantity, 0), ifNull(MainIWQuantity, 0))                AS MainIWQty,
        if(UnitID = ifNull(MainUnitID, toUUID('00000000-0000-0000-0000-000000000000')),
           ifNull(OWQuantity, 0), ifNull(MainOWQuantity, 0))                AS MainOWQty,
        ifNull(IWAmount, 0)                                                 AS IWAmt,
        ifNull(OWAmount, 0)                                                 AS OWAmt
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
    -- RowKind = 0: Tồn đầu kỳ, lấy hết dữ liệu trước FROM_DATE
    SELECT
        toUInt8(0)                                              AS RowKind,
        MaterialGoodsID                                         AS MaterialGoodsID,
        RepositoryID                                            AS RepositoryID,
        toUUID('00000000-0000-0000-0000-000000000000')          AS DetailID,
        toUUID('00000000-0000-0000-0000-000000000000')          AS ReferenceID,
        toInt32(0)                                              AS TypeID,
        toDateTime('1970-01-01 00:00:00')                       AS PostedDate,
        CAST(sum(MainIWQty - MainOWQty) AS Decimal128(10))      AS MainIWQuantity,
        CAST(0                          AS Decimal128(10))      AS MainOWQuantity,
        CAST(sum(IWAmt - OWAmt)         AS Decimal128(10))      AS IWAmount,
        CAST(0                          AS Decimal128(10))      AS OWAmount
    FROM ledger_scope
    WHERE PostedDate < toDateTime('{{FROM_DATE}}')
    GROUP BY MaterialGoodsID, RepositoryID
    HAVING sum(MainIWQty - MainOWQty) <> 0
        OR sum(IWAmt - OWAmt) <> 0

    UNION ALL

    -- RowKind = 1: Phát sinh trong kỳ, lấy hết dữ liệu sau FROM_DATE
    SELECT
        toUInt8(1)                                            AS RowKind,
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
    WHERE PostedDate >= toDateTime('{{FROM_DATE}}')
)
-- Gom theo (MaterialGoodsID, RepositoryID) để xử lý từng nhóm
ORDER BY MaterialGoodsID, RepositoryID, RowKind, PostedDate

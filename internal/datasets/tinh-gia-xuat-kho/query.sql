/*
19/08/2026: truongtd: Query lấy dữ liệu thô để Tính giá xuất kho - Bình quân cuối kỳ
- COMPANY_ID             Lọc cột CompanyID
- TYPE_LEDGER            Lọc cột TypeLedger, sổ đang làm việc
- FROM_DATE              Mốc chia tồn đầu kỳ và phát sinh, so với PostedDate
- TO_DATE                Giới hạn cuối kỳ, so với PostedDate
- REPOSITORY_FILTER      Lọc cột RepositoryID, rỗng nghĩa là lấy tất cả
- MATERIAL_GOODS_FILTER  Lọc cột MaterialGoodsID, rỗng nghĩa là lấy tất cả
- OPENING_STOCK_BODY     Thân nhánh tồn đầu kỳ, raw hoặc checkpoint, do service.go chọn
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
    -- IsOpeningStock = true: Tồn đầu kỳ. Thân cụ thể do service.go ghép vào, mặc định là
    -- opening_stock_raw.sql, đặt TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE=checkpoint thì
    -- đổi sang opening_stock_checkpoint.sql
{{OPENING_STOCK_BODY}}

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

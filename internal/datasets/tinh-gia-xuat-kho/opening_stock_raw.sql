/*
Nhánh RAW của tồn đầu kỳ — mặc định, dùng khi biến môi trường
TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE không phải "checkpoint".

Cộng dồn toàn bộ lịch sử PostedDate < FROM_DATE từ eb.repository_ledger. Chậm hơn nhánh
checkpoint nhưng chỉ phụ thuộc đúng một nguồn dữ liệu, nên luôn khớp với sổ kho.
*/
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
    WHERE ledger_scope.PostedDate < toDateTime('{{FROM_DATE}}')
    GROUP BY MaterialGoodsID, RepositoryID
    HAVING sum(MainIWQty - MainOWQty) <> 0
        OR sum(IWAmt - OWAmt) <> 0

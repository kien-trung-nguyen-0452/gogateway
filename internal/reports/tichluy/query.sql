WITH
    m_agg AS
        (
            SELECT
                RepositoryID,
                MaterialGoodsID,
                multiIf(PostedDate < toDate(makeDate({{YEAR}}, 1, 1)), 0, toMonth(PostedDate)) AS m,
                TypeLedger,
                sum(if(UnitID = MainUnitID, ifNull(IWQuantity, 0), ifNull(MainIWQuantity, 0))) AS IWQ,
                sum(if(UnitID = MainUnitID, ifNull(OWQuantity, 0), ifNull(MainOWQuantity, 0))) AS OWQ,
                sum(ifNull(IWAmount, 0)) AS IWA,
                sum(ifNull(OWAmount, 0)) AS OWA
            FROM eb.repository_ledger final
            WHERE CompanyID IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
    AND PostedDate < toDateTime(makeDate({{YEAR}} + 1, 1, 1))
GROUP BY RepositoryID, MaterialGoodsID, m, TypeLedger
    ),
    b AS
    (
SELECT
    arrayJoin(multiIf(TypeLedger = 0, [0], TypeLedger = 1, [1], TypeLedger = 2, [0, 1], emptyArrayInt8())) AS Bucket,
    RepositoryID,
    MaterialGoodsID,
    m,
    IWQ - OWQ AS NetQ,
    IWA - OWA AS NetA
FROM m_agg
    ),
    per_group AS
    (
SELECT
    Bucket,
    RepositoryID,
    MaterialGoodsID,
    arrayCumSum([
    sumIf(NetQ, m = 0), sumIf(NetQ, m = 1), sumIf(NetQ, m = 2), sumIf(NetQ, m = 3),
    sumIf(NetQ, m = 4), sumIf(NetQ, m = 5), sumIf(NetQ, m = 6), sumIf(NetQ, m = 7),
    sumIf(NetQ, m = 8), sumIf(NetQ, m = 9), sumIf(NetQ, m = 10), sumIf(NetQ, m = 11),
    sumIf(NetQ, m = 12)
    ]) AS CumQArr,
    arrayCumSum([
    sumIf(NetA, m = 0), sumIf(NetA, m = 1), sumIf(NetA, m = 2), sumIf(NetA, m = 3),
    sumIf(NetA, m = 4), sumIf(NetA, m = 5), sumIf(NetA, m = 6), sumIf(NetA, m = 7),
    sumIf(NetA, m = 8), sumIf(NetA, m = 9), sumIf(NetA, m = 10), sumIf(NetA, m = 11),
    sumIf(NetA, m = 12)
    ]) AS CumAArr
FROM b
GROUP BY Bucket, RepositoryID, MaterialGoodsID
    ),
    mcum AS
    (
SELECT
    Bucket,
    RepositoryID,
    MaterialGoodsID,
    idx - 1 AS m,
    CumQArr[idx] AS CumQ,
    CumAArr[idx] AS CumA
FROM per_group
    ARRAY JOIN range(1, 14) AS idx
    ),
    periods AS
    (
SELECT
    toDate(makeDate({{YEAR}}, mo, 1)) AS DateFrom,
    toDate(makeDate({{YEAR}}, mo, 1) + INTERVAL 1 MONTH - INTERVAL 1 DAY) AS DateTo,
    1 AS Type,
    mo AS seq
FROM (SELECT arrayJoin(range(1, 13)) AS mo)

UNION ALL

SELECT
    toDate(makeDate({{YEAR}}, (q - 1) * 3 + 1, 1)) AS DateFrom,
    toDate(makeDate({{YEAR}}, (q - 1) * 3 + 1, 1) + INTERVAL 3 MONTH - INTERVAL 1 DAY) AS DateTo,
    2 AS Type,
    q AS seq
FROM (SELECT arrayJoin(range(1, 5)) AS q)

UNION ALL

SELECT
    toDate(makeDate({{YEAR}}, 1, 1)) AS DateFrom,
    toDate(makeDate({{YEAR}}, 12, 31)) AS DateTo,
    3 AS Type,
    1 AS seq
    )

SELECT
    c.RepositoryID AS RepositoryID,
    c.MaterialGoodsID AS MaterialGoodsID,
    c.CumQ AS CumulativeQuantity,
    c.CumA AS CumulativeAmount,
    CAST(p.Type AS Int32) AS Type,
    p.DateTo AS ToDate,
    p.DateFrom AS FromDate,
    CAST(c.Bucket AS Int32) AS TypeLedger
FROM periods AS p
         INNER JOIN mcum AS c
                    ON c.m = multiIf(p.Type = 1, p.seq, p.Type = 2, p.seq * 3, 12)
ORDER BY c.Bucket, p.Type, c.RepositoryID, c.MaterialGoodsID, p.seq
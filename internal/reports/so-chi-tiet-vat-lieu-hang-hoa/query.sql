WITH
    ledger_scope AS
        (
            SELECT
                ID, ReferenceID, DetailID, TypeID, TypeLedger, CompanyID,
                RepositoryID, MaterialGoodsID, UnitID, UnitPrice,
                IWQuantity, OWQuantity, IWAmount, OWAmount,
                MainUnitID, MainUnitPrice, MainIWQuantity, MainOWQuantity, MainConvertRate,
                Reason, PostedDate, Date, NoFBook, NoMBook, AccountCorresponding,
    OrderPriority,
    BudgetItemID, CostSetID, StatisticsCodeID, ExpenseItemID,
    ContractID, DepartmentID, AccountingObjectID
FROM eb.repository_ledger final
WHERE PostedDate <= toDateTime('{{TO_DATE}}')
  AND CompanyID IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
    {{REPOSITORY_FILTER}}
    {{MATERIAL_GOODS_FILTER}}
  AND (TypeLedger = 0 OR TypeLedger = 2)
  AND __deleted = 0
  AND (
    TypeID NOT IN (420, 421, 422)
   OR {{IS_COMPANY_BUSINESS_TYPE_GAS}} <> 1
   OR (
    TypeID IN (420, 421, 422)
  AND {{IS_COMPANY_BUSINESS_TYPE_GAS}} = 1
  AND (
    (has(CAST([{{OWN_REPOSITORY_IDS}}] AS Array(UUID)), RepositoryID)
  AND CompanyID = '{{PRIMARY_COMPANY_ID}}')
   OR (has(CAST([{{OTHER_REPOSITORY_IDS}}] AS Array(UUID)), RepositoryID)
  AND CompanyID != '{{PRIMARY_COMPANY_ID}}')
    )
    )
    )
    ),

    opening_balance AS
    (
SELECT
    RepositoryID, MaterialGoodsID,
    sum(if(UnitID = MainUnitID, ifNull(IWQuantity, 0), ifNull(MainIWQuantity, 0))
    - if(UnitID = MainUnitID, ifNull(OWQuantity, 0), ifNull(MainOWQuantity, 0))) AS NetQuantity,
    sum(ifNull(IWAmount, 0) - ifNull(OWAmount, 0)) AS NetAmount,
    max(Date) AS MaxPreDate
FROM ledger_scope
WHERE PostedDate < toDateTime('{{FROM_DATE}}')
GROUP BY RepositoryID, MaterialGoodsID
    ),

    combined_rows AS
    (
SELECT
    RepositoryID, MaterialGoodsID, 0 AS is_detail,
    toUUID('00000000-0000-0000-0000-000000000000') AS ReferenceID,
    toUUID('00000000-0000-0000-0000-000000000000') AS DetailID,
    0 AS TypeID,
    toDateTime('1970-01-01 00:00:00') AS PostedDate,
    CAST(NULL AS Nullable(DateTime)) AS RefDate,
    MaxPreDate AS InRefOrder,
    '' AS RefNo, 'Số dư đầu kỳ' AS Reason, '' AS AccountCorresponding,
    toUUID('00000000-0000-0000-0000-000000000000') AS EffectiveUnitID,
    toDecimal64(1, 10) AS ConvertRate,
    NetQuantity AS MainQuantity,
    toDecimal64(0, 10) AS MainUnitPrice,
    toDecimal64(0, 10) AS UnitPrice,
    toDecimal64(0, 10) AS InwardQuantity, toDecimal64(0, 10) AS InwardAmount,
    toDecimal64(0, 10) AS OutwardQuantity, toDecimal64(0, 10) AS OutwardAmount,
    0 AS OrderPriority,
    toUUID('00000000-0000-0000-0000-000000000000') AS AccountingObjectID,
    toUUID('00000000-0000-0000-0000-000000000000') AS ExpenseItemID,
    toUUID('00000000-0000-0000-0000-000000000000') AS BudgetItemID,
    toUUID('00000000-0000-0000-0000-000000000000') AS CostSetID,
    toUUID('00000000-0000-0000-0000-000000000000') AS EMContractID,
    toUUID('00000000-0000-0000-0000-000000000000') AS StatisticsCodeID,
    toUUID('00000000-0000-0000-0000-000000000000') AS DepartmentID,
    NetQuantity AS NetQuantity, NetAmount AS NetAmount
FROM opening_balance

UNION ALL

SELECT
    RepositoryID, MaterialGoodsID, 1 AS is_detail,
    ReferenceID, DetailID, TypeID, PostedDate,
    CAST(Date AS Nullable(DateTime)) AS RefDate,
    Date AS InRefOrder,
    NoFBook AS RefNo, Reason, AccountCorresponding,
    MainUnitID AS EffectiveUnitID, MainConvertRate AS ConvertRate,
    multiIf(MainIWQuantity IS NOT NULL, MainIWQuantity, MainOWQuantity) AS MainQuantity,
    MainUnitPrice,
    multiIf(UnitID = MainUnitID, UnitPrice, MainUnitPrice) AS UnitPrice,
    if(UnitID = MainUnitID, ifNull(IWQuantity, 0), ifNull(MainIWQuantity, 0)) AS InwardQuantity,
    ifNull(IWAmount, 0) AS InwardAmount,
    if(UnitID = MainUnitID, ifNull(OWQuantity, 0), ifNull(MainOWQuantity, 0)) AS OutwardQuantity,
    ifNull(OWAmount, 0) AS OutwardAmount,
    OrderPriority,
    AccountingObjectID, ExpenseItemID, BudgetItemID, CostSetID,
    ContractID AS EMContractID, StatisticsCodeID, DepartmentID,
    (if(UnitID = MainUnitID, ifNull(IWQuantity, 0), ifNull(MainIWQuantity, 0))
    - if(UnitID = MainUnitID, ifNull(OWQuantity, 0), ifNull(MainOWQuantity, 0))) AS NetQuantity,
    (ifNull(IWAmount, 0) - ifNull(OWAmount, 0)) AS NetAmount
FROM ledger_scope
WHERE PostedDate >= toDateTime('{{FROM_DATE}}')
    ),

    running AS
    (
SELECT
    *,
    sum(NetQuantity) OVER (
    PARTITION BY RepositoryID, MaterialGoodsID
    ORDER BY RefDate ASC NULLS FIRST, InRefOrder,
    replaceRegexpAll(RefNo, '[^a-zA-Z0-9]', ''),
    RefNo, OrderPriority, ReferenceID, DetailID
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ClosingQuantity,
    sum(NetAmount) OVER (
    PARTITION BY RepositoryID, MaterialGoodsID
    ORDER BY RefDate ASC NULLS FIRST, InRefOrder,
    replaceRegexpAll(RefNo, '[^a-zA-Z0-9]', ''),
    RefNo, OrderPriority, ReferenceID, DetailID
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ClosingAmount
FROM combined_rows
    )

SELECT
    is_detail,
    RepositoryID,
    dictGetOrDefault('eb.dict_repository', 'repository_code', RepositoryID, '') AS RepositoryCode,
    dictGetOrDefault('eb.dict_repository', 'repository_name', RepositoryID, '') AS RepositoryName,
    MaterialGoodsID,
    dictGetOrDefault('eb.dict_material_goods', 'material_goods_code', MaterialGoodsID, '') AS MaterialGoodsCode,
    dictGetOrDefault('eb.dict_material_goods', 'material_goods_name', MaterialGoodsID, '') AS MaterialGoodsName,
    ReferenceID, DetailID, TypeID,
    PostedDate, RefDate, RefNo, Reason, AccountCorresponding,

    dictGetOrDefault('eb.dict_eb_organization_unit', 'currency_id', '{{PRIMARY_COMPANY_ID}}', 'VND') AS CurrencyID,

    EffectiveUnitID AS UnitID,
    dictGetOrDefault('eb.dict_unit', 'unit_name', EffectiveUnitID, '') AS UnitName,

    ConvertRate, MainQuantity, MainUnitPrice, UnitPrice,
    InwardQuantity, InwardAmount, OutwardQuantity, OutwardAmount,
    ClosingQuantity, ClosingAmount,

    AccountingObjectID,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_accounting_object', 'accounting_object_code', AccountingObjectID, '')) AS AccountingObjectCode,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_accounting_object', 'accounting_object_name', AccountingObjectID, '')) AS AccountingObjectName,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_accounting_object', 'accounting_object_address', AccountingObjectID, '')) AS AccountingObjectAddress,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_accounting_object', 'tax_code', AccountingObjectID, '')) AS TaxCode,

    ExpenseItemID,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_expense_item', 'expense_item_code', ExpenseItemID, '')) AS ExpenseItemCode,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_expense_item', 'expense_item_name', ExpenseItemID, '')) AS ExpenseItemName,

    BudgetItemID,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_budget_item', 'budget_item_code', BudgetItemID, '')) AS BudgetItemCode,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_budget_item', 'budget_item_name', BudgetItemID, '')) AS BudgetItemName,

    DepartmentID,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_eb_organization_unit', 'organization_unit_code', DepartmentID, '')) AS OrganizationUnitCode,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_eb_organization_unit', 'organization_unit_name', DepartmentID, '')) AS OrganizationUnitName,

    CostSetID,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_cost_set', 'cost_set_code', CostSetID, '')) AS CostSetCode,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_cost_set', 'cost_set_name', CostSetID, '')) AS CostSetName,

    EMContractID,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_em_contract', 'no', EMContractID, '')) AS ContractNo,

    StatisticsCodeID,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_statistics_code', 'statistics_code', StatisticsCodeID, '')) AS StatisticsCode,
    if(Reason = 'Số dư đầu kỳ', NULL, dictGetOrDefault('eb.dict_statistics_code', 'statistics_code_name', StatisticsCodeID, '')) AS StatisticsCodeName

FROM running
ORDER BY
    RepositoryCode, MaterialGoodsCode,
    RefDate ASC NULLS FIRST, InRefOrder,
    replaceRegexpAll(RefNo, '[^a-zA-Z0-9]', ''),
    RefNo, OrderPriority, ReferenceID, DetailID
    SETTINGS enable_optimize_predicate_expression = 0;
package so_chi_tiet_vat_lieu_hang_hoa

import (
	"time"

	"github.com/shopspring/decimal"
)

type Row struct {
	IsDetail bool `json:"isDetail"`

	RepositoryID   string `json:"repositoryId"`
	RepositoryCode string `json:"repositoryCode"`
	RepositoryName string `json:"repositoryName"`

	MaterialGoodsID   string `json:"materialGoodsId"`
	MaterialGoodsCode string `json:"materialGoodsCode"`
	MaterialGoodsName string `json:"materialGoodsName"`

	ReferenceID string `json:"referenceId"`
	DetailID    string `json:"detailId"`
	TypeID      int32  `json:"typeId"`

	PostedDate time.Time `json:"postedDate"`
	RefDate    time.Time `json:"refDate"`
	RefNo      string    `json:"refNo"`
	Reason     string    `json:"reason"`

	AccountCorresponding string `json:"accountCorresponding"`
	CurrencyID           string `json:"currencyId"`

	UnitID   string `json:"unitId"`
	UnitName string `json:"unitName"`

	ConvertRate   decimal.Decimal `json:"convertRate"`
	MainQuantity  decimal.Decimal `json:"mainQuantity"`
	MainUnitPrice decimal.Decimal `json:"mainUnitPrice"`
	UnitPrice     decimal.Decimal `json:"unitPrice"`

	InwardQuantity  decimal.Decimal `json:"inwardQuantity"`
	InwardAmount    decimal.Decimal `json:"inwardAmount"`
	OutwardQuantity decimal.Decimal `json:"outwardQuantity"`
	OutwardAmount   decimal.Decimal `json:"outwardAmount"`
	ClosingQuantity decimal.Decimal `json:"closingQuantity"`
	ClosingAmount   decimal.Decimal `json:"closingAmount"`

	AccountingObjectID      string `json:"accountingObjectId"`
	AccountingObjectCode    string `json:"accountingObjectCode"`
	AccountingObjectName    string `json:"accountingObjectName"`
	AccountingObjectAddress string `json:"accountingObjectAddress"`
	TaxCode                 string `json:"taxCode"`

	ExpenseItemID   string `json:"expenseItemId"`
	ExpenseItemCode string `json:"expenseItemCode"`
	ExpenseItemName string `json:"expenseItemName"`

	BudgetItemID   string `json:"budgetItemId"`
	BudgetItemCode string `json:"budgetItemCode"`
	BudgetItemName string `json:"budgetItemName"`

	DepartmentID         string `json:"departmentId"`
	OrganizationUnitCode string `json:"organizationUnitCode"`
	OrganizationUnitName string `json:"organizationUnitName"`

	CostSetID   string `json:"costSetId"`
	CostSetCode string `json:"costSetCode"`
	CostSetName string `json:"costSetName"`

	EMContractID string `json:"emContractId"`
	ContractNo   string `json:"contractNo"`

	StatisticsCodeID   string `json:"statisticsCodeId"`
	StatisticsCode     string `json:"statisticsCode"`
	StatisticsCodeName string `json:"statisticsCodeName"`
	MainUnitName       string `json:"mainUnitName"`
}

type Response struct {
	RowCount  int   `json:"rowCount"`
	ElapsedMs int64 `json:"elapsedMs"`
	Data      []Row `json:"data"`
}

package so_chi_tiet_vat_lieu_hang_hoa

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

//go:embed query.sql
var queryTemplate string

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type Service struct {
	conn clickhouse.Conn
}

func NewService(conn clickhouse.Conn) *Service {
	return &Service{conn: conn}
}

type QueryParams struct {
	CompanyIDs               []string
	PrimaryCompanyID         string
	RepositoryIDs            []string
	MaterialGoodsIDs         []string
	OwnRepositoryIDs         []string
	OtherRepositoryIDs       []string
	IsCompanyBusinessTypeGas int
	FromDate                 string
	ToDate                   string
	ParamCheckAll            bool // true = bỏ filter RepositoryID + MaterialGoodsID
	GetAccountHasData        bool
}

func (s *Service) GetSoChiTiet(ctx context.Context, p QueryParams) ([]Row, int64, error) {
	if err := validateParams(p); err != nil {
		return nil, 0, err
	}

	// Build repository filter động
	repoFilter := ""
	if len(p.RepositoryIDs) > 0 {
		repoFilter = fmt.Sprintf(
			"AND RepositoryID IN CAST([%s] AS Array(UUID))",
			quoteJoin(p.RepositoryIDs),
		)
	}

	// Build material goods filter động
	materialFilter := ""
	if !p.ParamCheckAll && len(p.MaterialGoodsIDs) > 0 {
		materialFilter = fmt.Sprintf(
			"AND MaterialGoodsID IN CAST([%s] AS Array(UUID))",
			quoteJoin(p.MaterialGoodsIDs),
		)
	} else if p.ParamCheckAll && len(p.MaterialGoodsIDs) > 0 {
		materialFilter = fmt.Sprintf(
			"AND MaterialGoodsID IN CAST([%s] AS Array(UUID))",
			quoteJoin(p.MaterialGoodsIDs),
		)
	}

	accountFilter := ""
	if p.GetAccountHasData {

		accountFilter = fmt.Sprint(
			"WHERE (is_detail = 1 AND (InwardQuantity <> 0 OR InwardAmount <> 0\n   OR OutwardQuantity <> 0 OR OutwardAmount <> 0))\n   OR (is_detail = 0 AND Reason = 'Số dư đầu kỳ' AND group_has_detail = 1)",
		)
	}

	sql := queryTemplate
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", p.ToDate)
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", p.FromDate)
	sql = strings.ReplaceAll(sql, "{{COMPANY_IDS}}", quoteJoin(p.CompanyIDs))
	sql = strings.ReplaceAll(sql, "{{PRIMARY_COMPANY_ID}}", p.PrimaryCompanyID)
	sql = strings.ReplaceAll(sql, "{{REPOSITORY_FILTER}}", repoFilter)
	sql = strings.ReplaceAll(sql, "{{MATERIAL_GOODS_FILTER}}", materialFilter)
	sql = strings.ReplaceAll(sql, "{{OWN_REPOSITORY_IDS}}", quoteJoin(p.OwnRepositoryIDs))
	sql = strings.ReplaceAll(sql, "{{OTHER_REPOSITORY_IDS}}", quoteJoin(p.OtherRepositoryIDs))
	sql = strings.ReplaceAll(sql, "{{IS_COMPANY_BUSINESS_TYPE_GAS}}", fmt.Sprintf("%d", p.IsCompanyBusinessTypeGas))
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_HAS_DATA_FILTER}}", accountFilter)

	start := time.Now()

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var result []Row
	for rows.Next() {
		var (
			// col[0]  UInt8
			isDetail uint8
			// col[1]  Nullable(UUID)
			repositoryID *uuid.UUID
			// col[2]  Nullable(String)
			repositoryCode *string
			// col[3]  Nullable(String)
			repositoryName *string
			// col[4]  Nullable(UUID)
			materialGoodsID *uuid.UUID
			// col[5]  Nullable(String)
			materialGoodsCode *string
			// col[6]  Nullable(String)
			materialGoodsName *string
			// col[7]  UUID
			referenceID uuid.UUID
			// col[8]  Nullable(UUID)
			detailID *uuid.UUID
			// col[9]  Nullable(Int32)
			typeID *int32
			// col[10] DateTime
			postedDate time.Time
			// col[11] Nullable(DateTime)
			refDate *time.Time
			// col[12] Nullable(String)
			refNo *string
			// col[13] Nullable(String)
			reason *string
			// col[14] Nullable(String)
			accountCorresponding *string
			// col[15] String
			currencyID string
			// col[16] Nullable(UUID)
			unitID *uuid.UUID
			// col[17] Nullable(String)
			unitName *string
			// col[18] Nullable(Decimal)
			convertRate *decimal.Decimal
			// col[19] Nullable(Decimal)
			mainQuantity *decimal.Decimal
			// col[20] Nullable(Decimal)
			mainUnitPrice *decimal.Decimal
			// col[21] Nullable(Decimal)
			unitPrice *decimal.Decimal
			// col[22] Decimal
			inwardQuantity decimal.Decimal
			// col[23] Decimal
			inwardAmount decimal.Decimal
			// col[24] Decimal
			outwardQuantity decimal.Decimal
			// col[25] Decimal
			outwardAmount decimal.Decimal
			// col[26] Decimal
			closingQuantity decimal.Decimal
			// col[27] Decimal
			closingAmount decimal.Decimal
			// col[28] Nullable(UUID)
			accountingObjectID *uuid.UUID
			// col[29] Nullable(String)
			accountingObjectCode *string
			// col[30] Nullable(String)
			accountingObjectName *string
			// col[31] Nullable(String)
			accountingObjectAddress *string
			// col[32] Nullable(String)
			taxCode *string
			// col[33] Nullable(UUID)
			expenseItemID *uuid.UUID
			// col[34] Nullable(String)
			expenseItemCode *string
			// col[35] Nullable(String)
			expenseItemName *string
			// col[36] Nullable(UUID)
			budgetItemID *uuid.UUID
			// col[37] Nullable(String)
			budgetItemCode *string
			// col[38] Nullable(String)
			budgetItemName *string
			// col[39] Nullable(UUID)
			departmentID *uuid.UUID
			// col[40] Nullable(String)
			organizationUnitCode *string
			// col[41] Nullable(String)
			organizationUnitName *string
			// col[42] Nullable(UUID)
			costSetID *uuid.UUID
			// col[43] Nullable(String)
			costSetCode *string
			// col[44] Nullable(String)
			costSetName *string
			// col[45] Nullable(UUID)
			emContractID *uuid.UUID
			// col[46] Nullable(String)
			contractNo *string
			// col[47] Nullable(UUID)
			statisticsCodeID *uuid.UUID
			// col[48] Nullable(String)
			statisticsCode *string
			// col[49] Nullable(String)
			statisticsCodeName *string
		)

		if err := rows.Scan(
			&isDetail,
			&repositoryID, &repositoryCode, &repositoryName,
			&materialGoodsID, &materialGoodsCode, &materialGoodsName,
			&referenceID, &detailID, &typeID,
			&postedDate, &refDate, &refNo, &reason, &accountCorresponding,
			&currencyID,
			&unitID, &unitName,
			&convertRate, &mainQuantity, &mainUnitPrice, &unitPrice,
			&inwardQuantity, &inwardAmount, &outwardQuantity, &outwardAmount,
			&closingQuantity, &closingAmount,
			&accountingObjectID, &accountingObjectCode, &accountingObjectName,
			&accountingObjectAddress, &taxCode,
			&expenseItemID, &expenseItemCode, &expenseItemName,
			&budgetItemID, &budgetItemCode, &budgetItemName,
			&departmentID, &organizationUnitCode, &organizationUnitName,
			&costSetID, &costSetCode, &costSetName,
			&emContractID, &contractNo,
			&statisticsCodeID, &statisticsCode, &statisticsCodeName,
		); err != nil {
			return nil, 0, fmt.Errorf("scan error: %w", err)
		}

		var refDateVal time.Time
		if refDate != nil {
			refDateVal = *refDate
		}

		result = append(result, Row{
			IsDetail:             isDetail == 1,
			RepositoryID:         uuidOrEmpty(repositoryID),
			RepositoryCode:       strOrEmpty(repositoryCode),
			RepositoryName:       strOrEmpty(repositoryName),
			MaterialGoodsID:      uuidOrEmpty(materialGoodsID),
			MaterialGoodsCode:    strOrEmpty(materialGoodsCode),
			MaterialGoodsName:    strOrEmpty(materialGoodsName),
			ReferenceID:          referenceID.String(),
			DetailID:             uuidOrEmpty(detailID),
			TypeID:               int32OrZero(typeID),
			PostedDate:           postedDate,
			RefDate:              refDateVal,
			RefNo:                strOrEmpty(refNo),
			Reason:               strOrEmpty(reason),
			AccountCorresponding: strOrEmpty(accountCorresponding),
			CurrencyID:           currencyID,
			UnitID:               uuidOrEmpty(unitID),
			UnitName:             strOrEmpty(unitName),
			ConvertRate:          decimalOrZero(convertRate),
			MainQuantity:         decimalOrZero(mainQuantity),
			MainUnitPrice:        decimalOrZero(mainUnitPrice),
			UnitPrice:            decimalOrZero(unitPrice),
			InwardQuantity:       inwardQuantity,
			InwardAmount:         inwardAmount,
			OutwardQuantity:      outwardQuantity,
			OutwardAmount:        outwardAmount,
			ClosingQuantity:      closingQuantity,
			ClosingAmount:        closingAmount,

			AccountingObjectID:      uuidOrEmpty(accountingObjectID),
			AccountingObjectCode:    strOrEmpty(accountingObjectCode),
			AccountingObjectName:    strOrEmpty(accountingObjectName),
			AccountingObjectAddress: strOrEmpty(accountingObjectAddress),
			TaxCode:                 strOrEmpty(taxCode),

			ExpenseItemID:   uuidOrEmpty(expenseItemID),
			ExpenseItemCode: strOrEmpty(expenseItemCode),
			ExpenseItemName: strOrEmpty(expenseItemName),

			BudgetItemID:   uuidOrEmpty(budgetItemID),
			BudgetItemCode: strOrEmpty(budgetItemCode),
			BudgetItemName: strOrEmpty(budgetItemName),

			DepartmentID:         uuidOrEmpty(departmentID),
			OrganizationUnitCode: strOrEmpty(organizationUnitCode),
			OrganizationUnitName: strOrEmpty(organizationUnitName),

			CostSetID:   uuidOrEmpty(costSetID),
			CostSetCode: strOrEmpty(costSetCode),
			CostSetName: strOrEmpty(costSetName),

			EMContractID: uuidOrEmpty(emContractID),
			ContractNo:   strOrEmpty(contractNo),

			StatisticsCodeID:   uuidOrEmpty(statisticsCodeID),
			StatisticsCode:     strOrEmpty(statisticsCode),
			StatisticsCodeName: strOrEmpty(statisticsCodeName),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("row iteration error: %w", err)
	}

	elapsedMs := time.Since(start).Milliseconds()
	return result, elapsedMs, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func uuidOrEmpty(u *uuid.UUID) string {
	if u == nil {
		return ""
	}
	return u.String()
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func int32OrZero(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

func decimalOrZero(v *decimal.Decimal) decimal.Decimal {
	if v == nil {
		return decimal.Zero
	}
	return *v
}

func validateParams(p QueryParams) error {
	if len(p.CompanyIDs) == 0 {
		return fmt.Errorf("companyIds khong duoc rong")
	}
	if p.PrimaryCompanyID == "" {
		return fmt.Errorf("primaryCompanyId khong duoc rong")
	}
	// Khi paramCheckAll = true thì không cần validate repositoryIds + materialGoodsIds
	if !p.ParamCheckAll {
		if len(p.RepositoryIDs) == 0 {
			return fmt.Errorf("repositoryIds khong duoc rong (truyen paramCheckAll=true de lay tat ca)")
		}
		if len(p.MaterialGoodsIDs) == 0 {
			return fmt.Errorf("materialGoodsIds khong duoc rong (truyen paramCheckAll=true de lay tat ca)")
		}
	}
	allIDs := append([]string{}, p.CompanyIDs...)
	allIDs = append(allIDs, p.PrimaryCompanyID)
	allIDs = append(allIDs, p.RepositoryIDs...)
	allIDs = append(allIDs, p.MaterialGoodsIDs...)
	allIDs = append(allIDs, p.OwnRepositoryIDs...)
	allIDs = append(allIDs, p.OtherRepositoryIDs...)
	for _, id := range allIDs {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("ID khong dung dinh dang UUID: %s", id)
		}
	}
	if p.FromDate == "" || p.ToDate == "" {
		return fmt.Errorf("fromDate/toDate khong duoc rong")
	}
	if p.IsCompanyBusinessTypeGas != 0 && p.IsCompanyBusinessTypeGas != 1 {
		return fmt.Errorf("isCompanyBusinessTypeGas phai la 0 hoac 1")
	}
	return nil
}

func quoteJoin(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf("'%s'", id)
	}
	return strings.Join(quoted, ", ")
}

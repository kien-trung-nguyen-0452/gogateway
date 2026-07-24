package so_chi_tiet_vat_lieu_hang_hoa

type SoChiTietVatLieuDTO struct {
	RefID                      string  `json:"refID"`
	DetailID                   string  `json:"detailID"`
	RefType                    int     `json:"refType"`
	RepositoryID               string  `json:"repositoryID"`
	RepositoryCode             string  `json:"repositoryCode"`
	RepositoryName             string  `json:"repositoryName"`
	MaterialGoodsID            string  `json:"materialGoodsID"`
	MaterialGoodsCode          string  `json:"materialGoodsCode"`
	MaterialGoodsName          string  `json:"materialGoodsName"`
	PostedDate                 string  `json:"postedDate"`
	PostedDateToString         string  `json:"postedDateToString"`
	RefDate                    string  `json:"refDate"`
	RefNo                      string  `json:"refNo"`
	AccountingObjectCode       string  `json:"accountingObjectCode"`
	AccountingObjectName       string  `json:"accountingObjectName"`
	AccountingObjectAddress    string  `json:"accountingObjectAddress"`
	TaxCode                    string  `json:"taxCode"`
	Reason                     string  `json:"reason"`
	CorrespondingAccountNumber string  `json:"correspondingAccountNumber"`
	UnitName                   string  `json:"unitName"`
	Unit                       string  `json:"unit"`
	ConvertRate                float64 `json:"convertRate"`
	MainQuantity               float64 `json:"mainQuantity"`
	MainUnitPrice              float64 `json:"mainUnitPrice"`
	UnitPrice                  float64 `json:"unitPrice"`
	InwardQuantity             float64 `json:"inwardQuantity"`
	InwardAmount               float64 `json:"inwardAmount"`
	OutwardQuantity            float64 `json:"outwardQuantity"`
	OutwardAmount              float64 `json:"outwardAmount"`
	ClosingQuantity            float64 `json:"closingQuantity"`
	ClosingAmount              float64 `json:"closingAmount"`
	ExpenseItemCode            string  `json:"expenseItemCode"`
	ExpenseItemName            string  `json:"expenseItemName"`
	BudgetItemCode             string  `json:"budgetItemCode"`
	BudgetItemName             string  `json:"budgetItemName"`
	OrganizationUnitCode       string  `json:"organizationUnitCode"`
	OrganizationUnitName       string  `json:"organizationUnitName"`
	CostSetCode                string  `json:"costSetCode"`
	CostSetName                string  `json:"costSetName"`
	ContractNo                 string  `json:"contractNo"`
	StatisticsCode             string  `json:"statisticsCode"`
	StatisticsCodeName         string  `json:"statisticsCodeName"`
	CustomField1               string  `json:"customField1"`
	CustomField2               string  `json:"customField2"`
	CustomField3               string  `json:"customField3"`
	CustomField4               string  `json:"customField4"`
	CustomField5               string  `json:"customField5"`
	CustomFieldDetail1         string  `json:"customFieldDetail1"`
	CustomFieldDetail2         string  `json:"customFieldDetail2"`
	CustomFieldDetail3         string  `json:"customFieldDetail3"`
	CustomFieldDetail4         string  `json:"customFieldDetail4"`
	CustomFieldDetail5         string  `json:"customFieldDetail5"`
	RowNum                     int     `json:"rowNum"`
	INRefOrder                 string  `json:"iNRefOrder"`
	SortOrder                  int     `json:"sortOrder"`
	ShowOnReport               bool    `json:"showOnReport"`
	DonGia                     string  `json:"donGia"`
	SlNhap                     string  `json:"slNhap"`
	TienNhap                   string  `json:"tienNhap"`
	SlXuat                     string  `json:"slXuat"`
	TienXuat                   string  `json:"tienXuat"`
	SlTon                      string  `json:"slTon"`
	TienTon                    string  `json:"tienTon"`
	NgayChungTu                string  `json:"ngayChungTu"`
	TongSLNhap                 string  `json:"tongSLNhap"`
	TongTienNhap               string  `json:"tongTienNhap"`
	TongSLXuat                 string  `json:"tongSLXuat"`
	TongTienXuat               string  `json:"tongTienXuat"`
	TongSLTon                  string  `json:"tongSLTon"`
	TongTienTon                string  `json:"tongTienTon"`
	Stt                        int     `json:"stt"`
	Loop                       bool    `json:"loop"`
	LinkRef                    string  `json:"linkRef"`
	OrderCode                  string  `json:"orderCode"`
	TotalResult                int     `json:"totalResult"`
	IsEmptyData                bool    `json:"isEmptyData"`
	SortType                   int     `json:"sortType"`
	Note                       int     `json:"note"`
	LotNo                      string  `json:"lotNo"`
	ExpireDate                 string  `json:"expireDate"`
	RefLink                    string  `json:"refLink"`
}

type Response struct {
	Data []SoChiTietVatLieuDTO `json:"data"`
}

type SoChiTietVatLieuRequest struct {
	CompanyID               string   `json:"companyID"`
	FromDate                string   `json:"fromDate"`
	ToDate                  string   `json:"toDate"`
	UnitType                int      `json:"unitType"`
	RepositoryID            string   `json:"repositoryID"`
	MaterialGoodsCategoryID string   `json:"materialGoodsCategoryID"`
	ListMaterialGoods       []string `json:"listMaterialGoods"`
	IsCheckAll              bool     `json:"isCheckAll"`
	MCodeFilter             string   `json:"mCodeFilter"`
	MNameFilter             string   `json:"mNameFilter"`
	GetAccountHasData       bool     `json:"getAccountHasData"`
	Dependent               bool     `json:"dependent"`
	OpeningBalanceMethod    int      `json:"openingBalanceMethod"`
}

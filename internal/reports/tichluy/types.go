package tichluy

import (
	"time"

	"github.com/shopspring/decimal"
)

type Row struct {
	RepositoryID       string          `json:"repositoryId"`
	MaterialGoodsID    string          `json:"materialGoodsId"`
	CumulativeQuantity decimal.Decimal `json:"cumulativeQuantity"`
	CumulativeAmount   decimal.Decimal `json:"cumulativeAmount"`
	Type               int32           `json:"type"`
	ToDate             time.Time       `json:"toDate"`
	FromDate           time.Time       `json:"fromDate"`
	TypeLedger         int32           `json:"typeLedger"`
}

// Response - wrapper kem metadata thoi gian, giup consumer (Java, hoac ai
// khac sau nay) so sanh hieu nang ma khong can doc log server.
type Response struct {
	RowCount  int   `json:"rowCount"`
	ElapsedMs int64 `json:"elapsedMs"`
	Data      []Row `json:"data"`
}

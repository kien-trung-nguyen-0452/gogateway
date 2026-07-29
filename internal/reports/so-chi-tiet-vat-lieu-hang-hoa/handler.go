package so_chi_tiet_vat_lieu_hang_hoa

import (
	"encoding/json"
	"log"
	"net/http"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

type RequestBody struct {
	CompanyIDs               []string `json:"companyId"`
	PrimaryCompanyID         string   `json:"primaryCompanyId"`
	RepositoryIDs            []string `json:"repositoryIds"`
	MaterialGoodsIDs         []string `json:"materialGoodsIds"`
	OwnRepositoryIDs         []string `json:"ownRepositoryIds"`
	OtherRepositoryIDs       []string `json:"otherRepositoryIds"`
	IsCompanyBusinessTypeGas int      `json:"isCompanyBusinessTypeGas"`
	FromDate                 string   `json:"fromDate"`
	ToDate                   string   `json:"toDate"`
	ParamCheckAll            bool     `json:"paramCheckAll"` // true = lấy tất cả kho + hàng hóa
	GetAccountHasData        bool     `json:"getAccountHasData"`
	UnitType                 int      `json:"unitType"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed - dung POST")
		return
	}

	var body RequestBody
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "khong parse duoc JSON body: "+err.Error())
		return
	}

	params := QueryParams{
		CompanyIDs:               body.CompanyIDs,
		PrimaryCompanyID:         body.PrimaryCompanyID,
		RepositoryIDs:            body.RepositoryIDs,
		MaterialGoodsIDs:         body.MaterialGoodsIDs,
		OwnRepositoryIDs:         body.OwnRepositoryIDs,
		OtherRepositoryIDs:       body.OtherRepositoryIDs,
		IsCompanyBusinessTypeGas: body.IsCompanyBusinessTypeGas,
		FromDate:                 body.FromDate,
		ToDate:                   body.ToDate,
		ParamCheckAll:            body.ParamCheckAll,
		GetAccountHasData:        body.GetAccountHasData,
		UnitType:                 body.UnitType,
	}

	rows, elapsedMs, err := h.service.GetSoChiTiet(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// ?metaOnly=true → chỉ trả rowCount + elapsedMs, không trả data
	metaOnly := r.URL.Query().Get("metaOnly") == "true"

	resp := Response{
		RowCount:  len(rows),
		ElapsedMs: elapsedMs,
	}
	if !metaOnly {
		resp.Data = rows
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("loi encode response (co the client da dong ket noi): %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}

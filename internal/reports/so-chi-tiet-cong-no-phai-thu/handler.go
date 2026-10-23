package so_chi_tiet_cong_no_phai_thu

import (
	"encoding/json"
	"log"
	"net/http"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler { return &Handler{service: service} }

type RequestBody struct {
	CompanyIDs       []string `json:"companyIds"`
	PrimaryCompanyID string   `json:"primaryCompanyId"`
	FromDate         string   `json:"fromDate"`
	ToDate           string   `json:"toDate"`
	IsFinancialBook  int      `json:"isFinancialBook"`
	IsDependent      int      `json:"isDependent"`
	GroupSameItem    int      `json:"groupSameItem"`
	AccountNumber    string   `json:"accountNumber"`
	CurrencyID       string   `json:"currencyId"`

	CheckAll          int      `json:"checkAll"`
	ObjectIDs         []string `json:"objectIds"`
	ObjectCode        string   `json:"accountingObjectCode"`
	ObjectName        string   `json:"accountingObjectName"`
	ObjectAddress     string   `json:"accountingObjectAddress"`
	AccountObjectGrID string   `json:"accountObjectGroupId"`

	GetAccountHasData int    `json:"getAccountHasData"`
	ClusterID         string `json:"clusterId"`
}

func (b RequestBody) params() QueryParams {
	return QueryParams{
		CompanyIDs:        b.CompanyIDs,
		PrimaryCompanyID:  b.PrimaryCompanyID,
		FromDate:          b.FromDate,
		ToDate:            b.ToDate,
		IsFinancialBook:   b.IsFinancialBook,
		IsDependent:       b.IsDependent,
		GroupSameItem:     b.GroupSameItem,
		AccountNumber:     b.AccountNumber,
		CurrencyID:        b.CurrencyID,
		CheckAll:          b.CheckAll,
		ObjectIDs:         b.ObjectIDs,
		ObjectCode:        b.ObjectCode,
		ObjectName:        b.ObjectName,
		ObjectAddr:        b.ObjectAddress,
		GroupID:           b.AccountObjectGrID,
		GetAccountHasData: b.GetAccountHasData,
		ClusterID:         b.ClusterID,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed - dùng POST")
		return
	}

	var body RequestBody
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "không parse được JSON body: "+err.Error())
		return
	}
	params := body.params()

	switch {
	case r.URL.Query().Get("metaOnly") == "true":
		h.serveMetaOnly(w, r, params)
	case r.URL.Query().Get("stream") == "true":
		h.serveNDJSON(w, r, params)
	default:
		h.serveJSON(w, r, params)
	}
}

// serveNDJSON stream mỗi dòng một object JSON, ngăn cách bằng '\n'. Đường nên
// dùng cho báo cáo lớn: bản JSON gộp phải giữ toàn bộ []Row trong RAM rồi
// json.Marshal thêm một bản nữa.
//
// Đánh đổi: không đặt được HTTP status sau khi đã ghi dòng đầu. Lỗi giữa chừng
// chỉ báo được bằng một object {"error": ...} ở cuối luồng — client phải kiểm
// tra dòng cuối.
func (h *Handler) serveNDJSON(w http.ResponseWriter, r *http.Request, p QueryParams) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)
	n := 0

	elapsed, err := h.service.StreamSoChiTiet(r.Context(), p, func(row Row) error {
		if err := enc.Encode(row); err != nil {
			return err
		}
		n++
		// Flush theo lô để client thấy dữ liệu sớm mà không tốn syscall cho từng dòng.
		if canFlush && n%500 == 0 {
			flusher.Flush()
		}
		return nil
	})

	if err != nil {
		_ = enc.Encode(map[string]string{"error": err.Error()})
		log.Printf("so_chi_tiet_cong_no_phai_thu: lỗi stream sau %d dòng: %v", n, err)
	} else {
		_ = enc.Encode(map[string]any{"rowCount": n, "elapsedMs": elapsed})
	}
	if canFlush {
		flusher.Flush()
	}
}

func (h *Handler) serveJSON(w http.ResponseWriter, r *http.Request, p QueryParams) {
	rows, elapsedMs, err := h.service.GetSoChiTiet(r.Context(), p)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(Response{
		RowCount:  len(rows),
		ElapsedMs: elapsedMs,
		Data:      rows,
	}); err != nil {
		log.Printf("lỗi encode response (có thể client đã đóng kết nối): %v", err)
	}
}

// serveMetaOnly chỉ đếm, không giữ dòng nào — dùng để ước lượng kích thước
// trước khi client quyết định tải hay không.
func (h *Handler) serveMetaOnly(w http.ResponseWriter, r *http.Request, p QueryParams) {
	n := 0
	elapsed, err := h.service.StreamSoChiTiet(r.Context(), p, func(Row) error {
		n++
		return nil
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Response{RowCount: n, ElapsedMs: elapsed})
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

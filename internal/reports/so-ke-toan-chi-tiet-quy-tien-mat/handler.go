package so_ke_toan_chi_tiet_quy_tien_mat

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
	AccountNumbers   []string `json:"accountNumbers"`
	FromDate         string   `json:"fromDate"`
	ToDate           string   `json:"toDate"`
	CurrencyID       string   `json:"currencyId"`
	TypeLedger       int      `json:"typeLedger"`
	TypeShowCurrency int      `json:"typeShowCurrency"`
	GroupTheSameItem int      `json:"groupTheSameItem"`
	IsDependent      int      `json:"isDependent"`
	ClusterID        string   `json:"clusterId"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed - dung POST")
		return
	}

	var body RequestBody
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "khong parse duoc JSON body: "+err.Error())
		return
	}

	params := QueryParams{
		CompanyIDs:       body.CompanyIDs,
		PrimaryCompanyID: body.PrimaryCompanyID,
		AccountNumbers:   body.AccountNumbers,
		FromDate:         body.FromDate,
		ToDate:           body.ToDate,
		CurrencyID:       body.CurrencyID,
		TypeLedger:       body.TypeLedger,
		TypeShowCurrency: body.TypeShowCurrency,
		GroupTheSameItem: body.GroupTheSameItem,
		IsDependent:      body.IsDependent,
		ClusterID:        body.ClusterID,
	}

	if r.URL.Query().Get("stream") == "true" {
		h.serveNDJSON(w, r, params)
		return
	}
	h.serveJSON(w, r, params)
}

// serveNDJSON stream mỗi dòng một object JSON.
//
// Đây là đường nên dùng cho báo cáo lớn. Bản JSON gộp phải giữ toàn bộ []Row
// trong RAM RỒI json.Marshal thêm một bản nữa.
//
// Đánh đổi: không đặt được HTTP status sau khi đã ghi dòng đầu. Lỗi giữa chừng
// chỉ báo được bằng một object {"error": ...} ở cuối luồng.
func (h *Handler) serveNDJSON(w http.ResponseWriter, r *http.Request, p QueryParams) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)
	n := 0

	elapsed, err := h.service.StreamSoQuy(r.Context(), p, func(row Row) error {
		if err := enc.Encode(row); err != nil {
			return err
		}
		n++
		if canFlush && n%500 == 0 {
			flusher.Flush()
		}
		return nil
	})

	if err != nil {
		_ = enc.Encode(map[string]string{"error": err.Error()})
		log.Printf("so_ke_toan_chi_tiet_quy_tien_mat: loi stream sau %d dong: %v", n, err)
	} else {
		_ = enc.Encode(map[string]any{"rowCount": n, "elapsedMs": elapsed})
	}
	if canFlush {
		flusher.Flush()
	}
}

func (h *Handler) serveJSON(w http.ResponseWriter, r *http.Request, p QueryParams) {
	rows, elapsedMs, err := h.service.GetSoQuy(r.Context(), p)
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
		log.Printf("loi encode response (co the client da dong ket noi): %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

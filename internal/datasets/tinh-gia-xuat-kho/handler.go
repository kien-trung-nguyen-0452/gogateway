package tinh_gia_xuat_kho

import (
	"encoding/json"
	"log"
	"net/http"
)

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// ServeHTTP nhận request POST, đọc JSON body, gọi Service rồi trả kết quả.
//
// Tham số truy vấn ?metaOnly=true chỉ trả số dòng và thời gian chạy, bỏ phần
// dữ liệu. Dùng khi muốn đo tốc độ mà không tải về khối JSON lớn.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed - dùng POST")
		return
	}

	var body RequestBody
	decoder := json.NewDecoder(r.Body)
	// Báo lỗi ngay khi gõ sai tên trường. Nếu bỏ qua, gõ nhầm "materialGoodsId"
	// thiếu chữ s sẽ khiến bộ lọc không có tác dụng mà không ai biết.
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "không parse được JSON body: "+err.Error())
		return
	}

	params := QueryParams{
		CompanyID:        body.CompanyID,
		TypeLedger:       body.TypeLedger,
		FromDate:         body.FromDate,
		ToDate:           body.ToDate,
		RepositoryIDs:    body.RepositoryIDs,
		MaterialGoodsIDs: body.MaterialGoodsIDs,
	}

	rows, elapsedMs, err := h.service.GetTinhGiaXuatKho(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

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
		log.Printf("lỗi encode response (có thể client đã đóng kết nối): %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(map[string]string{"error": msg})
	if err != nil {
		return
	} //nolint:errcheck
}

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
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Kiểm tra mothod truyền vào
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Không hỗ trợ HTTP Method - dùng POST")
		return
	}

	// Kiểm tra requestBody truyền vào
	var body RequestBody
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "Body không hợp lệ: "+err.Error())
		return
	}

	// Lấy dữ liệu, kiểm tra có lỗi thì trả lỗi
	rows, err := h.service.GetTinhGiaXuatKho(r.Context(), body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rows); err != nil {
		log.Printf("lỗi encode response (có thể client đã đóng kết nối): %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(map[string]string{"error": msg})
	if err != nil {
		return
	}
}

package tichluy

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	q := r.URL.Query()

	year, err := strconv.Atoi(q.Get("year"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "param 'year' khong hop le")
		return
	}

	companyIDsStr := q.Get("companyIds")
	if companyIDsStr == "" {
		writeError(w, http.StatusBadRequest, "param 'companyIds' khong duoc rong")
		return
	}
	companyIDs := strings.Split(companyIDsStr, ",")

	rows, elapsedMs, err := h.service.GetTichLuy(r.Context(), year, companyIDs)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	resp := Response{
		RowCount:  len(rows),
		ElapsedMs: elapsedMs,
		Data:      rows,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		// response da bat dau ghi, chi log duoc, khong the doi status code nua
		http.Error(w, `{"error":"encode response that bai"}`, http.StatusInternalServerError)
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

package api

import (
	"net/http"

	"softdream.vn/go-gateway/internal/reports/tichluy"
)

func NewRouter(tichLuyHandler *tichluy.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/internal/v1/reports/tich-luy", tichLuyHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK")) //nolint:errcheck
	})
	return mux
}

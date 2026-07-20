package main

import (
	"log"
	"net/http"

	"softdream.vn/go-gateway/internal/api"
	"softdream.vn/go-gateway/internal/chclient"
	"softdream.vn/go-gateway/internal/config"
	"softdream.vn/go-gateway/internal/reports/tichluy"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("loi cau hinh: %v", err)
	}

	conn, err := chclient.NewConn(cfg)
	if err != nil {
		log.Fatalf("loi ket noi ClickHouse: %v", err)
	}
	defer conn.Close()

	log.Printf("Da ket noi ClickHouse qua native protocol tai %s:%d", cfg.CHHost, cfg.CHPort)

	tichLuyService := tichluy.NewService(conn)
	tichLuyHandler := tichluy.NewHandler(tichLuyService)

	mux := api.NewRouter(tichLuyHandler)

	addr := ":" + cfg.HTTPPort
	log.Printf("Go satellite dang chay tai %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

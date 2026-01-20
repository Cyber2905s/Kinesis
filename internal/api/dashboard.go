package api

import (
	_ "embed"
	"net/http"
)

//go:embed web/index.html
var dashboard []byte

func serveDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboard)
}

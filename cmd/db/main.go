package main

import (
	"encoding/json"
	"github.com/bohdanbulakh/kpi-lab5/datastore"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

var db *datastore.Db

func main() {
	dataDir := "./data"
	_ = os.MkdirAll(dataDir, 0o755)

	var err error
	db, err = datastore.Open(filepath.Join(dataDir), 10*1024*1024) // 10MB
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}

	http.HandleFunc("/db/", handleDb)

	log.Println("DB service running on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func handleDb(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		value, err := db.Get(key)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		resp := map[string]string{
			"key":   key,
			"value": value,
		}
		_ = json.NewEncoder(w).Encode(resp)

	case http.MethodPost:
		var req struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if err := db.Put(key, req.Value); err != nil {
			http.Error(w, "failed to write", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

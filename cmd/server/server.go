package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bohdanbulakh/kpi-lab4/httptools"
	"github.com/bohdanbulakh/kpi-lab4/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

const dbURL = "http://db:8081/db"

const teamName = "dreamteam"

func main() {
	flag.Parse()
	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	go func() {
		time.Sleep(1 * time.Second)
		now := time.Now().Format("2006-01-02")
		payload, _ := json.Marshal(map[string]string{"value": now})
		_, _ = http.Post(fmt.Sprintf("%s/%s", dbURL, teamName), "application/json", bytes.NewReader(payload))
	}()

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "missing key", http.StatusBadRequest)
			return
		}

		resp, err := http.Get(fmt.Sprintf("%s/%s", dbURL, key))
		if err != nil || resp.StatusCode != http.StatusOK {
			http.NotFound(rw, r)
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write(body)
	})

	report := make(Report)
	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

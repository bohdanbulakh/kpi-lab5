package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/bohdanbulakh/kpi-lab5/httptools"
	"github.com/bohdanbulakh/kpi-lab5/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	trafficStats   = make(map[string]int64)
	healthyServers = []string{}
	statsMutex     sync.Mutex
	healthMutex    sync.RWMutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()

		written, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		} else {
			statsMutex.Lock()
			trafficStats[dst] += written
			statsMutex.Unlock()
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func chooseServerWithLeastTraffic() (string, error) {
	var bestServer string
	var minTraffic int64 = -1

	statsMutex.Lock()
	defer statsMutex.Unlock()

	healthMutex.RLock()
	defer healthMutex.RUnlock()

	for _, server := range healthyServers {
		traffic := trafficStats[server]
		if minTraffic == -1 || traffic < minTraffic {
			bestServer = server
			minTraffic = traffic
		}
	}

	if bestServer == "" {
		return "", fmt.Errorf("no healthy servers")
	}
	return bestServer, nil
}

func main() {
	flag.Parse()

	go func() {
		for range time.Tick(10 * time.Second) {
			var tempHealthy []string
			for _, server := range serversPool {
				if health(server) {
					tempHealthy = append(tempHealthy, server)
					log.Println(server, "healthy: true")
				} else {
					log.Println(server, "healthy: false")
				}
			}
			healthMutex.Lock()
			healthyServers = tempHealthy
			healthMutex.Unlock()
		}
	}()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server, err := chooseServerWithLeastTraffic()
		if err != nil {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}
		forward(server, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

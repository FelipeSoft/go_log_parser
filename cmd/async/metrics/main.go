package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var httpWg sync.WaitGroup
	server := http.NewServeMux()
	server.Handle("/metrics", promhttp.Handler())

	httpWg.Add(1)
	go func() {
		defer httpWg.Done()
		err := http.ListenAndServe("192.168.200.154:8080", server)
		if err != nil {
			log.Fatalf("Error on HTTP server starting: %v", err)
		}
	}()
	log.Print("HTTP Server for metrics listening on 192.168.200.154:8080")

	httpWg.Wait()
}

package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var (
	inflight       atomic.Int64
	inflightMetric metric.Int64UpDownCounter
)

func initMetrics() {
	exporter, err := prometheus.New()
	if err != nil {
		log.Fatal(err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	inflightMetric, err = provider.Meter("server").Int64UpDownCounter(
		"server_inflight_requests",
		metric.WithDescription("Number of requests currently being handled by the server"),
	)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		m := http.NewServeMux()
		m.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":9464", m))
	}()
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// logger logs each request with its status, duration and the number of
// requests currently in flight on the server.
func logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		n := inflight.Add(1)
		inflightMetric.Add(context.Background(), 1)
		defer func() {
			inflight.Add(-1)
			inflightMetric.Add(context.Background(), -1)
		}()

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		log.Printf("%s %s %s status=%d duration=%s inflight_at_start=%d",
			r.RemoteAddr, r.Method, r.URL.Path, rec.status, time.Since(start).Round(time.Millisecond), n)
	})
}

func exportCSV(w http.ResponseWriter, r *http.Request) {
	// Simulate a slow export; abort early if the client goes away.
	select {
	case <-time.After(10 * time.Second):
	case <-r.Context().Done():
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", `attachment; filename="export.csv"`)
	w.Write([]byte("id,name\n1,alice\n2,bob\n"))
}

func main() {
	initMetrics()
	mux := http.NewServeMux()
	mux.HandleFunc("/export-csv", exportCSV)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, logger(mux)))
}

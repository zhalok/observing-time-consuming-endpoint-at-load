package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"server/internal/jobs"
	"server/internal/telemetry"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var (
	inflight       atomic.Int64
	inflightMetric metric.Int64UpDownCounter
)

var (
	store *jobs.Store
	queue *jobs.Queue
)

func initMetrics() {
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9464"
	}
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
		log.Fatal(http.ListenAndServe(":"+metricsPort, m))
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
		// Health checks are frequent and uninteresting: keep them out of the
		// logs and out of the inflight metric.
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// exportCSV no longer does the work itself: it records a job in Postgres,
// queues it for the consumers and returns the job id right away.
func exportCSV(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	id, err := store.Create(ctx)
	if err != nil {
		log.Printf("create job: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not create job"})
		return
	}
	if err := queue.Publish(ctx, id); err != nil {
		log.Printf("publish job %s: %v", id, err)
		store.SetStatus(context.Background(), id, jobs.StatusFailed)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not queue job"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"job_id": id, "status": jobs.StatusPending})
}

// healthz reports whether the server can do its job: Postgres reachable and
// the RabbitMQ connection open.
func healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": "postgres: " + err.Error()})
		return
	}
	if !queue.Healthy() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": "rabbitmq: connection closed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// pollJob returns the current status of a job.
func pollJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	status, found, err := store.Get(r.Context(), id)
	switch {
	case err != nil:
		log.Printf("get job %s: %v", id, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not read job"})
	case !found:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
	default:
		writeJSON(w, http.StatusOK, map[string]string{"job_id": id, "status": status})
	}
}

func main() {
	initMetrics()
	defer telemetry.Init(context.Background(), "server")()

	var err error
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/jobs?sslmode=disable"
	}
	if store, err = jobs.NewStore(dbURL); err != nil {
		log.Fatal(err)
	}
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}
	// The API only publishes; the separate worker service consumes.
	if queue, err = jobs.NewQueue(amqpURL, jobs.EnvInt("PUBLISHER_CHANNELS", 10)); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /export-csv", exportCSV)
	mux.HandleFunc("GET /jobs/{id}", pollJob)
	mux.HandleFunc("GET /healthz", healthz)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("listening on :%s", port)

	// Trace job submissions. Polls are far more numerous and uninteresting, so
	// they are skipped unless TRACE_POLLS=true.
	tracePolls := os.Getenv("TRACE_POLLS") == "true"
	traced := otelhttp.NewHandler(mux, "http",
		otelhttp.WithFilter(func(r *http.Request) bool {
			return tracePolls || r.URL.Path == "/export-csv"
		}),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)
	log.Fatal(http.ListenAndServe(":"+port, logger(traced)))
}

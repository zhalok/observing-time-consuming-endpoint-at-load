// Package telemetry sets up OpenTelemetry tracing, exporting spans over OTLP
// (gRPC) to Tempo. The endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT.
package telemetry

import (
	"context"
	"log"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init installs a global tracer provider and W3C trace-context propagation.
// The returned function flushes pending spans; call it on shutdown.
func Init(ctx context.Context, serviceName string) func() {
	// Tracing every request of a big burst is expensive; allow sampling.
	ratio := 1.0
	if v := os.Getenv("TRACE_SAMPLE_RATIO"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			ratio = f
		}
	}

	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		log.Printf("tracing disabled: %v", err)
		return func() {}
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(serviceName))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	return func() { tp.Shutdown(context.Background()) }
}

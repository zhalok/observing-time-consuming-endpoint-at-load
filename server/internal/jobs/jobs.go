package jobs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("jobs")

// startSpan starts a span and returns a func that ends it, recording err.
func startSpan(ctx context.Context, name string, kind trace.SpanKind, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	ctx, span := tracer.Start(ctx, name, trace.WithSpanKind(kind), trace.WithAttributes(attrs...))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// amqpCarrier lets the OTel propagator read/write trace context in the
// headers of an AMQP message, so a trace continues across the queue.
type amqpCarrier amqp.Table

func (c amqpCarrier) Get(key string) string {
	if v, ok := c[key].(string); ok {
		return v
	}
	return ""
}
func (c amqpCarrier) Set(key, value string) { c[key] = value }
func (c amqpCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = amqpCarrier{}

var dbAttrs = []attribute.KeyValue{attribute.String("db.system", "postgresql")}

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusSucceeded  = "succeeded"
	StatusFailed     = "failed"

	queueName = "export_jobs"
)

// Statuses lists every job status, so metrics can report zero for empty ones.
var Statuses = []string{StatusPending, StatusProcessing, StatusSucceeded, StatusFailed}

// EnvInt reads an int from the environment, falling back to def.
func EnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// EnvDuration reads a duration from the environment, falling back to def.
func EnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// retry keeps calling fn until it succeeds or the deadline passes. Used at
// startup so the server can come up before Postgres/RabbitMQ are ready.
func retry(what string, d time.Duration, fn func() error) error {
	deadline := time.Now().Add(d)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: %w", what, err)
		}
		log.Printf("waiting for %s: %v", what, err)
		time.Sleep(2 * time.Second)
	}
}

// ---- Postgres ----

// Store keeps jobs in Postgres (schema: postgres/init.sql).
type Store struct{ pool *pgxpool.Pool }

// NewStore connects to Postgres, retrying until it is reachable.
func NewStore(url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	// A burst of job creations all hit the pool; size it explicitly.
	cfg.MaxConns = int32(EnvInt("DB_MAX_CONNS", 50))

	var pool *pgxpool.Pool
	err = retry("postgres", time.Minute, func() error {
		var err error
		pool, err = pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			return err
		}
		// The schema comes from postgres/init.sql; just check the DB is reachable.
		err = pool.Ping(context.Background())
		if err != nil {
			pool.Close()
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Ping checks that the database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// CountByStatus returns how many jobs are in each status.
func (s *Store) CountByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		counts[status] = n
	}
	return counts, rows.Err()
}

func (s *Store) Create(ctx context.Context) (string, error) {
	ctx, end := startSpan(ctx, "db insert job", trace.SpanKindClient, dbAttrs...)
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO jobs (status) VALUES ($1) RETURNING id::text`, StatusPending).Scan(&id)
	end(err)
	return id, err
}

func (s *Store) SetStatus(ctx context.Context, id, status string) error {
	ctx, end := startSpan(ctx, "db update job status", trace.SpanKindClient,
		append(dbAttrs, attribute.String("job.id", id), attribute.String("job.status", status))...)
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = $2, updated_at = now() WHERE id = $1::uuid`, id, status)
	end(err)
	return err
}

// ---- RabbitMQ ----

// Queue wraps RabbitMQ: publishing job ids and consuming them.
type Queue struct {
	conn *amqp.Connection
	pool chan *amqp.Channel // publisher channels (a channel is not goroutine-safe)
}

// NewQueue connects to RabbitMQ and opens `publishers` confirm-mode channels.
func NewQueue(url string, publishers int) (*Queue, error) {
	q := &Queue{}
	err := retry("rabbitmq", time.Minute, func() error {
		var err error
		q.conn, err = amqp.Dial(url)
		return err
	})
	if err != nil {
		return nil, err
	}

	go func() {
		log.Fatalf("rabbitmq connection closed: %v", <-q.conn.NotifyClose(make(chan *amqp.Error, 1)))
	}()

	q.pool = make(chan *amqp.Channel, publishers)
	for i := 0; i < publishers; i++ {
		ch, err := q.publisherChannel()
		if err != nil {
			return nil, err
		}
		q.pool <- ch
	}
	return q, nil
}

// Healthy reports whether the RabbitMQ connection is still open.
func (q *Queue) Healthy() bool { return !q.conn.IsClosed() }

func (q *Queue) declare(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
	return err
}

func (q *Queue) publisherChannel() (*amqp.Channel, error) {
	ch, err := q.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := q.declare(ch); err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	return ch, nil
}

// Publish sends the job id to the Queue and waits for the broker's confirm.
func (q *Queue) Publish(ctx context.Context, jobID string) error {
	// Producer span; its context travels in the message headers so the
	// worker's span becomes part of the same trace.
	ctx, end := startSpan(ctx, queueName+" publish", trace.SpanKindProducer,
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.destination.name", queueName),
		attribute.String("job.id", jobID))

	var ch *amqp.Channel
	select {
	case ch = <-q.pool:
	case <-ctx.Done():
		end(ctx.Err())
		return ctx.Err()
	}

	headers := amqp.Table{}
	otel.GetTextMapPropagator().Inject(ctx, amqpCarrier(headers))

	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Headers:      headers,
		Body:         []byte(jobID),
	})
	acked := false
	if err == nil {
		acked, err = conf.WaitContext(ctx)
	}
	if err == nil && !acked {
		err = errors.New("publish nacked by broker")
	}
	end(err)

	if ch.IsClosed() {
		// Replace a broken channel so the pool does not shrink.
		if nc, cerr := q.publisherChannel(); cerr == nil {
			ch = nc
		}
	}
	q.pool <- ch
	return err
}

// StartConsumers launches n consumers. Each takes one message at a time
// (prefetch 1), waits `work`, then marks the job succeeded.
func (q *Queue) StartConsumers(n int, store *Store, work time.Duration) error {
	for i := 1; i <= n; i++ {
		ch, err := q.conn.Channel()
		if err != nil {
			return err
		}
		if err := q.declare(ch); err != nil {
			return err
		}
		if err := ch.Qos(1, 0, false); err != nil {
			return err
		}
		tag := fmt.Sprintf("consumer-%d", i)
		msgs, err := ch.Consume(queueName, tag, false, false, false, false, nil)
		if err != nil {
			return err
		}
		go func() {
			for d := range msgs {
				id := string(d.Body)
				// Continue the trace started by the API (via the message headers).
				ctx := otel.GetTextMapPropagator().Extract(context.Background(), amqpCarrier(d.Headers))
				ctx, end := startSpan(ctx, queueName+" process", trace.SpanKindConsumer,
					attribute.String("messaging.system", "rabbitmq"),
					attribute.String("messaging.destination.name", queueName),
					attribute.String("messaging.consumer", tag),
					attribute.String("job.id", id))
				err := process(ctx, store, id, work)
				end(err)
				if err != nil {
					log.Printf("%s: job %s failed: %v (requeue)", tag, id, err)
					d.Nack(false, true)
					continue
				}
				d.Ack(false)
				log.Printf("%s: job %s succeeded", tag, id)
			}
		}()
	}
	return nil
}

func process(ctx context.Context, store *Store, id string, work time.Duration) error {
	if err := store.SetStatus(ctx, id, StatusProcessing); err != nil {
		return err
	}
	_, endWork := startSpan(ctx, "do work", trace.SpanKindInternal)
	time.Sleep(work) // the "time-consuming" part
	endWork(nil)
	return store.SetStatus(ctx, id, StatusSucceeded)
}

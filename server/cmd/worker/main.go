// Worker consumes jobs from RabbitMQ. Each consumer takes one job at a time,
// reads the customer CSV, sums its c_acctbal column and marks the job succeeded.
package main

import (
	"context"
	"log"
	"os"

	"server/internal/jobs"
	"server/internal/telemetry"
)

func main() {
	telemetry.Init(context.Background(), "worker")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/jobs?sslmode=disable"
	}
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	store, err := jobs.NewStore(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	queue, err := jobs.NewQueue(amqpURL, 0) // consume only, no publishing
	if err != nil {
		log.Fatal(err)
	}

	n := jobs.EnvInt("CONSUMER_COUNT", 5)
	if err := queue.StartConsumers(n, store, jobs.CSVPath()); err != nil {
		log.Fatal(err)
	}
	log.Printf("worker started with %d consumers", n)
	select {} // run until killed; a lost RabbitMQ connection exits via log.Fatal
}

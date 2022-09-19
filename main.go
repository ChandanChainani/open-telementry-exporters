package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"

	"github.com/ChandanChainani/open-telementry-exporters/exporters"
)

func main() {
	l := log.New(os.Stdout, "", 0)

	// Write telemetry data to a db.
	db, err := SqlOpen("sqlite3", "test.db")
	if err != nil {
		l.Fatal(err)
	}
	defer db.Close()

	tableName := "logs"
	err = CreateTable(db, tableName)
	if err != nil {
		l.Fatal(err)
	}

	logWriter := &exporters.LogWriter{
		Writer: func(span *exporters.SpanStub) error {
			return InsertLog(db, tableName, span)
		},
	}
	exp, err := newExporter(logWriter)
	if err != nil {
		l.Fatal(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(newResource()),
	)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			l.Fatal(err)
		}
	}()
	otel.SetTracerProvider(tp)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	errCh := make(chan error)
	app := NewApp(os.Stdin, l)
	go func() {
		errCh <- app.Run(context.Background())
	}()

	select {
	case <-sigCh:
		l.Println("\ngoodbye")
		return
	case err := <-errCh:
		if err != nil {
			l.Fatal(err)
		}
	}
}

// newExporter returns a console exporter.
func newExporter(w io.Writer) (trace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(w),
		// Use human-readable output.
		stdouttrace.WithPrettyPrint(),
		// Do not print timestamps for the demo.
		stdouttrace.WithoutTimestamps(),
	)
}

// newResource returns a resource describing this application.
func newResource() *resource.Resource {
	r, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("fib"),
			semconv.ServiceVersionKey.String("v0.1.0"),
			attribute.String("environment", "demo"),
		),
	)
	return r
}

const insertLogsSQLTemplate = `
  INSERT INTO %s (
    TraceId, SpanId, TraceFlags,
    ServiceName, ResourceAttributes,
    LogAttributes, StartTime, EndTime
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`

func InsertLog(db Sqlite, tableName string, span *exporters.SpanStub) error {
	query := fmt.Sprintf(insertLogsSQLTemplate, tableName)
	if err := db.Insert(
		query,
		span.SpanContext.TraceID().String(),
		span.SpanContext.SpanID().String(),
		span.SpanContext.TraceFlags().String(),
		span.InstrumentationLibrary.Name,
		fmt.Sprintf("%+v", span.Resource),
		fmt.Sprintf("%+v", span.Attributes),
		span.StartTime,
		span.EndTime,
	); err != nil {
		return fmt.Errorf("exec: insert sql: %w", err)
	}
	return nil
}

const createLogsTableSQL = `
  CREATE TABLE IF NOT EXISTS %s (
    TraceId TEXT,
    SpanId TEXT,
    TraceFlags INTEGER,
    ServiceName TEXT,
    ResourceAttributes TEXT,
    LogAttributes TEXT,
    StartTime DATETIME DEFAULT CURRENT_TIMESTAMP,
    EndTime DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`

func CreateTable(db Sqlite, tableName string) error {
	query := fmt.Sprintf(createLogsTableSQL, tableName)
	if err := db.Insert(query); err != nil {
		return fmt.Errorf("exec: create table sql: %w", err)
	}

	return nil
}

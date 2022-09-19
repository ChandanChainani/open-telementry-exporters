package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	_ "github.com/mattn/go-sqlite3"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

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

const insertLogsSQLTemplate = `
  INSERT INTO %s (
    TraceId, SpanId, TraceFlags,
    ServiceName, ResourceAttributes,
    LogAttributes, StartTime, EndTime
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`

type Sqlite struct {
	db *sql.DB
}

func (e *Sqlite) Insert(query string, args ...interface{}) error {
	return e.doWithTx(func(tx *sql.Tx) error {
		statement, err := tx.Prepare(query)
		if err != nil {
			return fmt.Errorf("Prepare: %w", err)
		}
		defer statement.Close()

		_, err = statement.Exec(args...)
		if err != nil {
			return fmt.Errorf("ExecContext: %w", err)
		}
		return nil
	})
}

func (s *Sqlite) doWithTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

type LogWriter struct {
	tableName string
	writer    Sqlite
}

type SpanStub struct {
	tracetest.SpanStub
	Resource []attribute.KeyValue
}

func (d *LogWriter) Write(data []byte) (n int, err error) {
	var span SpanStub
	if err = json.Unmarshal(data, &span); err == nil {
		query := fmt.Sprintf(insertLogsSQLTemplate, d.tableName)
		if err := d.writer.Insert(query, span.SpanContext.TraceID().String(), span.SpanContext.SpanID().String(), span.SpanContext.TraceFlags().String(), span.InstrumentationLibrary.Name, fmt.Sprintf("%+v", span.Resource), fmt.Sprintf("%+v", span.Attributes), span.StartTime, span.EndTime); err != nil {
			return -1, fmt.Errorf("exec: insert sql: %w", err)
		}
	}
	return len(data), nil
}

func (d *LogWriter) CreateTable() error {
	query := fmt.Sprintf(createLogsTableSQL, d.tableName)
	if err := d.writer.Insert(query); err != nil {
		return fmt.Errorf("exec: create table sql: %w", err)
	}

	return nil
}

func main() {
	l := log.New(os.Stdout, "", 0)

	// Write telemetry data to a db.
	db, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		l.Fatal(err)
	}
	defer db.Close()

	logWriter := &LogWriter{tableName: "logs", writer: Sqlite{db}}
	logWriter.CreateTable()
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

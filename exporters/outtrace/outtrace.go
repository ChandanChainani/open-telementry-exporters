package outtrace

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var zeroTime time.Time

var _ trace.SpanExporter = &Exporter{}

type WriteCall func(*tracetest.SpanStub) error

// New creates an Exporter with the passed options.
func New(w WriteCall) (*Exporter, error) {
	return &Exporter{
		writer:     w,
	}, nil
}

// Exporter is an implementation of trace.SpanSyncer that pass spans to writer callback.
type Exporter struct {
	writer     WriteCall
	writerMu   sync.Mutex

	stoppedMu sync.RWMutex
	stopped   bool
}

// ExportSpans pass spans to writer callback.
func (e *Exporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	e.stoppedMu.RLock()
	stopped := e.stopped
	e.stoppedMu.RUnlock()
	if stopped {
		return nil
	}

	if len(spans) == 0 {
		return nil
	}

	stubs := tracetest.SpanStubsFromReadOnlySpans(spans)

	e.writerMu.Lock()
	defer e.writerMu.Unlock()
	for i := range stubs {
		stub := &stubs[i]
		// Encode span stubs, one by one
		if err := e.writer(stub); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown is called to stop the exporter, it preforms no action.
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.stoppedMu.Lock()
	e.stopped = true
	e.stoppedMu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

// MarshalLog is the marshaling function used by the logging system to represent this exporter.
func (e *Exporter) MarshalLog() interface{} {
	return struct {
		Type           string
		WithTimestamps bool
	}{
		Type:           "outtrace",
		WithTimestamps: true,
	}
}

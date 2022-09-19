package exporters

import (
	"encoding/json"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type LogWriter struct {
	Writer    func(*SpanStub) error
}

type SpanStub struct {
	tracetest.SpanStub
	Resource []attribute.KeyValue
}

func (d *LogWriter) Write(data []byte) (n int, err error) {
	var span SpanStub
	if err = json.Unmarshal(data, &span); err == nil {
		err := d.Writer(&span)
		if err != nil {
			return -1, nil
		}
	}
	return len(data), nil
}

package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageLog represents a consumed RabbitMQ message for output.
type MessageLog struct {
	Timestamp  time.Time         `json:"timestamp"`
	RoutingKey string            `json:"routing_key"`
	Exchange   string            `json:"exchange,omitempty"`
	Headers    map[string]any    `json:"headers,omitempty"`
	Body       string            `json:"body"`
	BodyJSON   json.RawMessage   `json:"body_json,omitempty"`
}

// Writer writes formatted message logs to console and/or file.
type Writer struct {
	console io.Writer
	file    io.Writer
	format  string
}

// New creates a Writer. If logFile is empty, only console output is used.
func New(format, logFile string) (*Writer, error) {
	w := &Writer{
		console: os.Stdout,
		format:  format,
	}

	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("ouverture du fichier de log: %w", err)
		}
		w.file = f
	}

	return w, nil
}

// Close closes the log file if one was opened.
func (w *Writer) Close() error {
	if w.file == nil {
		return nil
	}
	if c, ok := w.file.(*os.File); ok {
		return c.Close()
	}
	return nil
}

// Log writes a message to configured outputs.
func (w *Writer) Log(msg amqp.Delivery) error {
	entry := MessageLog{
		Timestamp:  time.Now().UTC(),
		RoutingKey: msg.RoutingKey,
		Exchange:   msg.Exchange,
		Headers:    headersToMap(msg.Headers),
		Body:       string(msg.Body),
	}

	if json.Valid(msg.Body) {
		entry.BodyJSON = json.RawMessage(msg.Body)
	}

	line, err := w.formatEntry(entry)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w.console, line); err != nil {
		return err
	}

	if w.file != nil {
		if _, err := fmt.Fprintln(w.file, line); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) formatEntry(entry MessageLog) (string, error) {
	switch w.format {
	case "json":
		b, err := json.Marshal(entry)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return formatText(entry), nil
	}
}

func formatText(entry MessageLog) string {
	body := entry.Body
	if entry.BodyJSON != nil {
		body = string(entry.BodyJSON)
	}

	if len(entry.Headers) == 0 {
		return fmt.Sprintf("[%s] exchange=%s routing_key=%s body=%s",
			entry.Timestamp.Format(time.RFC3339Nano),
			entry.Exchange,
			entry.RoutingKey,
			body,
		)
	}

	return fmt.Sprintf("[%s] exchange=%s routing_key=%s headers=%v body=%s",
		entry.Timestamp.Format(time.RFC3339Nano),
		entry.Exchange,
		entry.RoutingKey,
		entry.Headers,
		body,
	)
}

func headersToMap(h amqp.Table) map[string]any {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]any, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

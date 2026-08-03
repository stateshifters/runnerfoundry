package daemon

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func newClusterLogSnapshotLogger(writer io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attribute slog.Attr) slog.Attr {
			if attribute.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attribute
		},
	}))
}

type synchronizedLogBuffer struct {
	mutex sync.Mutex
	logs  bytes.Buffer
}

type staticClusterLogSnapshot string

func (snapshot staticClusterLogSnapshot) String() string { return string(snapshot) }

func withoutClusterLogLinesContaining(logs string, fragment string) staticClusterLogSnapshot {
	lines := strings.Split(logs, "\n")
	filtered := lines[:0]
	for _, line := range lines {
		if !strings.Contains(line, fragment) {
			filtered = append(filtered, line)
		}
	}
	return staticClusterLogSnapshot(strings.Join(filtered, "\n"))
}

func (buffer *synchronizedLogBuffer) Write(data []byte) (int, error) {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	written, err := buffer.logs.Write(data)
	if err != nil {
		return written, fmt.Errorf("write synchronized test log: %w", err)
	}
	return written, nil
}

func (buffer *synchronizedLogBuffer) String() string {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.logs.String()
}

func assertClusterLogSnapshot(t *testing.T, logs interface{ String() string }, expected string) {
	t.Helper()
	actual := strings.TrimSpace(logs.String())
	if actual != expected {
		t.Fatalf("cluster log snapshot mismatch\nexpected: %s\nactual:   %s", expected, actual)
	}
}

package daemon

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
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

func assertClusterLogSnapshot(t *testing.T, logs *bytes.Buffer, expected string) {
	t.Helper()
	actual := strings.TrimSpace(logs.String())
	if actual != expected {
		t.Fatalf("cluster log snapshot mismatch\nexpected: %s\nactual:   %s", expected, actual)
	}
}

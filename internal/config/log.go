// Package config — structured logging adapter.
package config

import (
	"log/slog"
	"os"
	"time"
)

// Logger wraps slog with EtcFS-specific convenience methods.
type Logger struct {
	*slog.Logger
}

func NewLogger(level int) *Logger {
	var l slog.Level
	switch level {
	case 0:
		l = slog.LevelError
	case 1:
		l = slog.LevelInfo
	default:
		l = slog.LevelDebug
	}

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: l,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{Key: "t", Value: slog.StringValue(time.Now().Format("15:04:05.000"))}
			}
			return a
		},
	})

	return &Logger{slog.New(handler)}
}

// Fatal logs at ERROR level, then calls os.Exit(1).
func (l *Logger) Fatal(msg string, args ...any) {
	l.Error(msg, args...)
	os.Exit(1)
}

package nt4

import (
	"fmt"
	"log"
	"strings"
)

// LogLevel represents the severity of a log message.
type LogLevel int

const (
	// LogLevelDebug enables all logging including verbose debug messages.
	LogLevelDebug LogLevel = iota
	// LogLevelInfo enables informational messages, warnings, and errors.
	LogLevelInfo
	// LogLevelWarn enables warnings and errors only.
	LogLevelWarn
	// LogLevelError enables error messages only.
	LogLevelError
	// LogLevelSilent disables all logging.
	LogLevelSilent
)

// String returns the string representation of a LogLevel.
func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	case LogLevelSilent:
		return "SILENT"
	default:
		return "UNKNOWN"
	}
}

// Logger is the interface for logging within the NT4 client.
// It follows a structured logging pattern with key-value pairs, similar to slog.
//
// Example usage:
//
//	logger.Info("connected to server", "address", "10.20.64.2", "port", 5810)
//	logger.Error("connection failed", "error", err)
type Logger interface {
	// Debug logs a debug message with optional key-value pairs.
	Debug(msg string, keysAndValues ...any)
	// Info logs an informational message with optional key-value pairs.
	Info(msg string, keysAndValues ...any)
	// Warn logs a warning message with optional key-value pairs.
	Warn(msg string, keysAndValues ...any)
	// Error logs an error message with optional key-value pairs.
	Error(msg string, keysAndValues ...any)
}

// DefaultLogger provides a simple logger that writes to stdout using the standard library log package.
// It supports log levels and structured key-value pairs.
type DefaultLogger struct {
	level  LogLevel
	prefix string
}

// NewDefaultLogger creates a new DefaultLogger with the specified log level.
// Use LogLevelInfo for normal operation, LogLevelDebug for troubleshooting,
// or LogLevelSilent to disable all logging.
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		prefix: "[NT4] ",
	}
}

// Debug logs a debug message if the log level is LogLevelDebug.
func (l *DefaultLogger) Debug(msg string, keysAndValues ...any) {
	if l.level <= LogLevelDebug {
		l.log("DEBUG", msg, keysAndValues...)
	}
}

// Info logs an informational message if the log level is LogLevelInfo or lower.
func (l *DefaultLogger) Info(msg string, keysAndValues ...any) {
	if l.level <= LogLevelInfo {
		l.log("INFO", msg, keysAndValues...)
	}
}

// Warn logs a warning message if the log level is LogLevelWarn or lower.
func (l *DefaultLogger) Warn(msg string, keysAndValues ...any) {
	if l.level <= LogLevelWarn {
		l.log("WARN", msg, keysAndValues...)
	}
}

// Error logs an error message if the log level is LogLevelError or lower.
func (l *DefaultLogger) Error(msg string, keysAndValues ...any) {
	if l.level <= LogLevelError {
		l.log("ERROR", msg, keysAndValues...)
	}
}

func (l *DefaultLogger) log(level string, msg string, keysAndValues ...any) {
	var sb strings.Builder
	sb.WriteString(l.prefix)
	sb.WriteString(level)
	sb.WriteString(" ")
	sb.WriteString(msg)

	// Format key-value pairs
	if len(keysAndValues) > 0 {
		sb.WriteString(" |")
		for i := 0; i < len(keysAndValues); i += 2 {
			if i+1 < len(keysAndValues) {
				sb.WriteString(fmt.Sprintf(" %v=%v", keysAndValues[i], keysAndValues[i+1]))
			} else {
				sb.WriteString(fmt.Sprintf(" %v=<missing>", keysAndValues[i]))
			}
		}
	}

	log.Println(sb.String())
}

// SilentLogger is a logger that discards all log messages.
// Useful for testing or when you want to completely disable logging.
type SilentLogger struct{}

// NewSilentLogger returns a logger that discards all log messages.
func NewSilentLogger() Logger {
	return &SilentLogger{}
}

// Debug does nothing.
func (l *SilentLogger) Debug(msg string, keysAndValues ...any) {}

// Info does nothing.
func (l *SilentLogger) Info(msg string, keysAndValues ...any) {}

// Warn does nothing.
func (l *SilentLogger) Warn(msg string, keysAndValues ...any) {}

// Error does nothing.
func (l *SilentLogger) Error(msg string, keysAndValues ...any) {}

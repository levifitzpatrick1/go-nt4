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
type Logger interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// DefaultLogger provides a simple logger that writes to stdout using the standard library log package.
type DefaultLogger struct {
	level  LogLevel
	prefix string
}

// NewDefaultLogger creates a new DefaultLogger with the specified log level.
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		prefix: "[NT4] ",
	}
}

// Debug logs a debug message if the log level is LogLevelDebug.
func (l *DefaultLogger) Debug(msg string, keysAndValues ...any) {
	if l != nil && l.level <= LogLevelDebug {
		l.log("DEBUG", msg, keysAndValues...)
	}
}

// Info logs an informational message if the log level is LogLevelInfo or lower.
func (l *DefaultLogger) Info(msg string, keysAndValues ...any) {
	if l != nil && l.level <= LogLevelInfo {
		l.log("INFO", msg, keysAndValues...)
	}
}

// Warn logs a warning message if the log level is LogLevelWarn or lower.
func (l *DefaultLogger) Warn(msg string, keysAndValues ...any) {
	if l != nil && l.level <= LogLevelWarn {
		l.log("WARN", msg, keysAndValues...)
	}
}

// Error logs an error message if the log level is LogLevelError or lower.
func (l *DefaultLogger) Error(msg string, keysAndValues ...any) {
	if l != nil && l.level <= LogLevelError {
		l.log("ERROR", msg, keysAndValues...)
	}
}

func (l *DefaultLogger) log(level string, msg string, keysAndValues ...any) {
	var sb strings.Builder
	sb.WriteString(l.prefix)
	sb.WriteString(level)
	sb.WriteString(" ")
	sb.WriteString(msg)

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
type SilentLogger struct{}

// NewSilentLogger returns a logger that discards all log messages.
func NewSilentLogger() Logger {
	return &SilentLogger{}
}

func (l *SilentLogger) Debug(msg string, keysAndValues ...any) {}
func (l *SilentLogger) Info(msg string, keysAndValues ...any)  {}
func (l *SilentLogger) Warn(msg string, keysAndValues ...any)  {}
func (l *SilentLogger) Error(msg string, keysAndValues ...any) {}

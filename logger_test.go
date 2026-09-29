package nt4

import (
	"testing"
)

func TestLoggerLevels(t *testing.T) {
	if LogLevelDebug.String() != "DEBUG" {
		t.Errorf("expected DEBUG, got %s", LogLevelDebug.String())
	}
	if LogLevelInfo.String() != "INFO" {
		t.Errorf("expected INFO, got %s", LogLevelInfo.String())
	}
	if LogLevelWarn.String() != "WARN" {
		t.Errorf("expected WARN, got %s", LogLevelWarn.String())
	}
	if LogLevelError.String() != "ERROR" {
		t.Errorf("expected ERROR, got %s", LogLevelError.String())
	}
	if LogLevelSilent.String() != "SILENT" {
		t.Errorf("expected SILENT, got %s", LogLevelSilent.String())
	}
	if LogLevel(99).String() != "UNKNOWN" {
		t.Errorf("expected UNKNOWN, got %s", LogLevel(99).String())
	}
}

func TestSilentLogger(t *testing.T) {
	l := NewSilentLogger()
	// Should not panic on any log call
	l.Debug("debug msg", "k", "v")
	l.Info("info msg", "k", "v")
	l.Warn("warn msg", "k", "v")
	l.Error("error msg", "k", "v")
}

func TestDefaultLogger(t *testing.T) {
	l := NewDefaultLogger(LogLevelDebug)
	l.Debug("debug message", "key1", "val1", "key2", 42)
	l.Info("info message", "odd")
	l.Warn("warning message")
	l.Error("error message")

	// Test with higher level to exercise filtering
	silent := NewDefaultLogger(LogLevelSilent)
	silent.Debug("should not print")
	silent.Info("should not print")
	silent.Warn("should not print")
	silent.Error("should not print")
}

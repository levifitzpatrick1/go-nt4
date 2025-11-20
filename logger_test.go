package nt4

import (
	"testing"
)

func TestLogLevelString(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected string
	}{
		{LogLevelDebug, "DEBUG"},
		{LogLevelInfo, "INFO"},
		{LogLevelWarn, "WARN"},
		{LogLevelError, "ERROR"},
		{LogLevelSilent, "SILENT"},
		{LogLevel(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.level.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestNewDefaultLogger(t *testing.T) {
	logger := NewDefaultLogger(LogLevelInfo)

	if logger == nil {
		t.Fatal("Expected logger to be created")
	}
	if logger.level != LogLevelInfo {
		t.Errorf("Expected level Info, got %v", logger.level)
	}
	if logger.prefix != "[NT4] " {
		t.Errorf("Expected prefix [NT4], got %s", logger.prefix)
	}
}

func TestDefaultLoggerLevels(t *testing.T) {
	tests := []struct {
		name      string
		logLevel  LogLevel
		shouldLog map[string]bool
	}{
		{
			name:     "Debug level",
			logLevel: LogLevelDebug,
			shouldLog: map[string]bool{
				"debug": true,
				"info":  true,
				"warn":  true,
				"error": true,
			},
		},
		{
			name:     "Info level",
			logLevel: LogLevelInfo,
			shouldLog: map[string]bool{
				"debug": false,
				"info":  true,
				"warn":  true,
				"error": true,
			},
		},
		{
			name:     "Warn level",
			logLevel: LogLevelWarn,
			shouldLog: map[string]bool{
				"debug": false,
				"info":  false,
				"warn":  true,
				"error": true,
			},
		},
		{
			name:     "Error level",
			logLevel: LogLevelError,
			shouldLog: map[string]bool{
				"debug": false,
				"info":  false,
				"warn":  false,
				"error": true,
			},
		},
		{
			name:     "Silent level",
			logLevel: LogLevelSilent,
			shouldLog: map[string]bool{
				"debug": false,
				"info":  false,
				"warn":  false,
				"error": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := NewDefaultLogger(tt.logLevel)

			logger.Debug("debug message", "key", "value")
			logger.Info("info message", "key", "value")
			logger.Warn("warn message", "key", "value")
			logger.Error("error message", "key", "value")
		})
	}
}

func TestNewSilentLogger(t *testing.T) {
	logger := NewSilentLogger()

	if logger == nil {
		t.Fatal("Expected logger to be created")
	}

	// Verify it doesn't panic when called
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")
}

func TestSilentLoggerImplementsInterface(t *testing.T) {
	var _ Logger = &SilentLogger{}
	var _ Logger = NewSilentLogger()
}

func TestDefaultLoggerImplementsInterface(t *testing.T) {
	var _ Logger = &DefaultLogger{}
	var _ Logger = NewDefaultLogger(LogLevelInfo)
}

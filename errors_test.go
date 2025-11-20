package nt4

import (
	"errors"
	"testing"
)

func TestConnectionError(t *testing.T) {
	baseErr := errors.New("connection refused")
	err := newConnectionError("10.20.64.2", 5810, baseErr)

	connErr, ok := err.(*ConnectionError)
	if !ok {
		t.Fatal("Expected ConnectionError type")
	}

	if connErr.Address != "10.20.64.2" {
		t.Errorf("Expected address 10.20.64.2, got %s", connErr.Address)
	}
	if connErr.Port != 5810 {
		t.Errorf("Expected port 5810, got %d", connErr.Port)
	}

	expectedMsg := "failed to connect to 10.20.64.2:5810: connection refused"
	if connErr.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, connErr.Error())
	}

	if !errors.Is(err, baseErr) {
		t.Error("Expected error to wrap base error")
	}
}

func TestConnectionErrorNoUnderlying(t *testing.T) {
	err := &ConnectionError{
		Address: "127.0.0.1",
		Port:    5810,
	}

	expectedMsg := "failed to connect to 127.0.0.1:5810"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestProtocolError(t *testing.T) {
	baseErr := errors.New("invalid message")
	err := newProtocolError("failed to decode", baseErr)

	protErr, ok := err.(*ProtocolError)
	if !ok {
		t.Fatal("Expected ProtocolError type")
	}

	expectedMsg := "protocol error: failed to decode: invalid message"
	if protErr.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, protErr.Error())
	}

	if !errors.Is(err, baseErr) {
		t.Error("Expected error to wrap base error")
	}
}

func TestProtocolErrorNoUnderlying(t *testing.T) {
	err := &ProtocolError{
		Message: "invalid format",
	}

	expectedMsg := "protocol error: invalid format"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestTimeoutError(t *testing.T) {
	baseErr := errors.New("deadline exceeded")
	err := newTimeoutError("connection", baseErr)

	timeErr, ok := err.(*TimeoutError)
	if !ok {
		t.Fatal("Expected TimeoutError type")
	}

	expectedMsg := "timeout during connection: deadline exceeded"
	if timeErr.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, timeErr.Error())
	}

	if !errors.Is(err, baseErr) {
		t.Error("Expected error to wrap base error")
	}
}

func TestTimeoutErrorNoUnderlying(t *testing.T) {
	err := &TimeoutError{
		Operation: "read",
	}

	expectedMsg := "timeout during read"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestValidationError(t *testing.T) {
	err := newValidationError("port", 70000, "port must be between 1-65535")

	valErr, ok := err.(*ValidationError)
	if !ok {
		t.Fatal("Expected ValidationError type")
	}

	if valErr.Field != "port" {
		t.Errorf("Expected field port, got %s", valErr.Field)
	}
	if valErr.Value != 70000 {
		t.Errorf("Expected value 70000, got %v", valErr.Value)
	}

	expectedMsg := "validation error for port: port must be between 1-65535 (value: 70000)"
	if valErr.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, valErr.Error())
	}
}

func TestValidationErrorNoValue(t *testing.T) {
	err := &ValidationError{
		Field:   "address",
		Message: "address is required",
	}

	expectedMsg := "validation error for address: address is required"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestStandardErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		msg  string
	}{
		{"ErrNotConnected", ErrNotConnected, "not connected to server"},
		{"ErrAlreadyConnected", ErrAlreadyConnected, "already connected to server"},
		{"ErrInvalidOptions", ErrInvalidOptions, "invalid client options"},
		{"ErrTopicNotFound", ErrTopicNotFound, "topic not found"},
		{"ErrInvalidTopicID", ErrInvalidTopicID, "invalid topic ID"},
		{"ErrMessageEncoding", ErrMessageEncoding, "message encoding failed"},
		{"ErrMessageDecoding", ErrMessageDecoding, "message decoding failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.msg {
				t.Errorf("Expected error message %q, got %q", tt.msg, tt.err.Error())
			}
		})
	}
}

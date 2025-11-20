package nt4

import (
	"errors"
	"fmt"
)

// Common error types that can be checked with errors.Is().
var (
	// ErrNotConnected is returned when an operation requires an active connection.
	ErrNotConnected = errors.New("not connected to server")

	// ErrAlreadyConnected is returned when attempting to connect while already connected.
	ErrAlreadyConnected = errors.New("already connected to server")

	// ErrInvalidOptions is returned when client options are invalid.
	ErrInvalidOptions = errors.New("invalid client options")

	// ErrTopicNotFound is returned when a requested topic does not exist.
	ErrTopicNotFound = errors.New("topic not found")

	// ErrInvalidTopicID is returned when a topic ID is invalid or not yet assigned.
	ErrInvalidTopicID = errors.New("invalid topic ID")

	// ErrMessageEncoding is returned when message encoding/decoding fails.
	ErrMessageEncoding = errors.New("message encoding failed")

	// ErrMessageDecoding is returned when message decoding fails.
	ErrMessageDecoding = errors.New("message decoding failed")
)

// ConnectionError represents an error that occurred during connection establishment.
type ConnectionError struct {
	Address string
	Port    int
	Err     error
}

// Error implements the error interface.
func (e *ConnectionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("failed to connect to %s:%d: %v", e.Address, e.Port, e.Err)
	}
	return fmt.Sprintf("failed to connect to %s:%d", e.Address, e.Port)
}

// Unwrap returns the underlying error.
func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// ProtocolError represents an error in NT4 protocol handling.
type ProtocolError struct {
	Message string
	Err     error
}

// Error implements the error interface.
func (e *ProtocolError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("protocol error: %s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("protocol error: %s", e.Message)
}

// Unwrap returns the underlying error.
func (e *ProtocolError) Unwrap() error {
	return e.Err
}

// TimeoutError represents a timeout during an operation.
type TimeoutError struct {
	Operation string
	Err       error
}

// Error implements the error interface.
func (e *TimeoutError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("timeout during %s: %v", e.Operation, e.Err)
	}
	return fmt.Sprintf("timeout during %s", e.Operation)
}

// Unwrap returns the underlying error.
func (e *TimeoutError) Unwrap() error {
	return e.Err
}

// ValidationError represents invalid input or configuration.
type ValidationError struct {
	Field   string
	Value   any
	Message string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Value != nil {
		return fmt.Sprintf("validation error for %s: %s (value: %v)", e.Field, e.Message, e.Value)
	}
	return fmt.Sprintf("validation error for %s: %s", e.Field, e.Message)
}

// Helper functions for creating errors with context.

// newConnectionError creates a new ConnectionError.
func newConnectionError(address string, port int, err error) error {
	return &ConnectionError{
		Address: address,
		Port:    port,
		Err:     err,
	}
}

// newProtocolError creates a new ProtocolError.
func newProtocolError(message string, err error) error {
	return &ProtocolError{
		Message: message,
		Err:     err,
	}
}

// newTimeoutError creates a new TimeoutError.
func newTimeoutError(operation string, err error) error {
	return &TimeoutError{
		Operation: operation,
		Err:       err,
	}
}

// newValidationError creates a new ValidationError.
func newValidationError(field string, value any, message string) error {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

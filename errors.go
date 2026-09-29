package nt4

import "errors"

var (
	ErrClosed                   = errors.New("nt4: closed")
	ErrAlreadyStarted           = errors.New("nt4: already started")
	ErrInvalidHandle            = errors.New("nt4: invalid handle")
	ErrForeignHandle            = errors.New("nt4: foreign handle")
	ErrInvalidOptions           = errors.New("nt4: invalid options")
	ErrInvalidValue             = errors.New("nt4: invalid value")
	ErrInvalidType              = errors.New("nt4: invalid type")
	ErrTypeConflict             = errors.New("nt4: type conflict")
	ErrQueueFull                = errors.New("nt4: queue full")
	ErrIncompatibleOptions      = errors.New("nt4: incompatible options")
	ErrTimestampUnrepresentable = errors.New("nt4: timestamp unrepresentable")
	ErrSubscriptionOverflow     = errors.New("nt4: subscription overflow")
	ErrProtocol                 = errors.New("nt4: protocol error")
)

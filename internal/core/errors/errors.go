package errors

import "errors"

type ErrorClass string

const (
	ClassRetryable ErrorClass = "RETRYABLE"
	ClassFatal     ErrorClass = "FATAL"
	ClassWarning   ErrorClass = "WARNING"
)

var (
	ErrDeviceNotFound      = errors.New("device not found in registry")
	ErrUnsupportedProtocol = errors.New("unsupported protocol")
	ErrTimeout             = errors.New("operation timed out")
	ErrNetwork             = errors.New("network error occurred")
	ErrQueueFull           = errors.New("command queue is full")
)

// Classify determines how the router should handle the error.
func Classify(err error) ErrorClass {
	if errors.Is(err, ErrTimeout) || errors.Is(err, ErrNetwork) || errors.Is(err, ErrQueueFull) {
		return ClassRetryable
	}
	if errors.Is(err, ErrDeviceNotFound) || errors.Is(err, ErrUnsupportedProtocol) {
		return ClassFatal
	}
	// Default to warning if we don't know the error
	return ClassWarning
}

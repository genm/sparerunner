// Package reconcile restores Controller admission authority from durable desired
// state and authenticated Agent observations after a process or session boundary.
package reconcile

import "fmt"

// Error is a stable, non-secret reconciliation classification suitable for
// structured logs and future API responses.
type Error struct {
	Code    string
	Field   string
	Message string
}

func (err *Error) Error() string {
	return fmt.Sprintf("reconciliation failed: code=%s field=%s: %s", err.Code, err.Field, err.Message)
}

func invalid(code, field, message string) error {
	return &Error{Code: code, Field: field, Message: message}
}

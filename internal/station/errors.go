package station

import "errors"

var (
	ErrInvalid  = errors.New("invalid transport input")
	ErrLease    = errors.New("mailbox lease is missing or expired")
	ErrQuota    = errors.New("station quota exceeded")
	ErrConflict = errors.New("transport unit identifier conflicts with stored content")
)

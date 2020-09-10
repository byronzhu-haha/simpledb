package errors

import "errors"

var (
	ErrUpdateSliceIdxOutRange = errors.New("index of update slice out range")
	ErrNilKey                 = errors.New("key is nil")
	ErrNotFound               = errors.New("not found value")
	ErrNotInit                = errors.New("db is not init")
	ErrInvalidKey             = errors.New("type of key is invalid")
)

type withMessage struct {
	cause error
	msg   string
}

func (w *withMessage) Error() string { return w.msg + ": " + w.cause.Error() }

func WithMessage(err error, msg string) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   msg,
	}
}

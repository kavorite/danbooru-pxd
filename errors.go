package main

import (
	"fmt"
	"os"

	"github.com/go-errors/errors"
)

type Error struct {
	operation string
	cause *errors.Error
}

func (err *Error) Error() string {
	if err == nil || err.cause == nil {
		return error(nil).Error()
	}
	if err.operation == "" {
		return err.cause.Error()
	}
	return fmt.Sprintf("%s: %s", err.operation, err.cause)
}

func (err *Error) FCk() {
	if err != nil && err.cause != nil {
		trace := errors.WrapPrefix(err.cause, err.operation, 0).ErrorStack()
		fmt.Fprintf(os.Stderr, "fatal: %s\n", trace)
		os.Exit(1)
	}
}

func wrapError(op string, err error) *Error {
	if err != nil {
		return &Error{op, errors.Wrap(err, 1)}
	}
	return nil
}

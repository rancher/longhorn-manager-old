package manager

import (
	"fmt"
	"strings"
)

type Errs []error

func (errs Errs) Error() string {
	ss := []string{}
	for _, err := range errs {
		ss = append(ss, fmt.Sprintf("%+v", err))
	}
	return strings.Join(ss, "\n\n")
}

type ControllerError interface {
	Cause() error
}

type ctrlErr struct {
	err error
}

func NewControllerError(err error) error {
	return &ctrlErr{err}
}

func (e *ctrlErr) Error() string {
	return e.err.Error()
}

func (e *ctrlErr) Cause() error {
	return e.err
}

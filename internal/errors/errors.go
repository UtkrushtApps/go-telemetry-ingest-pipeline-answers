package errors

import "fmt"

// StageError records which pipeline stage produced an error.
type StageError struct {
	Stage string
	Cause error
}

func (e *StageError) Error() string {
	return fmt.Sprintf("pipeline stage %q: %v", e.Stage, e.Cause)
}

func (e *StageError) Unwrap() error {
	return e.Cause
}

// Wrap wraps an error with a stage label.
func Wrap(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &StageError{Stage: stage, Cause: err}
}

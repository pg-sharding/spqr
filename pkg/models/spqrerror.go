package models

import "fmt"

type SpqrError struct {
	error

	ErrorCode string
}

func NewSpqrError(errorMsg string, errorCode string) SpqrError {
	err := SpqrError{
		error:     fmt.Errorf(errorMsg),
		ErrorCode: errorCode,
	}
	return err
}

package spqrlog

import (
	"io"
	"log"
	"os"
	"reflect"
)

// GetPointer do the same thing like fmt.Sprintf("%p", &num) but fast
func GetPointer(value interface{}) uint {
	ptr := reflect.ValueOf(value).Pointer()
	uintPtr := uintptr(ptr)
	return uint(uintPtr)
}


func newWriter(filepath string) (*os.File, io.Writer, error) {
	if filepath == "" {
		return nil, os.Stdout, nil
	}
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return f, f, nil
}

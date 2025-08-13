package spqrlog

import (
	"io"
	"log"
	"os"
	"reflect"
)

// GetPointer do the same thing like fmt.Sprintf("%p", &num) but fast
// GetPointer returns the memory address of the given value as an unsigned integer.
func GetPointer(value any) uint {
	ptr := reflect.ValueOf(value).Pointer()
	uintPtr := uintptr(ptr)
	return uint(uintPtr)
}

// newWriter creates a new file writer based on the provided filepath.
// If the filepath is empty, it returns os.Stdout as the writer.
// Otherwise, it opens the file with the given filepath in append mode,
// creates the file if it doesn't exist, and returns the file and writer.
// The function returns an error if any error occurs during the file operations.
//
// Parameters:
//   - filepath: The path to the file where the data will be written.
//
// Returns:
//   - *os.File: The file where the data will be written.
//   - io.Writer: The writer that writes the data to the file.
//   - error: An error if any error occurs during the file operations.
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

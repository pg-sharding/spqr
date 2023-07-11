package spqrlog

import (
    "reflect"
)

// GetPointer do the same thing like fmt.Sprintf("%p", &num) but fast
func GetPointer(value interface{}) uint {
	ptr := reflect.ValueOf(value).Pointer()
	uintPtr := uintptr(ptr)
	return uint(uintPtr)
}
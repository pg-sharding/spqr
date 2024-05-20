package txstatus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestString tests the String method of the TXStatus type.
// It asserts that the String method returns the expected string representation for each TXStatus value.
func TestString(t *testing.T) {
	assert := assert.New(t)
	cases := map[TXStatus]string{
		TXStatus(73): "IDLE",
		TXStatus(69): "ERROR",
		TXStatus(84): "ACTIVE",
		TXStatus(1):  "INTERNAL STATE",
		TXStatus(0):  "invalid",
	}
	for status, except := range cases {
		assert.Equal(except, status.String())
	}
}

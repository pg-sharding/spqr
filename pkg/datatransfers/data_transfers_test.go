package datatransfers

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// createConnString and LoadConfig tests
// TODO : createConnString and LoadConfig tests

// TestConnectCreds is a test function that tests the ConnectCreds function.
//
// It creates a new assert object using the testing.T object and initializes a wait group.
// It then enters a loop that runs 100 times. In each iteration, it adds 1 to the wait group and spawns a new goroutine.
// Inside the goroutine, it enters another loop that runs 100 times. In each iteration, it calls the LoadConfig function with an empty string as the argument and asserts that there is no error.
// It then calls the createConnString function with "sh1" as the argument.
// After the inner loop, it calls Done on the wait group.
// After the outer loop, it waits for all the goroutines to finish using the Wait method of the wait group.
// Finally, it asserts that true is true.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestConnectCreds(t *testing.T) {
	assert := assert.New(t)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			for range 100 {
				err := LoadConfig("")
				assert.NoError(err)
				createConnString("sh1")
			}
		})
	}
	wg.Wait()
	assert.True(true)
}

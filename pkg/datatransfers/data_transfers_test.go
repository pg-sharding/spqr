package datatransfers

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// createConnString and LoadConfig tests
func TestConnectCreds(t *testing.T) {
	assert := assert.New(t)
	var wg sync.WaitGroup
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				err := LoadConfig("")
				assert.NoError(err)
				createConnString("sh1")
			}
			wg.Done()
		}()
	}
	wg.Wait()
	assert.True(true)
}

package coord

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/stretchr/testify/assert"
)

func TestListRoutersInner(t *testing.T) {
	t.Run("no host", func(t *testing.T) {
		is := assert.New(t)
		actual := listRoutersInner("", "7000")
		is.Equal(topology.Router{
			ID:    "local",
			State: DefaultLocalRouterState,
		}, *actual)
	})
	t.Run("no port", func(t *testing.T) {
		is := assert.New(t)
		actual := listRoutersInner("test_addr", "")
		is.Equal(topology.Router{
			ID:    "local",
			State: DefaultLocalRouterState,
		}, *actual)
	})
	t.Run("address without bracket", func(t *testing.T) {
		is := assert.New(t)
		actual := listRoutersInner("test_addr", "7000")
		is.Equal(topology.Router{
			ID:      DefaultRouterId,
			Address: "[test_addr]:7000",
			State:   DefaultLocalRouterState,
		}, *actual)
	})
	t.Run("address with bracket", func(t *testing.T) {
		is := assert.New(t)
		actual := listRoutersInner("[test_addr]", "7000")
		is.Equal(topology.Router{
			ID:      DefaultRouterId,
			Address: "[test_addr]:7000",
			State:   DefaultLocalRouterState,
		}, *actual)
	})
}
